import queue
from datetime import datetime, timezone

import pytest

from teleinfo_linky.config import Config
from teleinfo_linky.tic import DELAI_REOUVERTURE, decouper_groupe, lire_teleinfo, lire_trames, verifier_checksum

# Groupes réels reçus d'un compteur Linky en mode historique (voir README)
GROUPES_HISTORIQUE = [
    'MOTDETAT 000000 B',
    'OPTARIF BASE 0',
    'ISOUSC 45 ?',
    'BASE 028563823 0',
    'PTEC TH.. $',
    'IINST 003 Z',
    'IMAX 090 H',
    'PAPP 00760 .',
]


def _checksum(data):
    return chr((sum(map(ord, data)) & 0x3F) + 0x20)


@pytest.mark.parametrize('groupe', GROUPES_HISTORIQUE)
def test_checksum_methode_1_groupes_reels(groupe):
    etiquette, valeur, separateur, checksum = decouper_groupe(groupe)
    assert verifier_checksum(etiquette, valeur, separateur, checksum, 1)


def test_checksum_errone():
    assert not verifier_checksum('BASE', '028563824', ' ', '0', 1)


def test_checksum_methode_2_inclut_le_separateur_final():
    # Mode standard : séparateur tabulation, somme jusqu'au second séparateur inclus
    checksum = _checksum('EAST\t012345678\t')
    groupe = f'EAST\t012345678\t{checksum}'
    assert verifier_checksum(*decouper_groupe(groupe), 2)
    assert checksum != _checksum('EAST\t012345678')
    assert not verifier_checksum(*decouper_groupe(groupe), 1)


def test_decouper_groupe_espace():
    assert decouper_groupe('BASE 028563823 0') == ('BASE', '028563823', ' ', '0')


def test_decouper_groupe_tabulation():
    assert decouper_groupe('IRMS1\t003\t3') == ('IRMS1', '003', '\t', '3')


class FausseLiaisonSerie:
    """Renvoie les lignes données, puis lève EOFError."""

    def __init__(self, lignes):
        self.lignes = list(lignes)

    def readline(self):
        if not self.lignes:
            raise EOFError
        return self.lignes.pop(0)


def _trame_brute(groupes):
    """Découpe une trame STX ... ETX en lignes, comme le fait readline()."""
    flux = b'\x02' + b''.join(b'\n' + g.encode('ascii') + b'\r' for g in groupes) + b'\x03'
    return flux


def _lignes(flux):
    lignes, debut = [], 0
    while True:
        fin = flux.find(b'\n', debut)
        if fin < 0:
            if debut < len(flux):
                lignes.append(flux[debut:])
            return lignes
        lignes.append(flux[debut:fin + 1])
        debut = fin + 1


def test_lire_trames_etiquettes_retenues_et_checksum():
    horodatage = datetime(2026, 11, 23, 12, 0, tzinfo=timezone.utc)
    groupes = list(GROUPES_HISTORIQUE)
    groupes[5] = 'IINST 004 Z'  # somme de contrôle erronée : IINST ignorée
    # Une demi-trame avant le premier STX, puis deux trames complètes
    flux = b'PAPP 00760 .\r\x03' + _trame_brute(groupes) + _trame_brute(GROUPES_HISTORIQUE) + b'\n'
    serie = FausseLiaisonSerie(_lignes(flux))

    trames = []
    with pytest.raises(EOFError):
        for trame in lire_trames(serie, ('ISOUSC', 'BASE', 'IINST'), 1, horloge=lambda: horodatage):
            trames.append(trame)

    assert [t.valeurs for t in trames] == [
        {'ISOUSC': 45, 'BASE': 28563823},
        {'ISOUSC': 45, 'BASE': 28563823, 'IINST': 3},
    ]
    assert all(t.horodatage == horodatage for t in trames)


def test_lire_trames_groupe_illisible_ne_bloque_pas():
    flux = _trame_brute(['BASE 028563823 0', '', 'ISOUSC 45 ?']) + b'\n'
    serie = FausseLiaisonSerie(_lignes(b'\x02\n' + flux))
    trames = []
    with pytest.raises(EOFError):
        for trame in lire_trames(serie, ('ISOUSC', 'BASE'), 1):
            trames.append(trame)
    assert trames[0].valeurs == {'BASE': 28563823, 'ISOUSC': 45}


def test_checksum_espace_conserve():
    # La somme de contrôle de « IINST 009 » est un espace (0x20)
    serie = FausseLiaisonSerie(_lignes(_trame_brute(['IINST 009  ']) + b'\n'))
    with pytest.raises(EOFError):
        trames = []
        for trame in lire_trames(serie, ('IINST',), 1):
            trames.append(trame)
    assert trames[0].valeurs == {'IINST': 9}


def test_dernier_groupe_de_la_trame_lu():
    # Le dernier groupe partage sa ligne avec ETX et le STX de la trame suivante
    flux = _trame_brute(['ISOUSC 45 ?', 'BASE 028563823 0']) + _trame_brute(['ISOUSC 45 ?']) + b'\n'
    serie = FausseLiaisonSerie(_lignes(flux))
    trames = []
    with pytest.raises(EOFError):
        for trame in lire_trames(serie, ('ISOUSC', 'BASE'), 1):
            trames.append(trame)
    assert trames[0].valeurs == {'ISOUSC': 45, 'BASE': 28563823}


class FauxPort:
    """Remplace serial.Serial le temps d'une ouverture : renvoie les lignes données, ou lève
    l'exception rencontrée dans la liste."""

    def __init__(self, lignes):
        self.lignes = lignes

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def readline(self):
        ligne = self.lignes.pop(0)
        if isinstance(ligne, Exception):
            raise ligne
        return ligne


def test_port_rouvert_apres_erreur_de_lecture(monkeypatch):
    """Une erreur de lecture rouvre le port sans quitter : les trames déjà reçues restent dans
    la file d'envoi. Le programme s'arrête seulement si le port ne peut plus être ouvert."""
    serial = pytest.importorskip('serial')
    flux = _lignes(_trame_brute(['BASE 028563823 0']) + b'\n')
    erreur = serial.SerialException('device reports readiness to read but returned no data')
    sessions = [flux[:2] + [erreur], flux + [erreur]]
    ouvertures = []

    def ouvrir(**kwargs):
        ouvertures.append(kwargs)
        if not sessions:
            raise serial.SerialException(2, 'No such file or directory')
        return FauxPort(sessions.pop(0))

    monkeypatch.setattr(serial, 'Serial', ouvrir)
    config = Config(debug=False, location='Paris', legacy_mode=True, checksum_method=1, keys=('BASE',),
                    stty_port='/dev/ttyAMA0', influxdb=None, prometheus=None)
    file = queue.Queue()
    attentes = []
    with pytest.raises(SystemExit) as exc:
        lire_teleinfo(config, [file], attendre=attentes.append)

    assert exc.value.code == 1
    assert len(ouvertures) == 3       # deux ouvertures réussies, la troisième échoue
    assert ouvertures[0]['baudrate'] == 1200
    assert attentes == [DELAI_REOUVERTURE, DELAI_REOUVERTURE]
    assert [file.get_nowait().valeurs for _ in range(2)] == [{'BASE': 28563823}] * 2
