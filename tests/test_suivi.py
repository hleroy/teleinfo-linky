import logging

import pytest

from teleinfo_linky.envoi import FileBornee
from teleinfo_linky.suivi import Suivi, _duree, _nombre


@pytest.fixture(autouse=True)
def niveau_info(caplog):
    caplog.set_level(logging.INFO)


def _messages(caplog):
    return [(r.levelname, r.getMessage()) for r in caplog.records]


def test_nombre_accorde_et_milliers():
    assert _nombre(0, 'trame', 'envoyée') == '0 trame envoyée'
    assert _nombre(1, 'trame', 'envoyée') == '1 trame envoyée'
    assert _nombre(57412, 'trame', 'envoyée') == '57 412 trames envoyées'
    assert _duree(86400) == '24 heures'
    assert _duree(90) == '90 secondes'


def test_bilan_normal(caplog):
    suivi = Suivi()
    suivi.ajouter_sortie('InfluxDB', FileBornee())
    suivi.ajouter_sortie('Prometheus', FileBornee(10))
    for _ in range(57412):
        suivi.trame_recue()
    suivi.trames_envoyees('InfluxDB', 57412)
    suivi.trames_envoyees('Prometheus', 57412)
    suivi.bilan()
    assert _messages(caplog) == [
        ('INFO', 'Téléinfo : 57 412 trames reçues pendant les dernières 24 heures'),
        ('INFO', 'InfluxDB : 57 412 trames envoyées pendant les dernières 24 heures'),
        ('INFO', 'Prometheus : 57 412 trames envoyées pendant les dernières 24 heures'),
    ]


def test_bilan_sans_trame_recue(caplog):
    suivi = Suivi()
    suivi.ajouter_sortie('InfluxDB', FileBornee())
    suivi.bilan()
    assert _messages(caplog) == [
        ('WARNING', 'Téléinfo : aucune trame reçue pendant les dernières 24 heures : '
                    'vérifiez le câblage, raspberry.stty_port et linky.legacy_mode'),
        ('INFO', 'InfluxDB : 0 trame envoyée pendant les dernières 24 heures'),
    ]


def test_bilan_sortie_en_panne(caplog):
    """Rien d'envoyé : la file d'attente est indiquée ; les trames perdues comprennent celles
    abandonnées et celles écartées par la file pleine."""
    suivi = Suivi()
    file = FileBornee(3)
    suivi.ajouter_sortie('Prometheus', file)
    for i in range(5):
        suivi.trame_recue()
        file.put(i)
    suivi.trames_perdues('Prometheus', 4)
    suivi.bilan()
    assert _messages(caplog)[1] == (
        'WARNING', 'Prometheus : 0 trame envoyée pendant les dernières 24 heures, 3 en attente, 6 perdues')


def test_bilan_remet_les_compteurs_a_zero(caplog):
    suivi = Suivi()
    file = FileBornee(1)
    suivi.ajouter_sortie('Prometheus', file)
    suivi.trame_recue()
    file.put(1)
    file.put(2)  # la première est écartée
    suivi.trames_envoyees('Prometheus', 1)
    suivi.bilan()
    assert 'Prometheus : 1 trame envoyée pendant les dernières 24 heures, 1 perdue' in caplog.text
    file.get()
    caplog.clear()
    suivi.trame_recue()
    suivi.trames_envoyees('Prometheus', 1)
    suivi.bilan()
    assert _messages(caplog) == [
        ('INFO', 'Téléinfo : 1 trame reçue pendant les dernières 24 heures'),
        ('INFO', 'Prometheus : 1 trame envoyée pendant les dernières 24 heures'),
    ]


class Fin(Exception):
    pass


def _attentes(n):
    """Fonction d'attente qui mémorise les délais et s'arrête après n appels."""
    delais = []

    def attendre(secondes):
        if len(delais) == n:
            raise Fin
        delais.append(secondes)
    return attendre, delais


def test_surveiller_sans_trame_au_demarrage(caplog):
    suivi = Suivi()
    attendre, delais = _attentes(2)
    with pytest.raises(Fin):
        suivi.surveiller(attendre)
    assert delais == [60, 86400 - 60]
    assert _messages(caplog)[0] == ('WARNING', 'Téléinfo : aucune trame reçue depuis 60 secondes : '
                                               'vérifiez le câblage, raspberry.stty_port et linky.legacy_mode')
    # Un bilan après 24 heures
    assert sum('pendant les dernières 24 heures' in m for _, m in _messages(caplog)) == 1


def test_surveiller_avec_trame_au_demarrage(caplog):
    suivi = Suivi()
    suivi.trame_recue()
    attendre, _ = _attentes(1)
    with pytest.raises(Fin):
        suivi.surveiller(attendre)
    assert caplog.records == []
