import http.server
import threading
from datetime import datetime, timedelta, timezone

import cramjam
import pytest

from teleinfo_linky.config import ConfigPrometheus
from teleinfo_linky.envoi import ErreurDefinitive, ErreurTemporaire
from teleinfo_linky.sorties.prometheus import AGE_MAX, SortiePrometheus
from teleinfo_linky.tic import Trame

from test_remote_write import _decoder_write_request

MAINTENANT = datetime(2026, 11, 23, 12, 0, tzinfo=timezone.utc)


class Serveur:
    """Faux serveur remote write : répond avec le code demandé et mémorise les requêtes."""

    def __init__(self):
        self.code = 204
        self.requetes = []
        serveur = self

        class Gestionnaire(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                corps = self.rfile.read(int(self.headers['Content-Length']))
                serveur.requetes.append((self.path, dict(self.headers), corps))
                self.send_response(serveur.code)
                if 300 <= serveur.code < 400:
                    self.send_header('Location', 'https://ailleurs/api/v1/write')
                self.end_headers()
                if serveur.code != 204:
                    self.wfile.write(b'out of bounds')

            def log_message(self, *args):
                pass

        self.httpd = http.server.HTTPServer(('127.0.0.1', 0), Gestionnaire)
        self.url = f'http://127.0.0.1:{self.httpd.server_port}/api/v1/write'
        threading.Thread(target=self.httpd.serve_forever, kwargs={'poll_interval': 0.01}, daemon=True).start()


@pytest.fixture
def serveur():
    s = Serveur()
    yield s
    s.httpd.shutdown()


def _sortie(url, username='linky', password='secret', location='Paris'):
    config = ConfigPrometheus(url=url, username=username, password=password, push_interval=10)
    return SortiePrometheus(config, location, horloge=lambda: MAINTENANT.timestamp())


def _trames():
    return [
        Trame(MAINTENANT - timedelta(seconds=2.6), {'ISOUSC': 45, 'BASE': 28563823, 'IINST': 3}),
        Trame(MAINTENANT - timedelta(seconds=1.3), {'ISOUSC': 45, 'BASE': 28563824, 'IINST': 4}),
    ]


def test_envoi_reussi(serveur):
    _sortie(serveur.url).ecrire(_trames())

    chemin, entetes, corps = serveur.requetes[0]
    assert chemin == '/api/v1/write'
    assert entetes['Content-Encoding'] == 'snappy'
    assert entetes['Content-Type'] == 'application/x-protobuf'
    assert entetes['X-Prometheus-Remote-Write-Version'] == '0.1.0'
    assert entetes['Authorization'] == 'Basic bGlua3k6c2VjcmV0'
    assert entetes['User-Agent'].startswith('teleinfo-linky/')

    t0 = int((MAINTENANT - timedelta(seconds=2.6)).timestamp() * 1000)
    t1 = t0 + 1300
    assert _decoder_write_request(bytes(cramjam.snappy.decompress_raw(corps))) == [
        ([('__name__', 'linky_isousc'), ('job', 'linky'), ('location', 'Paris')], [(45.0, t0), (45.0, t1)]),
        ([('__name__', 'linky_base'), ('job', 'linky'), ('location', 'Paris')],
         [(28563823.0, t0), (28563824.0, t1)]),
        ([('__name__', 'linky_iinst'), ('job', 'linky'), ('location', 'Paris')], [(3.0, t0), (4.0, t1)]),
    ]


def test_sans_authentification(serveur):
    _sortie(serveur.url, username=None, password=None).ecrire(_trames())
    assert 'Authorization' not in serveur.requetes[0][1]


@pytest.mark.parametrize('code,erreur', [
    (400, ErreurDefinitive),
    (404, ErreurDefinitive),
    (401, ErreurTemporaire),
    (403, ErreurTemporaire),
    (429, ErreurTemporaire),
    (503, ErreurTemporaire),
])
def test_codes_http(serveur, code, erreur):
    serveur.code = code
    with pytest.raises(erreur):
        _sortie(serveur.url).ecrire(_trames())


@pytest.mark.parametrize('code', [301, 302, 307, 308])
def test_redirection_non_suivie(serveur, caplog, code):
    """Une redirection n'est pas suivie (urllib enverrait un GET) et le lot est conservé."""
    serveur.code = code
    with pytest.raises(ErreurTemporaire):
        _sortie(serveur.url).ecrire(_trames())
    assert len(serveur.requetes) == 1
    assert 'https://ailleurs/api/v1/write' in caplog.text


@pytest.mark.parametrize('location,attendu', [(75001, '75001'), (None, '')])
def test_location_numerique_ou_vide(serveur, location, attendu):
    # YAML lit un code postal comme un entier, et un champ vide comme None
    _sortie(serveur.url, location=location).ecrire(_trames())
    series = _decoder_write_request(bytes(cramjam.snappy.decompress_raw(serveur.requetes[0][2])))
    assert dict(series[0][0])['location'] == attendu


def test_serveur_injoignable():
    with pytest.raises(ErreurTemporaire):
        _sortie('http://127.0.0.1:9/api/v1/write').ecrire(_trames())


def test_trames_trop_anciennes_ignorees(serveur, caplog):
    anciennes = [Trame(MAINTENANT - timedelta(hours=2), {'BASE': 1})]
    _sortie(serveur.url).ecrire(anciennes)
    assert serveur.requetes == []
    assert '1 trame(s) de plus de 50 minutes ignorée(s)' in caplog.text

    _sortie(serveur.url).ecrire(anciennes + _trames())
    assert len(serveur.requetes) == 1


def test_etiquette_invalide_ignoree(serveur):
    sortie = _sortie(serveur.url)
    series = sortie.series([Trame(MAINTENANT, {'BASE': 1, 'PJOURF+1': 2})])
    assert [dict(labels)['__name__'] for labels, _ in series] == ['linky_base']


def test_age_max_limite(serveur):
    """Une trame d'exactement AGE_MAX secondes est envoyée, une seconde de plus et elle est écartée."""
    limite = Trame(MAINTENANT - timedelta(seconds=AGE_MAX), {'BASE': 1})
    trop_vieille = Trame(MAINTENANT - timedelta(seconds=AGE_MAX + 1), {'BASE': 1})
    _sortie(serveur.url).ecrire([trop_vieille])
    assert serveur.requetes == []
    _sortie(serveur.url).ecrire([limite])
    assert len(serveur.requetes) == 1


def test_age_max_avec_marge():
    # Prometheus refuse les échantillons de plus d'une heure : une marge est nécessaire
    assert AGE_MAX <= 3000
    # La file contient toujours au moins AGE_MAX secondes de trames, même à 2 trames par seconde
    assert SortiePrometheus.taille_max_file >= 2 * AGE_MAX
