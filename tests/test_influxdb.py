"""Sortie InfluxDB : modèle de données, requête d'écriture et gestion des erreurs.

Le dépôt est public et d'autres utilisateurs envoient leurs données vers InfluxDB : ces tests
figent le comportement de la version 1.0.0 (une measurement par étiquette, champ `value`, tag
`location`, horodatage à la seconde, aucune trame abandonnée).
"""

import http.server
import json
import sys
import threading
import urllib.parse
from datetime import datetime, timezone

import pytest

from teleinfo_linky.config import ConfigInfluxDB
from teleinfo_linky.envoi import ErreurTemporaire, envoyer
from teleinfo_linky.sorties.influxdb import SortieInfluxDB
from teleinfo_linky.tic import Trame

# Les fractions de seconde ne doivent pas être envoyées
INSTANT = datetime(2026, 11, 23, 12, 0, 0, 700000, tzinfo=timezone.utc)
NS = int(INSTANT.replace(microsecond=0).timestamp()) * 10**9


class Serveur:
    """Faux serveur InfluxDB 2.x : répond avec les codes demandés et mémorise les requêtes."""

    def __init__(self):
        self.codes = []     # codes HTTP des prochaines réponses ; 204 ensuite
        self.requetes = []
        serveur = self

        class Gestionnaire(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                corps = self.rfile.read(int(self.headers['Content-Length']))
                serveur.requetes.append((self.path, dict(self.headers), corps.decode()))
                code = serveur.codes.pop(0) if serveur.codes else 204
                self.send_response(code)
                if code == 204:
                    self.end_headers()
                    return
                erreur = json.dumps({'code': 'erreur', 'message': f'code {code}'}).encode()
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(erreur)))
                self.end_headers()
                self.wfile.write(erreur)

            def log_message(self, *args):
                pass

        self.httpd = http.server.HTTPServer(('127.0.0.1', 0), Gestionnaire)
        self.url = f'http://127.0.0.1:{self.httpd.server_port}'
        threading.Thread(target=self.httpd.serve_forever, kwargs={'poll_interval': 0.01}, daemon=True).start()


@pytest.fixture
def serveur():
    s = Serveur()
    yield s
    s.httpd.shutdown()


def _sortie(url):
    config = ConfigInfluxDB(url=url, bucket='teleinfo', token='jeton-secret', org='maison')
    return SortieInfluxDB(config, 'Paris')


def _trame():
    return Trame(INSTANT, {'ISOUSC': 45, 'BASE': 28563823, 'IINST': 9})


def test_requete_ecriture(serveur):
    assert _sortie(serveur.url).ecrire([_trame()]) == 1

    assert len(serveur.requetes) == 1
    chemin, entetes, corps = serveur.requetes[0]
    url = urllib.parse.urlsplit(chemin)
    assert url.path == '/api/v2/write'
    assert urllib.parse.parse_qs(url.query) == {'org': ['maison'], 'bucket': ['teleinfo'], 'precision': ['ns']}
    assert entetes['Authorization'] == 'Token jeton-secret'


def test_modele_de_donnees(serveur):
    """Une measurement par étiquette, champ entier `value`, tag `location`, horodatage à la seconde.

    Le tableau de bord Grafana (dashboard_grafana.json) dépend de ce modèle.
    """
    _sortie(serveur.url).ecrire([_trame()])

    assert serveur.requetes[0][2].split('\n') == [
        f'ISOUSC,location=Paris value=45i {NS}',
        f'BASE,location=Paris value=28563823i {NS}',
        f'IINST,location=Paris value=9i {NS}',
    ]


def test_etiquette_textuelle_champ_texte(serveur):
    _sortie(serveur.url).ecrire([Trame(INSTANT, {'PTEC': 'HP..', 'IINST': 9})])

    assert serveur.requetes[0][2].split('\n') == [
        f'PTEC,location=Paris value="HP.." {NS}',
        f'IINST,location=Paris value=9i {NS}',
    ]


def test_une_ecriture_par_trame():
    assert SortieInfluxDB.taille_max_lot == 1
    assert SortieInfluxDB.intervalle == 0


@pytest.mark.parametrize('code,message', [
    (404, "InfluxDB : le bucket teleinfo n'existe pas"),
    (403, 'InfluxDB : accès refusé (HTTP 403) : vérifiez le token et ses droits sur le bucket teleinfo'),
    (401, 'InfluxDB : accès refusé (HTTP 401) : vérifiez le token et ses droits sur le bucket teleinfo'),
    (500, 'InfluxDB : serveur indisponible (HTTP 500)'),
    (503, 'InfluxDB : serveur indisponible (HTTP 503)'),
])
def test_erreurs_http_temporaires(serveur, caplog, code, message):
    """Aucune erreur n'est définitive : la trame est conservée et l'écriture recommencée."""
    serveur.codes = [code]
    with pytest.raises(ErreurTemporaire):
        _sortie(serveur.url).ecrire([_trame()])
    assert message in caplog.text


def test_serveur_injoignable(monkeypatch, caplog):
    # Les nouvelles tentatives de connexion d'urllib3 (Retry) attendraient sinon plusieurs secondes
    monkeypatch.setattr('urllib3.util.retry.time.sleep', lambda s: None)
    with pytest.raises(ErreurTemporaire):
        _sortie('http://127.0.0.1:9').ecrire([_trame()])
    assert 'InfluxDB : serveur injoignable' in caplog.text


def test_trame_conservee_jusqu_au_retour_du_serveur(serveur):
    """Avec la boucle d'envoi, la trame est réécrite après les erreurs, sans être perdue."""
    serveur.codes = [503, 500]
    attentes = []
    assert envoyer(_sortie(serveur.url), [_trame()], attendre=attentes.append) == 1

    assert attentes == [1, 2]
    assert len(serveur.requetes) == 3
    assert serveur.requetes[0][2] == serveur.requetes[2][2]


def test_bibliotheque_absente(monkeypatch, caplog):
    monkeypatch.setitem(sys.modules, 'influxdb_client', None)
    with pytest.raises(SystemExit) as exc:
        _sortie('http://127.0.0.1:9')
    assert exc.value.code == 1
    assert 'pip install influxdb-client' in caplog.text
