"""Envoi des trames vers Prometheus par remote write (POST /api/v1/write).

Modèle de données : chaque étiquette TIC devient une métrique linky_<étiquette en minuscules>
(linky_base, linky_iinst...) avec les labels job="linky" et location.
"""

import base64
import http.client
import logging
import re
import time
import urllib.error
import urllib.request
from collections import OrderedDict

from .. import __version__
from ..envoi import ErreurDefinitive, ErreurTemporaire
from .remote_write import compresser_snappy, encoder_write_request

JOB = 'linky'
PREFIXE = 'linky_'
TIMEOUT = 10

# Sans fenêtre « out of order », Prometheus refuse les échantillons de plus d'une heure (par
# rapport au plus récent qu'il a reçu), et un seul échantillon refusé peut faire échouer toute la
# requête. La marge de 10 minutes couvre le décalage d'horloge et la durée de l'envoi.
AGE_MAX = 3000

NOM_VALIDE = re.compile(r'^[a-zA-Z_:][a-zA-Z0-9_:]*$')


class _SansRedirection(urllib.request.HTTPRedirectHandler):
    """Ne suit pas les redirections : urllib transformerait le POST en GET sans données."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_ouvrir_sans_redirection = urllib.request.build_opener(_SansRedirection).open


class SortiePrometheus:
    nom = 'Prometheus'
    taille_max_lot = 2000
    # Trames conservées au plus en mémoire : même à deux trames par seconde, la file contient
    # plus de AGE_MAX secondes de trames. Les plus anciennes, que Prometheus refuserait, sont
    # écartées lorsque le serveur reste longtemps injoignable.
    taille_max_file = 2 * AGE_MAX

    def __init__(self, config, location, ouvrir=_ouvrir_sans_redirection, horloge=time.time):
        self.url = config.url
        self.intervalle = config.push_interval
        # Un lieu numérique (code postal) ou vide est accepté, comme avec InfluxDB
        self.location = '' if location is None else str(location)
        self._ouvrir = ouvrir
        self._horloge = horloge
        self._noms_invalides = set()
        self._entetes = {
            'Content-Encoding': 'snappy',
            'Content-Type': 'application/x-protobuf',
            'X-Prometheus-Remote-Write-Version': '0.1.0',
            'User-Agent': f'teleinfo-linky/{__version__}',
        }
        if config.username:
            identifiants = f'{config.username}:{config.password or ""}'.encode('utf-8')
            self._entetes['Authorization'] = 'Basic ' + base64.b64encode(identifiants).decode('ascii')
        logging.info(f'Envoi vers Prometheus : {self.url}, toutes les {self.intervalle} secondes')

    def series(self, trames):
        """Regroupe les valeurs des trames en séries remote write, une par étiquette."""
        par_etiquette = OrderedDict()
        for trame in trames:
            horodatage_ms = int(round(trame.horodatage.timestamp() * 1000))
            for etiquette, valeur in trame.valeurs.items():
                par_etiquette.setdefault(etiquette, []).append((float(valeur), horodatage_ms))

        series = []
        for etiquette, samples in par_etiquette.items():
            metrique = PREFIXE + etiquette.lower()
            if not NOM_VALIDE.match(metrique):
                if metrique not in self._noms_invalides:
                    logging.warning(f'Etiquette {etiquette} ignorée : {metrique} n\'est pas un nom de métrique valide')
                    self._noms_invalides.add(metrique)
                continue
            labels = sorted([('__name__', metrique), ('job', JOB), ('location', self.location)])
            series.append((labels, samples))
        return series

    def ecrire(self, trames):
        """Envoie les trames en une requête.

        Erreur temporaire (lot conservé) : serveur injoignable, HTTP 5xx, 429, 401, 403 et
        redirections (3xx), qui demandent de corriger la configuration.
        Erreur définitive (lot abandonné) : autres réponses 4xx, par exemple des échantillons
        refusés car trop anciens ou dans le désordre.
        """
        maintenant = self._horloge()
        recentes = [t for t in trames if maintenant - t.horodatage.timestamp() <= AGE_MAX]
        if len(recentes) < len(trames):
            logging.warning(f'{len(trames) - len(recentes)} trame(s) de plus de {AGE_MAX // 60} minutes '
                            'ignorée(s) : Prometheus les refuserait')
        if not recentes:
            return

        corps = compresser_snappy(encoder_write_request(self.series(recentes)))
        requete = urllib.request.Request(self.url, data=corps, headers=self._entetes, method='POST')

        logging.info(f'Ecriture dans Prometheus ({len(recentes)} trame(s))')
        try:
            with self._ouvrir(requete, timeout=TIMEOUT) as reponse:
                reponse.read()
        except urllib.error.HTTPError as exc:
            detail = exc.read(500).decode('utf-8', 'replace').strip()
            if 300 <= exc.code < 400:
                logging.error(f'Prometheus redirige la requête (HTTP {exc.code}) vers '
                              f'{exc.headers.get("Location")} : indiquez cette adresse dans prometheus.url')
                raise ErreurTemporaire() from exc
            if exc.code in (401, 403):
                logging.error(f'Accès refusé par Prometheus (HTTP {exc.code}) : '
                              'vérifiez les identifiants ou la liste d\'adresses IP autorisées')
                raise ErreurTemporaire() from exc
            if exc.code == 429 or exc.code >= 500:
                logging.error(f'Prometheus indisponible (HTTP {exc.code}) : {detail}')
                raise ErreurTemporaire() from exc
            raise ErreurDefinitive(f'HTTP {exc.code} : {detail}') from exc
        except (urllib.error.URLError, http.client.HTTPException, OSError) as exc:
            logging.error(f'Serveur Prometheus injoignable : {exc}')
            raise ErreurTemporaire() from exc
