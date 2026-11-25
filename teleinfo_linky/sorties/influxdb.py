"""Ecriture des trames dans un bucket InfluxDB 2.x.

Modèle de données : chaque étiquette TIC est une measurement (BASE, IINST...) avec un champ
unique `value` et un tag `location`. Le tableau de bord Grafana dépend de ce modèle.
"""

import logging

from ..envoi import ErreurTemporaire


class SortieInfluxDB:
    nom = 'InfluxDB'
    taille_max_lot = 1   # une écriture par trame
    intervalle = 0
    taille_max_file = 0  # file illimitée : aucune trame n'est abandonnée

    def __init__(self, config, location):
        try:
            from influxdb_client import InfluxDBClient
            from influxdb_client.client.exceptions import InfluxDBError
            from influxdb_client.client.write_api import SYNCHRONOUS, PointSettings
            from urllib3 import Retry
        except ImportError:
            logging.error('InfluxDB : la bibliothèque influxdb-client est nécessaire pour envoyer les données')
            logging.error('  $ pip install influxdb-client')
            raise SystemExit(1)

        self.bucket = config.bucket

        # Création du client : aucune connexion n'est établie avant la première écriture
        try:
            logging.info(f'InfluxDB : envoi vers {config.url}, bucket {self.bucket}')
            retries = Retry(connect=4, read=2, redirect=5, backoff_factor=0.5)
            client = InfluxDBClient(url=config.url, token=config.token, org=config.org, retries=retries)
        except InfluxDBError as exc:
            logging.error(f'InfluxDB : erreur de configuration du client : {exc}')
            raise SystemExit(1)

        # Obtention du client d'API en écriture
        point_settings = PointSettings()
        point_settings.add_default_tag('location', location)
        self._write_api = client.write_api(write_options=SYNCHRONOUS, point_settings=point_settings)

    def ecrire(self, trames):
        """Ecrit les trames de façon synchrone et renvoie leur nombre. Toutes les erreurs sont
        temporaires."""
        from influxdb_client import Point
        from influxdb_client.client.exceptions import InfluxDBError
        from influxdb_client.rest import ApiException
        from urllib3.exceptions import HTTPError

        record = []
        for trame in trames:
            ftime = trame.horodatage.strftime('%Y-%m-%dT%H:%M:%SZ')
            for measure, value in trame.valeurs.items():
                record.append(Point(measure).field('value', value).time(ftime))

        logging.debug('InfluxDB : écriture')
        try:
            self._write_api.write(bucket=self.bucket, record=record)
        except ApiException as exc:
            if exc.status == 404:
                logging.error(f'InfluxDB : le bucket {self.bucket} n\'existe pas')
            elif exc.status in (401, 403):
                logging.error(f'InfluxDB : accès refusé (HTTP {exc.status}) : '
                              f'vérifiez le token et ses droits sur le bucket {self.bucket}')
            else:
                logging.error(f'InfluxDB : serveur indisponible (HTTP {exc.status}) : {exc.reason}')
            raise ErreurTemporaire() from exc
        except InfluxDBError as exc:
            logging.error(f'InfluxDB : erreur lors de l\'écriture : {exc}')
            raise ErreurTemporaire() from exc
        except (OSError, HTTPError) as exc:
            logging.error(f'InfluxDB : serveur injoignable : {exc}')
            raise ErreurTemporaire() from exc
        return len(trames)
