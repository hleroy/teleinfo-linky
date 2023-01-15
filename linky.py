#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# __author__ = "Hervé Le Roy"
# __licence__ = "GNU General Public License v3.0"

# Python 3, pré-requis : pip install PyYAML pySerial influxdb-client

import logging
import yaml
import termios
import serial
import signal

from urllib3 import Retry
from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.exceptions import InfluxDBError
from influxdb_client.client.write_api import SYNCHRONOUS, PointSettings

ETIQUETTES_MODE_HISTORIQUE = ('ISOUSC', 'IMAX', 'IINST', 'PAPP',)
MODE_CALCUL_CHECKSUM = 1

START_FRAME = b'\x02'  # STX, Start of Text
STOP_FRAME = b'\x03'   # ETX, End of Text


def _handler(signum, frame):
    logging.info('Programme interrompu par CTRL+C')
    raise SystemExit(0)


def _write_measures(client, bucket, time, measures):
    """Ecrit les mesures dans un bucket InfluxDB."""
    record = []
    for measure, value in measures.items():
        logging.debug(f'Ecriture dans InfluxDB : {time} {measure} {value}')

        point = Point(measure).field('value', value).time(time)
        record.append(point)

    client.write(bucket=bucket, record=record)

    # Sous forme de "Point"
    # record = [
    #           Point('teleinfo')
    #           .tag('adresse_linky', measures['ADCO'])
    #           .tag('option_tarifaire', measures['OPTARIF'])
    #           .tag('periode_tarifaire', measures['PTEC'])
    #           .field('intensite_souscrite', int(measures['ISOUSC']))
    #           .field('intensite_maximale', int(measures['IMAX']))
    #           .field('intensite_instantanee', int(measures['IINST']))
    #           .field('puissance_apparente', int(measures['PAPP']))
    #           .time(time)
    #          ]

    # Sous forme de dictionnaire Python
    # record = [{
    #             'measurement': 'tic',
    #             'tags': tags,
    #             'time': time,
    #             'fields': fields
    #         }]


def _checksum(key, val, separator, checksum):
    """Vérifie la somme de contrôle du groupe d'information. Réf Enedis-NOI-CPT_02E, page 19"""
    data = f'{key}{separator}{val}'
    if MODE_CALCUL_CHECKSUM == 2:
        data += separator
    s = sum([ord(c) for c in data])
    s = (s & 0x3F) + 0x20
    return (checksum == chr(s))


def main():

    # Creation du logger
    logging.basicConfig(level=logging.INFO,
                        format='%(levelname)s: %(message)s')
    logging.info('Démarrage Linky Téléinfo')

    # Capture élégamment une interruption par CTRL+C
    signal.signal(signal.SIGINT, _handler)

    # Lecture du fichier de configuration
    try:
        with open("config.yml", "r") as f:
            cfg = yaml.load(f, Loader=yaml.SafeLoader)
    except FileNotFoundError:
        logging.error(
            'Il manque le fichier de configuration config.yml')
        raise SystemExit(1)
    except yaml.YAMLError as exc:
        if hasattr(exc, 'problem_mark'):
            mark = exc.problem_mark
            logging.error(
                'Le fichier de configuration comporte une erreur de syntaxe')
            logging.error(
                f'La position de l\'erreur semble être en ligne {mark.line+1} colonne {mark.column+1}')
            raise SystemExit(1)
    except (OSError, IOError):
        logging.error(
            'Erreur de lecture du fichier config.yml. Vérifiez les permissions ?')
        raise SystemExit(1)
    except Exception:
        logging.critical(
            'Erreur lors de la lecture du fichier de configuration', exc_info=True)
        raise SystemExit(1)

    try:
        debug = cfg.get('debug', False)
        linky_location = cfg['linky']['location']
        linky_strip_address = cfg['linky']['strip_address']
        linky_legacy_mode = cfg['linky']['legacy_mode']
        raspberry_stty_port = cfg['raspberry']['stty_port']
        influxdb_send_data = cfg['influxdb']['send_data']
        if influxdb_send_data:
            influxdb_url = cfg['influxdb']['url']
            influxdb_bucket = cfg['influxdb']['bucket']
            influxdb_token = cfg['influxdb']['token']
            influxdb_org = cfg['influxdb']['org']
    except KeyError as exc:
        logging.error(
            f'Erreur : il manque la clé {exc} dans le fichier de configuration')
        raise SystemExit(1)
    except Exception:
        logging.critical(
            'Erreur lors de la lecture du fichier de configuration', exc_info=True)
        raise SystemExit(1)

    # Configuration du logger en mode debug
    if debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Connnexion à InfluxDB
    if influxdb_send_data:
        try:
            logging.info(f'Connexion à {influxdb_url}')
            retries = Retry(connect=5, read=2, redirect=5)
            influx_client = InfluxDBClient(url=influxdb_url,
                                           token=influxdb_token,
                                           org=influxdb_org,
                                           retries=retries)

        except InfluxDBError as exc:
            logging.error(f'Erreur de connexion à InfluxDB: {exc}')
            raise SystemExit(1)

        # Obtention du client d'API en écriture
        point_settings = PointSettings()
        point_settings.add_default_tag('location', linky_location)
        write_client = influx_client.write_api(
            write_options=SYNCHRONOUS, point_settings=point_settings)

    # Ouverture du port série
    try:
        baudrate = 1200 if linky_legacy_mode else 9600
        logging.info(
            f'Ouverture du port série {raspberry_stty_port} à {baudrate} Bd')
        with serial.Serial(port=raspberry_stty_port,
                           baudrate=baudrate,
                           parity=serial.PARITY_EVEN,
                           stopbits=serial.STOPBITS_ONE,
                           bytesize=serial.SEVENBITS,
                           timeout=1) as ser:

            # Boucle pour partir sur un début de trame
            logging.info('Attente d\'une première trame...')
            line = ser.readline()
            while START_FRAME not in line:  # Recherche du caractère de début de trame, c'est-à-dire STX 0x02
                line = ser.readline()

            # Initialisation d'une trame vide
            frame = dict()

            # Boucle infinie
            while True:

                # -------------------------------------------------------------------------------------------------------------
                # |                                 Etendue d'un groupe d'information                                         |
                # -------------------------------------------------------------------------------------------------------------
                # | LF (0x0A) | Champ 'étiquette' | Séparateur* | Champ 'donnée' | Séparateur* | Champ 'contrôle' | CR (0x0D) |
                # -------------------------------------------------------------------------------------------------------------
                #             | Etendue checksum mode n°1                        |                                            |
                # -------------------------------------------------------------------------------------------------------------
                #             | Etendue checksum mode n°2                                      |                              |
                # -------------------------------------------------------------------------------------------------------------
                #
                # *Le séparateur peut être SP (0x20) ou HT (0x09)

                try:
                    # Lecture de la première ligne de la première trame
                    line = ser.readline()

                    # Décodage ASCII et nettoyage du retour à la ligne
                    line_str = line.decode('ascii').rstrip()
                    logging.debug(f'Groupe d\'information brut : {line_str}')

                    # Récupération de la somme de contrôle (qui est le dernier caractère de la ligne)
                    checksum = line_str[-1]

                    # Identification du séparateur en vigueur (espace ou tabulation)
                    separator = line_str[-2]

                    # Position du séparateur entre le champ étiquette et le champ données
                    pos = line_str.find(separator)

                    # Extraction de l'étiquette
                    key = line_str[0:pos]

                    # Extraction de la donnée
                    val = line_str[pos+1:-2]

                    # Est-ce une étiquette qui nous intéresse ?
                    if key in ETIQUETTES_MODE_HISTORIQUE:
                        # Vérification de la somme de contrôle
                        if _checksum(key, val, separator, checksum):
                            # Ajout de la valeur
                            frame[key] = int(val)

                    # Si caractère de fin de trame dans la ligne, on écrit les données dans InfluxDB
                    if STOP_FRAME in line:

                        now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

                        # Ecriture dans InfluxDB
                        if influxdb_send_data:
                            _write_measures(
                                write_client, influxdb_bucket, now, frame)

                        # On réinitialise  une nouvelle trame
                        frame = dict()

                except Exception as e:
                    logging.error(
                        f'Une exception s\'est produite : {e}', exc_info=True)
                    logging.error(f'Etiquette : {key}  Donnée : {val}')

    except termios.error:
        logging.error('Erreur lors de la configuration du port série')
        if raspberry_stty_port == '/dev/ttyS0':
            logging.error(
                'Essayez d\'utiliser /dev/ttyAMA0 plutôt que /dev/ttyS0')
        raise SystemExit(1)

    except serial.SerialException as exc:
        if exc.errno == 13:
            logging.error('Erreur de permission sur le port série')
            logging.error(
                'Avez-vous ajouté l\'utilisateur au groupe dialout ?')
            logging.error('  $ sudo usermod -G dialout $USER')
            logging.error(
                'Vous devez vous déconnecter de votre session puis vous reconnecter pour que les droits prennent effet.')
        else:
            logging.error(
                f'Erreur lors de l\'ouverture du port série : {exc}')
        raise SystemExit(1)


if __name__ == '__main__':
    main()
