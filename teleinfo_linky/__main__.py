"""Point d'entrée : python -m teleinfo_linky ou teleinfo-linky."""

import argparse
import logging
import os
import signal

from . import __version__
from . import config as configuration
from .envoi import demarrer
from .suivi import Suivi
from .tic import lire_teleinfo


def _handler(signum, frame):
    logging.info('Programme interrompu par CTRL+C')
    raise SystemExit(0)


def _sortie_vers_journald():
    """Indique si la sortie d'erreur est reliée à journald.

    systemd définit JOURNAL_STREAM (périphérique:inode) ; la variable est aussi héritée par les
    programmes lancés depuis une session de bureau, d'où la comparaison avec la sortie d'erreur.
    """
    try:
        stat = os.fstat(2)
        return os.environ.get('JOURNAL_STREAM') == f'{stat.st_dev}:{stat.st_ino}'
    except OSError:
        return False


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog='teleinfo-linky',
        description='Lit la télé-information client du compteur Linky et envoie les données '
                    'vers InfluxDB et/ou Prometheus.')
    parser.add_argument('-c', '--config', default='config.yml',
                        help='fichier de configuration (par défaut : config.yml dans le répertoire courant)')
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    args = parser.parse_args(argv)

    # Création du logger. Sous systemd, journald horodate déjà chaque ligne.
    format_log = '%(levelname)s: %(message)s'
    if not _sortie_vers_journald():
        format_log = '%(asctime)s ' + format_log
    logging.basicConfig(level=logging.INFO, format=format_log)
    logging.info(f'Démarrage de Téléinfo Linky {__version__}')

    # Capture élégamment une interruption par CTRL+C
    signal.signal(signal.SIGINT, _handler)

    config = configuration.charger(args.config)

    # Configuration du logger en mode debug
    if config.debug:
        logging.getLogger().setLevel(logging.DEBUG)

    # Une file et un thread d'envoi par destination
    sorties = []
    if config.influxdb:
        from .sorties.influxdb import SortieInfluxDB
        sorties.append(SortieInfluxDB(config.influxdb, config.location))
    if config.prometheus:
        from .sorties.prometheus import SortiePrometheus
        sorties.append(SortiePrometheus(config.prometheus, config.location))
    if not sorties:
        logging.info('Aucun envoi configuré : les trames sont lues sans être envoyées')
    suivi = Suivi()
    files = [demarrer(sortie, suivi) for sortie in sorties]

    # Bilan toutes les 24 heures, puis boucle infinie de lecture de la téléinfo
    suivi.demarrer()
    lire_teleinfo(config, files, suivi)


if __name__ == '__main__':
    main()
