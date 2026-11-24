"""Point d'entrée : python -m teleinfo_linky ou teleinfo-linky."""

import argparse
import logging
import signal

from . import __version__
from . import config as configuration
from .envoi import demarrer
from .tic import lire_teleinfo


def _handler(signum, frame):
    logging.info('Programme interrompu par CTRL+C')
    raise SystemExit(0)


def main(argv=None):
    parser = argparse.ArgumentParser(
        prog='teleinfo-linky',
        description='Lit la télé-information client du compteur Linky et envoie les données '
                    'vers InfluxDB et/ou Prometheus.')
    parser.add_argument('-c', '--config', default='config.yml',
                        help='fichier de configuration (par défaut : config.yml dans le répertoire courant)')
    parser.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    args = parser.parse_args(argv)

    # Création du logger
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
    logging.info(f'Démarrage Linky Téléinfo {__version__}')

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
    files = [demarrer(sortie) for sortie in sorties]

    # Lance la boucle infinie de lecture de la téléinfo
    lire_teleinfo(config, files)


if __name__ == '__main__':
    main()
