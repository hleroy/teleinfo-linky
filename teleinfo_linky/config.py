"""Lecture et validation du fichier de configuration config.yml."""

import logging
from dataclasses import dataclass
from typing import Optional, Tuple

import yaml

DEFAULT_KEYS = ('ISOUSC', 'BASE', 'IINST',)
DEFAULT_CHECKSUM_METHOD = 1
DEFAULT_PUSH_INTERVAL = 10


@dataclass(frozen=True)
class ConfigInfluxDB:
    url: str
    bucket: str
    token: Optional[str]
    org: Optional[str]


@dataclass(frozen=True)
class ConfigPrometheus:
    url: str
    username: Optional[str]
    password: Optional[str]
    push_interval: float


@dataclass(frozen=True)
class Config:
    debug: bool
    location: str
    legacy_mode: bool
    checksum_method: int
    keys: Tuple[str, ...]
    stty_port: str
    influxdb: Optional[ConfigInfluxDB]        # None si l'envoi vers InfluxDB est désactivé
    prometheus: Optional[ConfigPrometheus]    # None si l'envoi vers Prometheus est désactivé


def lire_fichier(chemin):
    """Lit le fichier YAML et renvoie son contenu. Quitte le programme en cas d'erreur."""
    try:
        with open(chemin, 'r') as f:
            return yaml.load(f, Loader=yaml.SafeLoader)
    except FileNotFoundError:
        logging.error(f'Il manque le fichier de configuration {chemin}')
        raise SystemExit(1)
    except yaml.YAMLError as exc:
        logging.error('Le fichier de configuration comporte une erreur de syntaxe')
        if hasattr(exc, 'problem_mark'):
            mark = exc.problem_mark
            logging.error(f'La position de l\'erreur semble être en ligne {mark.line+1} colonne {mark.column+1}')
        raise SystemExit(1)
    except (OSError, IOError):
        logging.error(f'Erreur de lecture du fichier {chemin}. Vérifiez les permissions ?')
        raise SystemExit(1)
    except Exception:
        logging.critical('Erreur lors de la lecture du fichier de configuration', exc_info=True)
        raise SystemExit(1)


def analyser(cfg):
    """Transforme le contenu du fichier de configuration en objet Config.

    Les sections influxdb et prometheus sont facultatives : une section absente
    équivaut à send_data: false. Quitte le programme en cas d'erreur.
    """
    try:
        cfg = cfg or {}
        influxdb = cfg.get('influxdb') or {}
        prometheus = cfg.get('prometheus') or {}

        config_influxdb = None
        if influxdb.get('send_data', False):
            config_influxdb = ConfigInfluxDB(
                url=influxdb['url'],
                bucket=influxdb['bucket'],
                token=influxdb['token'],
                org=influxdb['org'],
            )

        config_prometheus = None
        if prometheus.get('send_data', False):
            push_interval = prometheus.get('push_interval', DEFAULT_PUSH_INTERVAL)
            if isinstance(push_interval, bool) or not isinstance(push_interval, (int, float)) or push_interval <= 0:
                logging.error('Erreur : prometheus.push_interval doit être un nombre de secondes positif')
                raise SystemExit(1)
            config_prometheus = ConfigPrometheus(
                url=prometheus['url'],
                username=prometheus.get('username'),
                password=prometheus.get('password'),
                push_interval=push_interval,
            )

        return Config(
            debug=cfg.get('debug', False),
            location=cfg['linky']['location'],
            legacy_mode=cfg['linky']['legacy_mode'],
            checksum_method=cfg['linky'].get('checksum_method', DEFAULT_CHECKSUM_METHOD),
            keys=tuple(cfg['linky'].get('keys', DEFAULT_KEYS)),
            stty_port=cfg['raspberry']['stty_port'],
            influxdb=config_influxdb,
            prometheus=config_prometheus,
        )
    except KeyError as exc:
        logging.error(f'Erreur : il manque la clé {exc} dans le fichier de configuration')
        raise SystemExit(1)
    except SystemExit:
        raise
    except Exception:
        logging.critical('Erreur lors de la lecture du fichier de configuration', exc_info=True)
        raise SystemExit(1)


def charger(chemin='config.yml'):
    """Lit et valide le fichier de configuration."""
    return analyser(lire_fichier(chemin))
