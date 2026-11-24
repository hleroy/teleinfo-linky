from pathlib import Path

import pytest

from teleinfo_linky.config import DEFAULT_KEYS, analyser, charger

LINKY = {'location': 'Paris', 'legacy_mode': True}
RASPBERRY = {'stty_port': '/dev/ttyAMA0'}


def test_ancien_fichier_sans_section_prometheus():
    # Fichier de configuration de la version 1.0.0
    cfg = {
        'debug': False,
        'linky': {**LINKY, 'checksum_method': 1, 'keys': ['ISOUSC', 'BASE', 'IINST']},
        'raspberry': RASPBERRY,
        'influxdb': {'send_data': True, 'url': 'https://influx', 'bucket': 'teleinfo',
                     'token': 'secret', 'org': 'maison'},
    }
    config = analyser(cfg)
    assert config.influxdb.url == 'https://influx'
    assert config.influxdb.bucket == 'teleinfo'
    assert config.prometheus is None
    assert config.keys == ('ISOUSC', 'BASE', 'IINST')


def test_exemple_de_configuration():
    config = charger(str(Path(__file__).parent.parent / 'config.example.yml'))
    assert config.influxdb is None
    assert config.prometheus is None


def test_prometheus_seul_sans_section_influxdb():
    cfg = {'linky': LINKY, 'raspberry': RASPBERRY,
           'prometheus': {'send_data': True, 'url': 'https://prom/api/v1/write',
                          'username': 'linky', 'password': 'secret'}}
    config = analyser(cfg)
    assert config.influxdb is None
    assert config.prometheus.url == 'https://prom/api/v1/write'
    assert config.prometheus.push_interval == 10
    assert config.keys == DEFAULT_KEYS
    assert config.checksum_method == 1


def test_section_vide_desactivee():
    cfg = {'linky': LINKY, 'raspberry': RASPBERRY, 'influxdb': None, 'prometheus': {'send_data': False}}
    config = analyser(cfg)
    assert config.influxdb is None and config.prometheus is None


def test_cle_manquante(caplog):
    cfg = {'linky': LINKY, 'raspberry': RASPBERRY, 'influxdb': {'send_data': True, 'url': 'x'}}
    with pytest.raises(SystemExit):
        analyser(cfg)
    assert "il manque la clé 'bucket'" in caplog.text


@pytest.mark.parametrize('intervalle', [0, -5, 'dix', True])
def test_push_interval_invalide(intervalle):
    cfg = {'linky': LINKY, 'raspberry': RASPBERRY,
           'prometheus': {'send_data': True, 'url': 'x', 'push_interval': intervalle}}
    with pytest.raises(SystemExit):
        analyser(cfg)


def test_fichier_absent(tmp_path, caplog):
    with pytest.raises(SystemExit):
        charger(str(tmp_path / 'config.yml'))
    assert 'Il manque le fichier de configuration' in caplog.text


def test_erreur_de_syntaxe(tmp_path, caplog):
    chemin = tmp_path / 'config.yml'
    chemin.write_text('linky:\n  location: [\n')
    with pytest.raises(SystemExit):
        charger(str(chemin))
    assert 'erreur de syntaxe' in caplog.text
