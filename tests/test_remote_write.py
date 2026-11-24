import struct

import cramjam
import pytest

from teleinfo_linky.sorties.remote_write import _varint, compresser_snappy, encoder_write_request


def _lire_varint(data, pos):
    resultat, decalage = 0, 0
    while True:
        octet = data[pos]
        pos += 1
        resultat |= (octet & 0x7F) << decalage
        decalage += 7
        if not octet & 0x80:
            return resultat, pos


def _decoder(data):
    """Décodeur protobuf minimal : liste de (numéro de champ, valeur)."""
    champs, pos = [], 0
    while pos < len(data):
        cle, pos = _lire_varint(data, pos)
        numero, type_fil = cle >> 3, cle & 7
        if type_fil == 0:
            valeur, pos = _lire_varint(data, pos)
        elif type_fil == 1:
            valeur = struct.unpack('<d', data[pos:pos + 8])[0]
            pos += 8
        elif type_fil == 2:
            longueur, pos = _lire_varint(data, pos)
            valeur = data[pos:pos + longueur]
            pos += longueur
        else:
            raise AssertionError(f'type de fil inattendu {type_fil}')
        champs.append((numero, valeur))
    return champs


def _decoder_write_request(data):
    series = []
    for numero, serie in _decoder(data):
        assert numero == 1
        labels, samples = [], []
        for champ, valeur in _decoder(serie):
            if champ == 1:
                label = dict(_decoder(valeur))
                labels.append((label[1].decode(), label[2].decode()))
            else:
                sample = dict(_decoder(valeur))
                samples.append((sample[1], sample[2]))
        series.append((labels, samples))
    return series


@pytest.mark.parametrize('n,attendu', [(0, b'\x00'), (1, b'\x01'), (127, b'\x7f'), (128, b'\x80\x01'),
                                        (300, b'\xac\x02'), (-1, b'\xff' * 9 + b'\x01')])
def test_varint(n, attendu):
    assert _varint(n) == attendu


@pytest.mark.parametrize('taille', [0, 1, 59, 60, 61, 255, 256, 257, 65535, 65536, 65537, 200000])
def test_snappy_decodable(taille):
    donnees = bytes(i % 251 for i in range(taille))
    assert bytes(cramjam.snappy.decompress_raw(compresser_snappy(donnees))) == donnees


def test_write_request_aller_retour():
    series = [
        ([('__name__', 'linky_base'), ('job', 'linky'), ('location', 'Pézenas')],
         [(28563823.0, 1763899200000), (28563824.0, 1763899201300)]),
        ([('__name__', 'linky_iinst'), ('job', 'linky'), ('location', 'Pézenas')],
         [(3.0, 1763899200000)]),
    ]
    corps = compresser_snappy(encoder_write_request(series))
    assert _decoder_write_request(bytes(cramjam.snappy.decompress_raw(corps))) == series
