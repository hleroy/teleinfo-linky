"""Encodage d'une requête Prometheus remote write (protocole 1.0), sans dépendance externe.

Le corps d'une requête est un message protobuf WriteRequest compressé en snappy (format bloc) :

    message WriteRequest { repeated TimeSeries timeseries = 1; }
    message TimeSeries   { repeated Label labels = 1; repeated Sample samples = 2; }
    message Label        { string name = 1; string value = 2; }
    message Sample       { double value = 1; int64 timestamp = 2; }   // timestamp en ms

Le protobuf est encodé à la main et la compression snappy n'utilise que des littéraux : le
résultat est un flux snappy valide, simplement non compressé. Cela évite d'installer protobuf
et python-snappy, difficiles à obtenir sur un Raspberry Pi Zero (armv6).
"""

import struct

# Types de fil protobuf
_VARINT = 0
_FIXED64 = 1
_LONGUEUR = 2

# Taille maximale d'un littéral snappy émis par compresser_snappy
_TAILLE_LITTERAL = 65536


def _varint(n):
    if n < 0:
        n += 1 << 64  # int64 négatif : complément à deux sur 64 bits
    octets = bytearray()
    while True:
        octet = n & 0x7F
        n >>= 7
        if n:
            octets.append(octet | 0x80)
        else:
            octets.append(octet)
            return bytes(octets)


def _cle(numero, type_fil):
    return _varint((numero << 3) | type_fil)


def _champ_longueur(numero, donnees):
    return _cle(numero, _LONGUEUR) + _varint(len(donnees)) + donnees


def _label(nom, valeur):
    return _champ_longueur(1, nom.encode('utf-8')) + _champ_longueur(2, valeur.encode('utf-8'))


def _sample(valeur, horodatage_ms):
    return _cle(1, _FIXED64) + struct.pack('<d', valeur) + _cle(2, _VARINT) + _varint(horodatage_ms)


def encoder_write_request(series):
    """Encode une liste de séries en message WriteRequest.

    series : liste de (labels, samples), où labels est une liste de (nom, valeur) déjà
    triée par nom et samples une liste de (valeur, horodatage en ms) en ordre chronologique.
    """
    message = bytearray()
    for labels, samples in series:
        serie = bytearray()
        for nom, valeur in labels:
            serie += _champ_longueur(1, _label(nom, valeur))
        for valeur, horodatage_ms in samples:
            serie += _champ_longueur(2, _sample(valeur, horodatage_ms))
        message += _champ_longueur(1, bytes(serie))
    return bytes(message)


def compresser_snappy(donnees):
    """Produit un flux snappy (format bloc) composé uniquement de littéraux."""
    sortie = bytearray(_varint(len(donnees)))
    for debut in range(0, len(donnees), _TAILLE_LITTERAL):
        morceau = donnees[debut:debut + _TAILLE_LITTERAL]
        n = len(morceau) - 1
        if n < 60:
            sortie.append(n << 2)
        elif n < 0x100:
            sortie.append(60 << 2)
            sortie.append(n)
        else:
            sortie.append(61 << 2)
            sortie += n.to_bytes(2, 'little')
        sortie += morceau
    return bytes(sortie)
