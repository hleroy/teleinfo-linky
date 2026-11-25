"""Lecture de la télé-information client (TIC) sur le port série."""

import logging
import termios
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Union

START_FRAME = b'\x02'  # STX, Start of Text
STOP_FRAME = b'\x03'   # ETX, End of Text

# Attente avant de rouvrir le port série après une erreur de lecture, en secondes
DELAI_REOUVERTURE = 10

# Etiquettes dont la donnée est un texte, conservée telle quelle (sans les espaces de bourrage).
# Les autres sont converties en entier. Réf Enedis-NOI-CPT_02E (historique) et
# Enedis-NOI-CPT_54E (standard).
ETIQUETTES_TEXTE = frozenset([
    # Mode historique
    'OPTARIF',   # option tarifaire (BASE, HC.., EJP., BBRx)
    'PTEC',      # période tarifaire en cours (TH.., HP.., HCJB...)
    'DEMAIN',    # couleur du lendemain en option Tempo (----, BLEU, BLAN, ROUG)
    'HHPHC',     # horaire heures pleines / heures creuses (A, C, D, E, Y)
    # Mode standard
    'NGTF',      # nom du calendrier tarifaire fournisseur
    'LTARF',     # libellé tarif fournisseur en cours
    'MSG1',      # message court
    'MSG2',      # message ultra court
    'PJOURF+1',  # profil du prochain jour calendrier fournisseur
    'PPOINTE',   # profil du prochain jour de pointe
])


@dataclass(frozen=True)
class Trame:
    """Trame reçue : horodatage UTC de réception et valeurs des étiquettes retenues."""
    horodatage: datetime
    valeurs: Dict[str, Union[int, str]]


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


def verifier_checksum(etiquette, valeur, separateur, checksum, methode):
    """Vérifie la somme de contrôle du groupe d'information. Réf Enedis-NOI-CPT_02E, page 19."""
    data = f'{etiquette}{separateur}{valeur}'
    if methode == 2:
        data += separateur
    s = sum([ord(c) for c in data])
    s = (s & 0x3F) + 0x20
    return checksum == chr(s)


def decouper_groupe(groupe):
    """Découpe un groupe d'information (sans LF ni CR) en (étiquette, donnée, séparateur, contrôle).

    En mode standard, la donnée d'un groupe horodaté contient l'horodatage, un séparateur puis
    la valeur : la somme de contrôle porte sur l'ensemble (voir separer_horodatage).
    """
    # La somme de contrôle est le dernier caractère de la ligne
    checksum = groupe[-1]
    # Le séparateur en vigueur (espace ou tabulation) est l'avant-dernier caractère
    separateur = groupe[-2]
    # Position du séparateur entre le champ étiquette et le champ donnée
    pos = groupe.find(separateur)
    return groupe[0:pos], groupe[pos+1:-2], separateur, checksum


def separer_horodatage(donnee, separateur):
    """Renvoie la valeur d'une donnée, sans l'horodatage des groupes horodatés du mode standard.

    Groupe horodaté : étiquette HT horodatage HT valeur HT contrôle. La valeur est vide pour
    l'étiquette DATE. En mode historique (séparateur espace), la donnée est renvoyée telle quelle.
    """
    if separateur == '\t' and '\t' in donnee:
        return donnee.split('\t', 1)[1]
    return donnee


def convertir(etiquette, valeur):
    """Convertit la valeur en entier, sauf pour les étiquettes textuelles (ETIQUETTES_TEXTE)."""
    if etiquette in ETIQUETTES_TEXTE:
        return valeur.strip(' ')
    return int(valeur)


def _annoncer_premiere_trame(etiquettes, valeurs, vues):
    """Indique les étiquettes retenues dans la première trame, et celles qui en sont absentes."""
    retenues = [e for e in etiquettes if e in valeurs]
    logging.info(f'Téléinfo : première trame reçue ({", ".join(retenues) or "aucune étiquette retenue"})')
    absentes = [e for e in etiquettes if e not in vues]
    if absentes:
        # Simple information : certaines étiquettes n'apparaissent que par moments (ADPS, PEJP...)
        logging.info(f'Téléinfo : étiquette(s) absente(s) de la trame : {", ".join(absentes)}')


def lire_trames(ser, etiquettes, methode_checksum, horloge=lambda: datetime.now(timezone.utc)):
    """Lit les groupes d'information sur ser (objet disposant de readline()) et renvoie les trames.

    Générateur infini : chaque trame complète (fin de trame ETX) est renvoyée avec les
    valeurs des étiquettes retenues dont la somme de contrôle est correcte.
    """
    # Boucle pour partir sur un début de trame. Le STX d'une trame est toujours suivi du LF de
    # son premier groupe : un octet 0x02 ailleurs dans une ligne est un caractère mal reçu, par
    # exemple pendant la synchronisation de l'UART à l'ouverture du port.
    logging.info('Téléinfo : attente de la première trame...')
    line = ser.readline()
    while not line.endswith(START_FRAME + b'\n'):
        line = ser.readline()

    valeurs = dict()
    # Étiquettes de linky.keys présentes dans la trame en cours, même sans valeur retenue
    vues = set()
    premiere = True
    # Une première trame sans aucune étiquette de linky.keys peut être tronquée : la suivante
    # confirme le problème avant de le signaler
    trame_sans_etiquette = False
    while True:
        line = ser.readline()
        line_str = ''
        try:
            # Décodage ASCII et suppression de CR, LF et des caractères de fin et de début de
            # trame (ETX, STX) qui suivent le dernier groupe. Pas de rstrip() sans argument :
            # la somme de contrôle peut être un espace (0x20).
            line_str = line.decode('ascii').rstrip('\r\n\x02\x03')
            logging.debug(f'Groupe d\'information brut : {line_str}')

            # Une ligne vide (STX seul, délai de lecture écoulé) ne contient pas de groupe
            key = None
            if line_str:
                key, val, separator, checksum = decouper_groupe(line_str)

            # Est-ce une étiquette qui nous intéresse ?
            if key in etiquettes:
                vues.add(key)
                if verifier_checksum(key, val, separator, checksum, methode_checksum):
                    val = separer_horodatage(val, separator)
                    # Un groupe sans valeur (DATE) ne contient que son horodatage
                    if val:
                        valeurs[key] = convertir(key, val)
                else:
                    logging.warning(f'Téléinfo : somme de contrôle erronée pour {key}')

        except Exception as e:
            # La trace complète n'est utile qu'en mode debug
            logging.error(f'Téléinfo : groupe d\'information illisible {line_str!r} : {e!r}',
                          exc_info=logging.getLogger().isEnabledFor(logging.DEBUG))

        # Si caractère de fin de trame dans la ligne, la trame est complète
        if STOP_FRAME in line:
            if premiere:
                if vues:
                    _annoncer_premiere_trame(etiquettes, valeurs, vues)
                    premiere = False
                elif trame_sans_etiquette:
                    logging.warning('Téléinfo : trames reçues, mais aucune des étiquettes de linky.keys '
                                    'n\'y figure : vérifiez linky.keys et linky.legacy_mode')
                    premiere = False
                else:
                    trame_sans_etiquette = True
            logging.debug(f'Trame reçue : {valeurs}')
            yield Trame(horodatage=horloge(), valeurs=valeurs)
            valeurs = dict()
            vues = set()


def _ouvrir_port(config, serial):
    """Ouvre le port série en 7E1. Quitte le programme en cas d'erreur (configuration, droits)."""
    try:
        baudrate = 1200 if config.legacy_mode else 9600
        mode = 'historique' if config.legacy_mode else 'standard'
        logging.info(f'Téléinfo : lecture de {config.stty_port} en mode {mode} ({baudrate} Bd)')
        return serial.Serial(port=config.stty_port,
                             baudrate=baudrate,
                             parity=serial.PARITY_EVEN,
                             stopbits=serial.STOPBITS_ONE,
                             bytesize=serial.SEVENBITS,
                             timeout=1)

    except termios.error:
        logging.error(f'Téléinfo : erreur lors de la configuration du port série {config.stty_port}')
        if config.stty_port == '/dev/ttyS0':
            logging.error('Téléinfo : essayez d\'utiliser /dev/ttyAMA0 plutôt que /dev/ttyS0')
        raise SystemExit(1)

    except serial.SerialException as exc:
        if exc.errno == 13:
            logging.error(f'Téléinfo : accès refusé au port série {config.stty_port}')
            logging.error('Téléinfo : avez-vous ajouté l\'utilisateur au groupe dialout ?')
            logging.error('  $ sudo usermod -a -G dialout $USER')
            logging.error(
                'Vous devez vous déconnecter de votre session puis vous reconnecter pour que les droits prennent effet.')
        else:
            logging.error(f'Téléinfo : erreur sur le port série : {exc}')
        raise SystemExit(1)


def lire_teleinfo(config, files, suivi, attendre=time.sleep):
    """Ouvre le port série et place chaque trame reçue dans chacune des files d'envoi.

    Après une erreur de lecture, le port est fermé puis rouvert : les trames en attente
    d'envoi restent en mémoire. Si le port ne peut plus être ouvert, le programme s'arrête.
    """
    import serial

    while True:
        try:
            with _ouvrir_port(config, serial) as ser:
                for trame in lire_trames(ser, config.keys, config.checksum_method):
                    suivi.trame_recue()
                    for file in files:
                        file.put(trame)
        except serial.SerialException as exc:
            logging.error(f'Téléinfo : erreur de lecture sur le port série : {exc}')
            logging.error(f'Téléinfo : réouverture du port dans {DELAI_REOUVERTURE} secondes')
            attendre(DELAI_REOUVERTURE)
