#!/usr/bin/env python3
"""Vérifie qu'une étiquette git vX.Y.Z correspond à la version du paquet et au CHANGELOG.

Usage : python3 scripts/notes_version.py vX.Y.Z > notes.md

Affiche sur la sortie standard la section du CHANGELOG de cette version (notes de version
GitHub). Quitte avec une erreur si la version du paquet diffère, si la section est absente ou
si elle n'est pas datée.
"""

import re
import sys
from pathlib import Path

RACINE = Path(__file__).resolve().parent.parent


def erreur(message):
    print(f'ERREUR : {message}', file=sys.stderr)
    raise SystemExit(1)


def version_paquet():
    texte = (RACINE / 'teleinfo_linky' / '__init__.py').read_text(encoding='utf-8')
    return re.search(r"^__version__ = '([^']+)'", texte, re.MULTILINE).group(1)


def section_changelog(version, texte):
    entete = re.compile(rf'^## \[{re.escape(version)}\] - (.+)$', re.MULTILINE)
    debut = entete.search(texte)
    if not debut:
        erreur(f'CHANGELOG.md ne contient pas de section ## [{version}]')
    if not re.fullmatch(r'\d{4}-\d{2}-\d{2}', debut.group(1).strip()):
        erreur(f'la section [{version}] du CHANGELOG.md n\'est pas datée (AAAA-MM-JJ) : {debut.group(1)}')
    suite = re.compile(r'^(## |\[[^\]]+\]: )', re.MULTILINE).search(texte, debut.end())
    fin = suite.start() if suite else len(texte)
    return texte[debut.end():fin].strip()


def main():
    if len(sys.argv) != 2 or not re.fullmatch(r'v\d+\.\d+\.\d+', sys.argv[1]):
        erreur('usage : notes_version.py vX.Y.Z')
    version = sys.argv[1][1:]
    if version_paquet() != version:
        erreur(f'l\'étiquette {sys.argv[1]} ne correspond pas à __version__ = {version_paquet()}')
    notes = section_changelog(version, (RACINE / 'CHANGELOG.md').read_text(encoding='utf-8'))
    if not notes:
        erreur(f'la section [{version}] du CHANGELOG.md est vide')
    print(notes)


if __name__ == '__main__':
    main()
