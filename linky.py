#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# __author__ = "Hervé Le Roy"
# __licence__ = "GNU General Public License v3.0"

# Lanceur de compatibilité pour les installations antérieures à la version 1.1.0, dont le
# service systemd exécute /usr/bin/python3 ~/linky/linky.py. Le code est désormais dans le
# paquet teleinfo_linky, lancé par la commande teleinfo-linky.

from teleinfo_linky.__main__ import main

if __name__ == '__main__':
    main()
