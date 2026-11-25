"""Suivi du fonctionnement : compteurs de trames et bilan périodique dans le journal.

Après le démarrage, le journal reste silencieux tant que tout fonctionne. Le bilan émis toutes
les 24 heures indique combien de trames ont été reçues et envoyées vers chaque sortie : il
confirme que le programme fonctionne toujours, et passe en avertissement dans le cas contraire.
"""

import logging
import threading
import time

# Intervalle entre deux bilans, en secondes
PERIODE = 24 * 3600
# Délai après lequel l'absence de trame est signalée au démarrage, en secondes
DELAI_PREMIERE_TRAME = 60

CONSEIL_LIAISON = 'vérifiez le câblage, raspberry.stty_port et linky.legacy_mode'


def _entier(n):
    """Entier avec un espace comme séparateur des milliers : 57 412."""
    return f'{n:,}'.replace(',', ' ')


def _nombre(n, *mots):
    """Nombre suivi de mots accordés : 0 trame envoyée, 1 trame envoyée, 2 trames envoyées."""
    pluriel = 's' if n > 1 else ''
    return ' '.join([_entier(n)] + [mot + pluriel for mot in mots])


def _duree(secondes):
    """Durée en heures si elle tombe juste (24 heures), en secondes sinon."""
    if secondes % 3600 == 0:
        return _nombre(secondes // 3600, 'heure')
    return _nombre(secondes, 'seconde')


class _Sortie:
    """Compteurs d'une sortie depuis le dernier bilan."""

    def __init__(self, file):
        self.file = file
        self.envoyees = 0
        self.perdues = 0
        # Trames écartées par la file (FileBornee) déjà comptées dans un bilan précédent
        self.ecartees_comptees = 0


class Suivi:
    """Compteurs partagés entre le thread de lecture et les threads d'envoi."""

    def __init__(self, periode=PERIODE, delai_premiere_trame=DELAI_PREMIERE_TRAME):
        self.periode = periode
        self.delai_premiere_trame = delai_premiere_trame
        self._verrou = threading.Lock()
        self._recues = 0
        self._premiere_trame_recue = False
        self._sorties = {}

    def ajouter_sortie(self, nom, file):
        with self._verrou:
            self._sorties[nom] = _Sortie(file)

    def trame_recue(self):
        with self._verrou:
            self._recues += 1
            self._premiere_trame_recue = True

    def trames_envoyees(self, nom, n):
        with self._verrou:
            self._sorties[nom].envoyees += n

    def trames_perdues(self, nom, n):
        with self._verrou:
            self._sorties[nom].perdues += n

    def bilan(self):
        """Écrit le bilan de la période écoulée dans le journal, puis remet les compteurs à zéro."""
        duree = _duree(self.periode)
        with self._verrou:
            recues, self._recues = self._recues, 0
            releves = []
            for nom, sortie in self._sorties.items():
                ecartees = getattr(sortie.file, 'ecartees', 0)
                perdues = sortie.perdues + ecartees - sortie.ecartees_comptees
                releves.append((nom, sortie.envoyees, perdues, sortie.file.qsize()))
                sortie.ecartees_comptees = ecartees
                sortie.envoyees = sortie.perdues = 0

        if recues:
            logging.info(f'Téléinfo : {_nombre(recues, "trame", "reçue")} pendant les dernières {duree}')
        else:
            logging.warning(f'Téléinfo : aucune trame reçue pendant les dernières {duree} : {CONSEIL_LIAISON}')

        for nom, envoyees, perdues, en_attente in releves:
            message = f'{nom} : {_nombre(envoyees, "trame", "envoyée")} pendant les dernières {duree}'
            niveau = logging.INFO
            if not envoyees and (recues or en_attente):
                message += f', {_entier(en_attente)} en attente'
                niveau = logging.WARNING
            if perdues:
                message += f', {_nombre(perdues, "perdue")}'
                niveau = logging.WARNING
            logging.log(niveau, message)

    def surveiller(self, attendre=time.sleep):
        """Signale l'absence de trame peu après le démarrage, puis écrit un bilan à chaque période."""
        attendre(self.delai_premiere_trame)
        with self._verrou:
            recue = self._premiere_trame_recue
        if not recue:
            logging.warning(f'Téléinfo : aucune trame reçue depuis {_duree(self.delai_premiere_trame)} : '
                            f'{CONSEIL_LIAISON}')
        attendre(max(0, self.periode - self.delai_premiere_trame))
        while True:
            self.bilan()
            attendre(self.periode)

    def demarrer(self):
        """Démarre le thread de suivi."""
        thread = threading.Thread(target=self.surveiller, name='Suivi', daemon=True)
        thread.start()
        return thread
