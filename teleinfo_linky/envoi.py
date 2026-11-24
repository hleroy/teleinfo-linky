"""Envoi des trames vers une destination, avec ré-essais."""

import collections
import logging
import queue
import threading
import time

# Attente maximale entre deux essais : 2**10 secondes, soit environ 17 minutes
EXPOSANT_MAX = 10


class ErreurTemporaire(Exception):
    """L'envoi a échoué mais peut réussir plus tard : le lot est conservé et renvoyé."""


class ErreurDefinitive(Exception):
    """L'envoi a été refusé et ne réussira jamais : le lot est abandonné."""


class FileBornee(queue.Queue):
    """File qui écarte ses éléments les plus anciens au-delà de taille_max (0 : illimitée).

    put() ne bloque jamais : la lecture du port série ne doit pas attendre une sortie.
    """

    def __init__(self, taille_max=0):
        self.taille_max = taille_max
        super().__init__()

    def _init(self, maxsize):
        self.queue = collections.deque(maxlen=self.taille_max or None)

    def _put(self, item):
        if len(self.queue) == self.queue.maxlen:
            # L'élément le plus ancien est écarté : il ne sera jamais marqué task_done()
            self.unfinished_tasks -= 1
        self.queue.append(item)


def recuperer_lot(file, taille_max, intervalle=0, attendre=time.sleep):
    """Attend une trame, puis récupère celles déjà en attente (au plus taille_max).

    Avec intervalle > 0, attend ce délai après la première trame pour regrouper les suivantes.
    """
    lot = [file.get()]
    if intervalle:
        attendre(intervalle)
    while len(lot) < taille_max:
        try:
            lot.append(file.get_nowait())
        except queue.Empty:
            break
    return lot


def envoyer(sortie, lot, attendre=time.sleep):
    """Envoie un lot de trames et ré-essaie en boucle tant que l'erreur est temporaire.

    Renvoie True si le lot a été écrit, False s'il a été abandonné.
    """
    x = 0
    while True:
        try:
            sortie.ecrire(lot)
            return True
        except ErreurDefinitive as exc:
            logging.error(f'{sortie.nom} : envoi abandonné pour {len(lot)} trame(s) : {exc}')
            return False
        except ErreurTemporaire:
            pass
        except Exception:
            logging.error(f'{sortie.nom} : erreur inattendue', exc_info=True)

        # Attends de plus en plus longtemps
        sleep = 2 ** x
        logging.error(f'{sortie.nom} : nouvel essai dans {sleep} seconde(s)')
        attendre(sleep)
        # Mais pas trop quand même
        if x < EXPOSANT_MAX:
            x += 1


def boucle_envoi(sortie, file):
    """Envoie en continu les trames de la file vers la sortie. Une trame n'est perdue que si
    la sortie la refuse définitivement."""
    logging.debug(f'Thread d\'envoi vers {sortie.nom} démarré')
    while True:
        lot = recuperer_lot(file, sortie.taille_max_lot, sortie.intervalle)
        envoyer(sortie, lot)
        for _ in lot:
            file.task_done()


def demarrer(sortie):
    """Démarre le thread d'envoi d'une sortie et renvoie la file qui l'alimente."""
    file = FileBornee(sortie.taille_max_file)
    logging.debug(f'Démarrage du thread d\'envoi vers {sortie.nom}')
    thread = threading.Thread(target=boucle_envoi, args=(sortie, file), name=sortie.nom, daemon=True)
    thread.start()
    return file
