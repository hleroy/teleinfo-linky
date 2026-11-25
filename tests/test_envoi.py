import logging
import queue

import pytest

from teleinfo_linky.envoi import (ErreurDefinitive, ErreurTemporaire, FileBornee, boucle_envoi, envoyer,
                                  recuperer_lot)
from teleinfo_linky.suivi import Suivi
from teleinfo_linky.sorties.influxdb import SortieInfluxDB
from teleinfo_linky.sorties.prometheus import SortiePrometheus


class FausseSortie:
    nom = 'Test'

    def __init__(self, erreurs):
        self.erreurs = list(erreurs)
        self.appels = 0

    def ecrire(self, lot):
        self.appels += 1
        if self.erreurs:
            raise self.erreurs.pop(0)
        return len(lot)


def test_reessais_avec_attente_croissante_plafonnee():
    sortie = FausseSortie([ErreurTemporaire()] * 13)
    attentes = []
    assert envoyer(sortie, ['trame'], attendre=attentes.append) == 1
    assert sortie.appels == 14
    assert attentes == [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 1024, 1024]


def test_erreur_inattendue_reessayee():
    sortie = FausseSortie([ValueError('bug')])
    assert envoyer(sortie, ['trame'], attendre=lambda s: None) == 1
    assert sortie.appels == 2


def test_erreur_definitive_abandonne_le_lot():
    sortie = FausseSortie([ErreurDefinitive('HTTP 400')])
    assert envoyer(sortie, ['trame'], attendre=lambda s: None) == 0
    assert sortie.appels == 1


def test_envoi_retabli_signale(caplog):
    caplog.set_level(logging.INFO)
    assert envoyer(FausseSortie([]), ['trame'], attendre=lambda s: None) == 1
    assert 'rétabli' not in caplog.text
    assert envoyer(FausseSortie([ErreurTemporaire()] * 2), ['a', 'b'], attendre=lambda s: None) == 2
    assert caplog.text.count('Test : envoi rétabli') == 1
    # Le premier envoi réussi est signalé par boucle_envoi : pas de double message
    caplog.clear()
    envoyer(FausseSortie([ErreurTemporaire()]), ['a'], attendre=lambda s: None, signaler_retablissement=False)
    assert 'rétabli' not in caplog.text


class Fin(Exception):
    pass


class FausseFile:
    def task_done(self):
        pass

    def qsize(self):
        return 0


def test_boucle_envoi_premier_envoi_et_compteurs(monkeypatch, caplog):
    """Le premier envoi réussi est signalé une seule fois ; les trames abandonnées sont perdues."""
    caplog.set_level(logging.INFO)
    lots = [['a', 'b'], ['c', 'd', 'e'], ['f']]

    def recuperer(file, taille_max, intervalle):
        if not lots:
            raise Fin
        return lots.pop(0)

    monkeypatch.setattr('teleinfo_linky.envoi.recuperer_lot', recuperer)
    sortie = FausseSortie([ErreurDefinitive('HTTP 400')])
    sortie.taille_max_lot, sortie.intervalle = 10, 0
    suivi = Suivi()
    suivi.ajouter_sortie('Test', FausseFile())
    with pytest.raises(Fin):
        boucle_envoi(sortie, FausseFile(), suivi)

    messages = [r.getMessage() for r in caplog.records if 'premier envoi' in r.getMessage()]
    assert messages == ['Test : premier envoi réussi (3 trames)']
    assert suivi._sorties['Test'].envoyees == 4
    assert suivi._sorties['Test'].perdues == 2


def test_recuperer_lot():
    file = queue.Queue()
    for i in range(5):
        file.put(i)
    assert recuperer_lot(file, 1) == [0]
    attentes = []
    assert recuperer_lot(file, 3, intervalle=10, attendre=attentes.append) == [1, 2, 3]
    assert attentes == [10]
    assert recuperer_lot(file, 100) == [4]


def test_file_bornee_ecarte_les_plus_anciennes():
    file = FileBornee(3)
    for i in range(10):
        file.put_nowait(i)
    assert [file.get_nowait() for _ in range(3)] == [7, 8, 9]
    assert file.ecartees == 7
    for _ in range(3):
        file.task_done()
    # Les éléments écartés ne restent pas comptés comme en cours de traitement
    assert file.unfinished_tasks == 0
    file.join()


def test_file_illimitee():
    file = FileBornee(0)
    for i in range(10000):
        file.put_nowait(i)
    assert file.qsize() == 10000


def test_file_de_chaque_sortie():
    assert SortieInfluxDB.taille_max_file == 0  # aucune trame InfluxDB abandonnée
    assert SortiePrometheus.taille_max_file > 0
