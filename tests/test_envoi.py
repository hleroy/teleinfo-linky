import queue

from teleinfo_linky.envoi import ErreurDefinitive, ErreurTemporaire, FileBornee, envoyer, recuperer_lot
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


def test_reessais_avec_attente_croissante_plafonnee():
    sortie = FausseSortie([ErreurTemporaire()] * 13)
    attentes = []
    assert envoyer(sortie, ['trame'], attendre=attentes.append)
    assert sortie.appels == 14
    assert attentes == [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 1024, 1024]


def test_erreur_inattendue_reessayee():
    sortie = FausseSortie([ValueError('bug')])
    assert envoyer(sortie, ['trame'], attendre=lambda s: None)
    assert sortie.appels == 2


def test_erreur_definitive_abandonne_le_lot():
    sortie = FausseSortie([ErreurDefinitive('HTTP 400')])
    assert not envoyer(sortie, ['trame'], attendre=lambda s: None)
    assert sortie.appels == 1


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
