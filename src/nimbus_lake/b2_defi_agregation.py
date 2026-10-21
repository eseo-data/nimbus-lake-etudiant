"""Bloc 2, défi ★★★ — une agrégation hors mémoire, écrite à la main.

Facultatif. Vous écrivez ce que fait DuckDB quand un GROUP BY dépasse memory_limit :
une agrégation par hachage hybride.

1. Tant que la table de hachage tient dans le budget, chaque nouvelle clé y entre.
2. Quand le budget est plein, une ligne dont la clé est DÉJÀ dans la table y est agrégée ;
   une ligne de clé nouvelle est écrite dans une des P partitions du déversoir, choisie par
   hachage de la clé.
3. En fin d'entrée, on émet la table, on libère le budget, puis on traite chaque partition
   de la même façon (récursion, avec un hachage différent à chaque profondeur).

Agrégat : pour chaque (ic_id, date) -> tn = min(t), tx = max(t), n = nombre de t non nuls.
Une clé dont toutes les valeurs sont nulles sort avec tn = tx = None et n = 0.

Règles vérifiées par les tests :
- chaque clé présente en mémoire est réservée dans `Budget` (reserver / liberer) ;
  `budget.pic` ne dépasse jamais `budget.max_groupes` ;
- aucune ligne n'est écrite dans le déversoir quand toutes les clés tiennent dans le budget ;
- pas de DuckDB, pas de `group_by` pyarrow ni de concaténation de toute l'entrée dans ce module ;
- résultat identique à DuckDB.
"""

from __future__ import annotations

import tempfile
from collections.abc import Iterable, Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.ipc as ipc

SCHEMA_ENTREE = pa.schema([("ic_id", pa.string()), ("date", pa.date32()), ("t", pa.float64())])
SCHEMA_SORTIE = pa.schema([("ic_id", pa.string()), ("date", pa.date32()),
                           ("tn", pa.float64()), ("tx", pa.float64()), ("n", pa.int64())])
N_PARTITIONS = 8


class Budget:
    """Fourni. Comptabilité des groupes en mémoire."""

    def __init__(self, max_groupes: int) -> None:
        self.max_groupes = max_groupes
        self.courant = 0
        self.pic = 0

    @property
    def disponible(self) -> int:
        return self.max_groupes - self.courant

    def reserver(self, n: int = 1) -> None:
        if self.courant + n > self.max_groupes:
            raise MemoryError(f"budget dépassé : {self.courant + n} > {self.max_groupes}")
        self.courant += n
        self.pic = max(self.pic, self.courant)

    def liberer(self, n: int) -> None:
        self.courant -= n
        assert self.courant >= 0


class Deversoir:
    """Fourni. P partitions sur disque (fichiers Arrow IPC), écrites puis relues une fois."""

    def __init__(self, n_partitions: int = N_PARTITIONS, dossier: Path | None = None) -> None:
        self.n_partitions = n_partitions
        self.dossier = dossier or Path(tempfile.mkdtemp(prefix="deversoir-"))
        self.lignes_ecrites = 0
        self._ecrivains: dict[int, ipc.RecordBatchFileWriter] = {}
        self._chemins: dict[int, Path] = {}

    def ecrire(self, partition: int, lot: pa.RecordBatch) -> None:
        if partition not in self._ecrivains:
            chemin = self.dossier / f"p{partition}-{id(self)}.arrow"
            self._chemins[partition] = chemin
            self._ecrivains[partition] = ipc.new_file(str(chemin), SCHEMA_ENTREE)
        self._ecrivains[partition].write_batch(lot)
        self.lignes_ecrites += lot.num_rows

    def partitions(self) -> Iterator[Iterator[pa.RecordBatch]]:
        """Ferme les fichiers, puis rend, partition par partition, un itérateur sur ses lots."""
        for e in self._ecrivains.values():
            e.close()
        for p in sorted(self._chemins):
            yield self._lire(self._chemins[p])

    @staticmethod
    def _lire(chemin: Path) -> Iterator[pa.RecordBatch]:
        with ipc.open_file(str(chemin)) as lecteur:
            for i in range(lecteur.num_record_batches):
                yield lecteur.get_batch(i)


def agreger_hors_memoire(lots: Iterable[pa.RecordBatch], budget: Budget,
                         profondeur: int = 0) -> Iterator[pa.RecordBatch]:
    """D1. Agrégation hybride décrite en tête de module ; rend des lots au SCHEMA_SORTIE (ordre libre).

    Créez un `Deversoir()` par niveau de récursion. Au-delà de la profondeur 6, levez
    RuntimeError (clés trop nombreuses pour le budget, même partitionnées).
    """
    raise NotImplementedError("Bloc 2 D1 — à compléter")
