"""Bloc 3, partie A — une table Apache Iceberg en local : catalogue, snapshots, time travel, évolution.

Une table Iceberg, c'est des fichiers Parquet (les données) plus des métadonnées :
- metadata.json : schémas, partition specs, sort orders, liste des snapshots ;
- manifest list (Avro) : un par snapshot, la liste de ses manifests ;
- manifests (Avro) : la liste des fichiers de données, avec leurs statistiques.
Un catalogue (ici SQLite) ne retient qu'une chose par table : le chemin du metadata.json
courant. Publier une nouvelle version = remplacer atomiquement ce pointeur.

Exercices A1 à A5. Tests : `uv run pytest tests/test_b3_iceberg.py`.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import MonthTransform
from pyiceberg.types import DateType, DoubleType, LongType, NestedField, StringType

NOM_TABLE = "gold.journaliere"

SCHEMA_ICEBERG = Schema(
    NestedField(1, "ic_id", StringType(), required=True),
    NestedField(2, "date", DateType(), required=True),
    NestedField(3, "tn", DoubleType(), required=False),
    NestedField(4, "tx", DoubleType(), required=False),
    NestedField(5, "rr", DoubleType(), required=False),
    identifier_field_ids=[1, 2],
)
SCHEMA_ARROW = SCHEMA_ICEBERG.as_arrow()


def catalogue(entrepot: Path) -> SqlCatalog:
    """A1. Catalogue SQL nommé "nimbus" : base SQLite `entrepot/catalogue.db`, entrepôt `file://entrepot/donnees`.

    Créez le dossier `entrepot` s'il n'existe pas.
    """
    raise NotImplementedError("Bloc 3 A1 — à compléter")


def creer_journaliere(cat: SqlCatalog) -> Table:
    """A2. Crée (ou recharge si elle existe) la table NOM_TABLE au schéma SCHEMA_ICEBERG.

    - namespace "gold", créé s'il n'existe pas ;
    - partitionnement caché par mois de `date` (transform month), champ de partition "mois" ;
    - ordre de tri : ic_id puis date.
    Les lecteurs filtrent sur `date` ; ils n'ont jamais à connaître la colonne de partition.
    """
    raise NotImplementedError("Bloc 3 A2 — à compléter")


@dataclass(frozen=True, slots=True)
class EtatTable:
    n_snapshots: int
    n_fichiers: int               # fichiers de données du snapshot courant
    n_lignes: int                 # lignes visibles dans le snapshot courant
    operations: tuple[str, ...]   # opération de chaque snapshot, du plus ancien au plus récent ("append", ...)


def etat(table: Table) -> EtatTable:
    """A3. Résumé de la table, lu dans les métadonnées (inspect, snapshots) : ne scannez pas les données.

    n_lignes = somme des record_count des fichiers du snapshot courant (table sans suppressions).
    Table vide (aucun snapshot) : 0 snapshot, 0 fichier, 0 ligne, ().
    """
    raise NotImplementedError("Bloc 3 A3 — à compléter")


def lire_snapshot(table: Table, snapshot_id: int) -> pa.Table:
    """A4. Time travel : le contenu de la table telle qu'elle était au snapshot `snapshot_id`,
    trié par (ic_id, date)."""
    raise NotImplementedError("Bloc 3 A4 — à compléter")


def snapshot_au(table: Table, horodatage_ms: int) -> int | None:
    """A4 (suite). L'identifiant du snapshot courant à l'instant `horodatage_ms` (ms depuis l'epoch),
    None si la table n'existait pas encore. Indice : snapshot_as_of_timestamp."""
    raise NotImplementedError("Bloc 3 A4 — à compléter")


def faire_evoluer_schema(table: Table) -> None:
    """A5. Schema evolution, sans réécrire aucun fichier de données :
    - ajouter la colonne optionnelle `n_obs` (entier 64 bits) ;
    - renommer `rr` en `rr_mm`.
    Les anciens snapshots restent lisibles avec leur schéma d'origine.
    """
    raise NotImplementedError("Bloc 3 A5 — à compléter")
