"""Bloc 1, partie A — Parquet de l'intérieur.

Un fichier Parquet est découpé en row groups ; chaque row group stocke chaque
colonne séparément (column chunk), encodée puis compressée. Le footer, à la fin du
fichier, contient le schéma et, pour chaque column chunk, des statistiques min/max.
Un moteur lit le footer d'abord et saute les row groups dont les statistiques
excluent le prédicat : c'est le predicate pushdown au niveau fichier.

Exercices A1 à A7. Tests : `uv run pytest tests/test_b1_parquet.py`.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

# Schéma Bronze du CSV Météo-France. NUM_POSTE est un identifiant, pas un nombre :
# « 01918007 » doit garder son zéro initial. AAAAMMJJHH reste un texte au Bronze.
SCHEMA_MF_BRUT = pa.schema([
    pa.field("NUM_POSTE", pa.string()),
    pa.field("AAAAMMJJHH", pa.string()),
    pa.field("T", pa.float64()),
    pa.field("U", pa.float64()),
    pa.field("PMER", pa.float64()),
    pa.field("RR1", pa.float64()),
])


def lire_bronze_mf(chemin: Path) -> pa.Table:
    """A1. Lit un CSV Météo-France (séparateur « ; ») avec le schéma SCHEMA_MF_BRUT.

    Ne laissez pas pyarrow inférer les types : il ferait de NUM_POSTE un entier.
    Une cellule vide devient null.
    """
    raise NotImplementedError("Bloc 1 A1 — à compléter")


def ecrire_parquet(table: pa.Table, chemin: Path, compression: str | None,
                   lignes_par_row_group: int) -> int:
    """A2. Écrit `table` en Parquet et renvoie la taille du fichier en octets.

    `compression` : None, "snappy" ou "zstd".
    """
    raise NotImplementedError("Bloc 1 A2 — à compléter")


@dataclass(frozen=True, slots=True)
class InfosParquet:
    n_lignes: int
    n_row_groups: int
    lignes_par_row_group: tuple[int, ...]
    colonnes: tuple[str, ...]
    codecs: frozenset[str]          # ex. frozenset({"ZSTD"})
    taille_footer: int              # octets du footer sérialisé (FileMetaData)


def inspecter(chemin: Path) -> InfosParquet:
    """A3. Lit uniquement le footer (pq.ParquetFile(...).metadata) et le résume."""
    raise NotImplementedError("Bloc 1 A3 — à compléter")


def bornes_row_groups(chemin: Path, colonne: str) -> list[tuple[object, object]]:
    """A4. (min, max) de `colonne` pour chaque row group, lus dans les statistiques du footer."""
    raise NotImplementedError("Bloc 1 A4 — à compléter")


def row_groups_candidats(chemin: Path, colonne: str, valeur: object) -> list[int]:
    """A5. Indices des row groups qui PEUVENT contenir `colonne == valeur`.

    C'est le raisonnement d'un moteur avant de lire les données : un row group dont
    l'intervalle [min, max] exclut la valeur est sauté sans être décompressé.
    """
    raise NotImplementedError("Bloc 1 A5 — à compléter")


def tailles_par_format(table: pa.Table, dossier: Path) -> dict[str, int]:
    """A6. Écrit `table` en CSV puis en Parquet sans compression, snappy et zstd.

    Renvoie {"csv": ..., "parquet_none": ..., "parquet_snappy": ..., "parquet_zstd": ...}
    en octets. Row groups de 100 000 lignes.
    """
    raise NotImplementedError("Bloc 1 A6 — à compléter")


def effet_du_tri(table: pa.Table, colonne: str, valeur: object, dossier: Path,
                 lignes_par_row_group: int) -> tuple[int, int]:
    """A7. Nombre de row groups candidats pour `colonne == valeur`, sans puis avec tri.

    Écrit la table telle quelle, puis triée sur `colonne`, et compare. Renvoie
    (candidats_sans_tri, candidats_avec_tri).
    """
    raise NotImplementedError("Bloc 1 A7 — à compléter")
