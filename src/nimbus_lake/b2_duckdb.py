"""Bloc 2, partie A — DuckDB : se connecter, lire un plan, écrire une agrégation contractuelle.

DuckDB est un moteur SQL analytique embarqué : une bibliothèque, pas un serveur. Il lit
Parquet, CSV et Arrow directement, exécute par vecteurs de valeurs (et non ligne par
ligne), utilise tous les cœurs, et déborde sur disque quand une opération dépasse la
mémoire autorisée.

`EXPLAIN ANALYZE` exécute la requête et rend l'arbre des opérateurs avec, pour chacun,
le nombre de lignes produites et le temps passé. En `FORMAT JSON`, le même profil se lit
par programme : c'est l'objet de A3.

Exercices A1 à A4. Tests : `uv run pytest tests/test_b2_duckdb.py`.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa

SCHEMA_MENSUEL = pa.schema([
    pa.field("ic_id", pa.string()),
    pa.field("mois", pa.date32()),
    pa.field("tnm", pa.float64()),
    pa.field("txm", pa.float64()),
    pa.field("tmm", pa.float64()),
    pa.field("rr", pa.float64()),
    pa.field("tnn", pa.float64()),
    pa.field("tnn_date", pa.date32()),
    pa.field("txx", pa.float64()),
    pa.field("txx_date", pa.date32()),
    pa.field("nb_jours_gel", pa.int64()),
    pa.field("n_jours", pa.int64()),
])


def connecter(memoire: str = "2GB", threads: int | None = None, temp: Path | None = None
              ) -> duckdb.DuckDBPyConnection:
    """A1. Connexion en mémoire avec `memory_limit`, `threads` (si donné) et `temp_directory` (si donné).

    `memoire` s'écrit comme DuckDB l'attend : "2GB", "512MB"...
    """
    raise NotImplementedError("Bloc 2 A1 — à compléter")


def reglage(con: duckdb.DuckDBPyConnection, nom: str) -> str:
    """Fourni. Valeur courante d'un réglage DuckDB (current_setting)."""
    ligne = con.execute("SELECT current_setting(?)", [nom]).fetchone()
    assert ligne is not None
    return str(ligne[0])


@dataclass(frozen=True, slots=True)
class Profil:
    latence_s: float
    pic_memoire_octets: int          # system_peak_buffer_memory
    pic_temp_octets: int             # system_peak_temp_dir_size : ce qui a débordé sur disque
    lignes_resultat: int             # cardinalité de l'opérateur racine (sous EXPLAIN_ANALYZE)
    lignes_scannees: int             # somme des operator_rows_scanned
    operateurs: tuple[str, ...]      # operator_type, parcours préfixe, sans EXPLAIN_ANALYZE
    fichiers_lus: int                # somme des « Total Files Read » des scans (0 si aucun)


def profiler(con: duckdb.DuckDBPyConnection, sql: str) -> Profil:
    """A3. Exécute `EXPLAIN (ANALYZE, FORMAT JSON) <sql>` et résume le profil.

    Le résultat est une seule ligne ; sa deuxième colonne est le JSON. L'objet racine porte
    latency, system_peak_buffer_memory, system_peak_temp_dir_size ; ses `children` forment
    l'arbre des opérateurs (operator_type, operator_cardinality, operator_rows_scanned,
    extra_info). Le premier enfant est l'opérateur EXPLAIN_ANALYZE lui-même.
    """
    raise NotImplementedError("Bloc 2 A3 — à compléter")


def lire_parquet(motif: str | Path) -> str:
    """Fourni. Fragment SQL qui lit un ou plusieurs fichiers Parquet (partitioning hive compris)."""
    return f"read_parquet('{motif}', hive_partitioning = true, union_by_name = true)"


def gold_mensuel(con: duckdb.DuckDBPyConnection, source: str) -> pa.Table:
    """A4. Climatologie mensuelle à partir d'une climatologie journalière, en UNE requête SQL.

    `source` est une expression de table SQL (un nom de table, ou `lire_parquet(...)`) aux
    colonnes ic_id, date, tn, tx, rr (SCHEMA_JOURNALIER). Contrat inspiré de gold_v2.mensuelle
    sur portail.chom.engineering :

    - mois  : premier jour du mois ;
    - tnm, txm : moyennes des tn et des tx du mois (nulls ignorés) ;
    - tmm   : moyenne de tm = (tn + tx) / 2, calculé seulement les jours où tn et tx existent ;
    - rr    : cumul du mois, null si aucune valeur ;
    - tnn, txx : min des tn, max des tx ; tnn_date, txx_date : leur date. En cas d'égalité,
      la date la PLUS ANCIENNE ; null si le mois n'a aucun tn (resp. tx) ;
    - nb_jours_gel : jours avec tn < 0 ; n_jours : jours présents ;
    - résultat conforme à SCHEMA_MENSUEL, trié par mois puis ic_id.
    """
    raise NotImplementedError("Bloc 2 A4 — à compléter")


def requete_a2(source: str) -> str:
    """A2. Une requête qui compte les jours de tx >= 30 °C par station, triée par nombre décroissant.

    Colonnes : ic_id, n (BIGINT). Servira à lire votre premier plan (A3) : repérez le
    TABLE_SCAN, l'agrégation, le tri, et le nombre de lignes à chaque étage.
    """
    raise NotImplementedError("Bloc 2 A2 — à compléter")
