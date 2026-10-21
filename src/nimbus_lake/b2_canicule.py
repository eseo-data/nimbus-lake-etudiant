"""Bloc 2, partie B — reprendre le script hérité `b2_legacy.py` en une requête DuckDB.

Spécification des indicateurs de chaleur, par station (ic_id) et année civile (annee) :

- n_jours_tx30       : jours avec tx >= 30 °C ;
- n_nuits_tropicales : jours avec tn >= 20 °C ;
- un épisode chaud   : au moins 3 DATES CALENDAIRES CONSÉCUTIVES avec tx >= 30 °C, dans la
  même année. Un jour absent (pas de ligne) ou un tx nul interrompt l'épisode ;
- n_episodes         : nombre d'épisodes ;
- plus_long_episode  : longueur du plus long, 0 s'il n'y en a pas ;
- debut_plus_long    : date de début du plus long ; à longueur égale, le plus ancien ;
  null s'il n'y a pas d'épisode ;
- une ligne par (ic_id, annee) présent dans la source, résultat conforme à
  SCHEMA_CHALEUR, trié par ic_id puis annee.

Critères de réussite (tests/test_b2_canicule.py) :
1. le résultat respecte la spécification (implémentation de référence naïve dans les tests) ;
2. il est identique au script hérité sur les stations sans jour manquant ;
3. une seule requête SQL, avec une window function (le plan contient un opérateur WINDOW) ;
4. au banc d'essai (`uv run python -m nimbus_lake.b2_canicule 200`), comparer avec le legacy.
"""

from __future__ import annotations

import sys
import time

import duckdb
import pyarrow as pa

SCHEMA_CHALEUR = pa.schema([
    pa.field("ic_id", pa.string()),
    pa.field("annee", pa.int32()),
    pa.field("n_jours_tx30", pa.int64()),
    pa.field("n_nuits_tropicales", pa.int64()),
    pa.field("n_episodes", pa.int64()),
    pa.field("plus_long_episode", pa.int64()),
    pa.field("debut_plus_long", pa.date32()),
])


def requete_chaleur(source: str) -> str:
    """B1. Le texte de LA requête SQL qui calcule les indicateurs sur `source`.

    `source` est une expression de table (nom de table ou read_parquet(...)) aux colonnes
    ic_id, date, tn, tx. Indice : « gaps and islands ». Sur les seuls jours chauds d'une
    station, `date - row_number() OVER (...)` est constant le long d'une suite de dates
    consécutives.
    """
    raise NotImplementedError("Bloc 2 B1 — à compléter")


def indicateurs_chaleur(con: duckdb.DuckDBPyConnection, source: str) -> pa.Table:
    """B2. Exécute `requete_chaleur(source)` et rend une table conforme à SCHEMA_CHALEUR."""
    raise NotImplementedError("Bloc 2 B2 — à compléter")


def plus_longs_episodes(con: duckdb.DuckDBPyConnection, source: str, n: int) -> pa.Table:
    """Bonus. Les `n` plus longs épisodes chauds toutes stations confondues.

    Colonnes : ic_id, debut (date), fin (date), duree (BIGINT), tx_max (DOUBLE, max de tx
    pendant l'épisode). Tri : duree décroissante, puis debut, puis ic_id. Utilisez QUALIFY
    ou une window de rang plutôt qu'un LIMIT appliqué après coup à une sous-requête.
    """
    raise NotImplementedError("Bloc 2 Bonus — à compléter")


def main() -> None:  # pragma: no cover - coquille impérative
    """Fourni. Banc d'essai legacy contre DuckDB.

        uv run python -m nimbus_lake.b2_canicule 200
    """
    import tempfile
    from pathlib import Path

    import pyarrow.parquet as pq

    from nimbus_lake import b2_legacy
    from nimbus_lake.donnees import journalier_synthetique

    n_stations = int(sys.argv[1]) if len(sys.argv) > 1 else 200
    with tempfile.TemporaryDirectory() as d:
        chemin = Path(d) / "journalier.parquet"
        table = journalier_synthetique(n_stations=n_stations)
        pq.write_table(table, chemin)
        print(f"{table.num_rows:,} lignes journalières, {n_stations} stations".replace(",", " "))
        debut = time.perf_counter()
        b2_legacy.executer(chemin)  # type: ignore[no-untyped-call]
        legacy = time.perf_counter() - debut
        con = duckdb.connect()
        debut = time.perf_counter()
        indicateurs_chaleur(con, f"read_parquet('{chemin}')")
        sql = time.perf_counter() - debut
        print(f"legacy : {legacy:.2f} s   duckdb : {sql:.3f} s   rapport : {legacy / sql:.0f}×")


if __name__ == "__main__":  # pragma: no cover
    main()
