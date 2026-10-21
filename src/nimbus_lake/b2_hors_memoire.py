"""Bloc 2, partie C — plus gros que la mémoire : 29 millions de lignes avec 64 Mo.

Quand une agrégation ou un tri dépasse `memory_limit`, DuckDB écrit des morceaux
intermédiaires dans `temp_directory` puis les relit (spilling). La requête ralentit mais
aboutit. Certaines opérations ne savent pas déborder et échouent avec
`duckdb.OutOfMemoryException`. Le profil (A3) donne le pic de mémoire et le volume écrit
sur disque.

Préparation (une fois, environ 15 s et 300 Mo pour 1 000 stations) :

    uv run python -m nimbus_lake.donnees --silver-massif 1000

Exercices C1 à C3. Tests : `uv run pytest tests/test_b2_hors_memoire.py`.
Banc d'essai : `uv run python -m nimbus_lake.b2_hors_memoire data/silver_massif_1000`.
"""

from __future__ import annotations

import sys
import tempfile
from collections.abc import Sequence
from pathlib import Path

import duckdb

from nimbus_lake.b2_duckdb import Profil, connecter, lire_parquet, profiler


def requete_journaliere(racine_silver: Path) -> str:
    """C1. SQL : Silver massif -> climatologie journalière.

    Une ligne par (ic_id, date UTC) : tn = min(t_c), tx = max(t_c), rr = somme de rr1_mm,
    n_obs = nombre de t_c non nuls. Colonnes dans cet ordre : ic_id, date, tn, tx, rr, n_obs.
    Pas d'ORDER BY : on veut mesurer l'agrégation seule.
    """
    raise NotImplementedError("Bloc 2 C1 — à compléter")


def ecrire_journalier(con: duckdb.DuckDBPyConnection, racine_silver: Path, sortie: Path) -> int:
    """C1 (suite). Écrit le résultat de `requete_journaliere` en un fichier Parquet zstd, avec COPY.

    Renvoie le nombre de lignes écrites. Les données ne doivent pas transiter par Python.
    """
    raise NotImplementedError("Bloc 2 C1 — à compléter")


def mesurer(sql: str, memoire: str, threads: int, temp: Path) -> Profil:
    """C2. Profil de `sql` exécuté sur une connexion neuve limitée à `memoire` et `threads`.

    Utilisez `connecter` (A1) et `profiler` (A3). Ajoutez `SET preserve_insertion_order = false`,
    qui laisse DuckDB réordonner librement et réduit la mémoire nécessaire.
    """
    raise NotImplementedError("Bloc 2 C2 — à compléter")


def plus_petite_memoire(sql: str, candidats: Sequence[str], threads: int, temp: Path) -> str | None:
    """C3. Le plus petit `memory_limit` de `candidats` (triés du plus petit au plus grand) qui laisse
    `sql` aboutir. None si aucun ne suffit.

    Une requête qui ne tient pas lève duckdb.OutOfMemoryException : interceptez CETTE
    exception seulement, pas Exception.
    """
    raise NotImplementedError("Bloc 2 C3 — à compléter")


def main() -> None:  # pragma: no cover - coquille impérative
    """Fourni. Banc d'essai : agrégation et tri complet sous différentes limites de mémoire."""
    racine = Path(sys.argv[1] if len(sys.argv) > 1 else "data/silver_massif_1000")
    threads = int(sys.argv[2]) if len(sys.argv) > 2 else 2
    source = lire_parquet(racine / "**" / "*.parquet")
    requetes = {
        "agrégation": requete_journaliere(racine),
        "tri complet": f"SELECT * FROM {source} ORDER BY t_c DESC, ts",
    }
    print(f"{'requête':<12} {'memory_limit':>12} {'durée s':>8} {'pic mém. Mo':>12} {'débordé Mo':>11}")
    for nom, sql in requetes.items():
        for memoire in ("4GB", "512MB", "128MB", "64MB"):
            with tempfile.TemporaryDirectory() as d:
                try:
                    p = mesurer(sql, memoire, threads, Path(d))
                    print(f"{nom:<12} {memoire:>12} {p.latence_s:>8.1f} {p.pic_memoire_octets / 2**20:>12.0f} "
                          f"{p.pic_temp_octets / 2**20:>11.0f}")
                except duckdb.OutOfMemoryException:
                    print(f"{nom:<12} {memoire:>12} {'échec : OutOfMemoryException':>33}")


if __name__ == "__main__":  # pragma: no cover
    main()
