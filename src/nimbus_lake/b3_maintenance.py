"""Bloc 3, partie C — des publications quotidiennes : petits fichiers, snapshots, maintenance.

Chaque publication crée un snapshot et de nouveaux fichiers. Au bout de quelques mois : des
centaines de snapshots, des milliers de petits fichiers, des métadonnées qui grossissent.
Une table Iceberg s'entretient :
- compaction : réécrire les données en peu de gros fichiers (le contenu ne change pas) ;
- expiration des snapshots : oublier l'historique ancien (le time travel n'y va plus) ;
- évolution du partitionnement : changer de spec sans réécrire l'existant.

Exercices C1 à C3. Tests : `uv run pytest tests/test_b3_maintenance.py`.
Banc d'essai : `uv run python -m nimbus_lake.b3_maintenance 50 60`.
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyiceberg.table import Table
from pyiceberg.transforms import YearTransform


def taille_metadonnees(table: Table) -> int:
    """Fourni. Octets du dossier metadata/ de la table (metadata.json, manifest lists, manifests)."""
    dossier = Path(table.location().removeprefix("file://")) / "metadata"
    return sum(p.stat().st_size for p in dossier.iterdir() if p.is_file())


def compacter(table: Table) -> int:
    """C1. Réécrit tout le contenu courant en une seule transaction (`table.overwrite`), sans changer les données.

    Lisez le snapshot courant, triez par (ic_id, date), réécrivez avec `table.overwrite`.
    Renvoie le nombre de fichiers de données après compaction (au plus un par partition).
    """
    raise NotImplementedError("Bloc 3 C1 — à compléter")


def expirer(table: Table, garder: int) -> int:
    """C2. Expire tous les snapshots sauf les `garder` plus récents (le snapshot courant est toujours
    gardé). Renvoie le nombre de snapshots expirés. Indice : table.maintenance.expire_snapshots().by_ids."""
    raise NotImplementedError("Bloc 3 C2 — à compléter")


def passer_a_l_annee(table: Table) -> None:
    """C3. Partition evolution : remplacer le champ de partition "mois" (month(date)) par "annee"
    (year(date)). Aucun fichier existant n'est réécrit ; les nouvelles écritures suivent la nouvelle spec."""
    raise NotImplementedError("Bloc 3 C3 — à compléter")


def specs_des_fichiers(table: Table) -> dict[int, int]:
    """Fourni. Nombre de fichiers de données du snapshot courant par spec_id."""
    compte: dict[int, int] = {}
    for spec_id in table.inspect.files()["spec_id"].to_pylist():
        compte[int(spec_id)] = compte.get(int(spec_id), 0) + 1
    return compte


def main() -> None:  # pragma: no cover - coquille impérative
    """Fourni. n_jours publications quotidiennes (défaut : 60 jours, 50 stations), puis maintenance."""
    import tempfile
    import time

    from nimbus_lake.b3_iceberg import catalogue, creer_journaliere, etat
    from nimbus_lake.b3_publication import publier
    from nimbus_lake.donnees import livraisons

    n_stations = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    n_jours = int(sys.argv[2]) if len(sys.argv) > 2 else 60
    lots = livraisons(n_stations=n_stations, n_jours=n_jours)
    with tempfile.TemporaryDirectory() as d:
        table = creer_journaliere(catalogue(Path(d)))
        debut = time.perf_counter()
        for lot in lots:
            publier(table, lot)
        duree = time.perf_counter() - debut
        e = etat(table)
        print(f"{n_jours} publications : {duree:.1f} s ({1000 * duree / n_jours:.0f} ms chacune)")
        print(f"avant maintenance : {e.n_snapshots} snapshots, {e.n_fichiers} fichiers, "
              f"{e.n_lignes} lignes, métadonnées {taille_metadonnees(table) / 1e6:.1f} Mo")
        debut = time.perf_counter()
        apres = compacter(table)
        print(f"compaction : {apres} fichiers en {time.perf_counter() - debut:.1f} s")
        print(f"expiration : {expirer(table, 1)} snapshots expirés")
        e = etat(table)
        print(f"après maintenance : {e.n_snapshots} snapshots, {e.n_fichiers} fichiers, {e.n_lignes} lignes, "
              f"métadonnées {taille_metadonnees(table) / 1e6:.1f} Mo (anciens fichiers non supprimés)")


if __name__ == "__main__":  # pragma: no cover
    main()
