"""Bloc 3, partie B — publier le Gold sur une table Iceberg, atomiquement.

Spécification :
- une livraison (SCHEMA_LIVRAISON) peut contenir plusieurs versions d'une même ligne
  (même ic_id et même date) : celle de plus grand `recu_a` fait foi ;
- la publication insère les lignes nouvelles et remplace les lignes existantes (upsert sur
  la clé (ic_id, date)) ;
- elle est ATOMIQUE : soit toute la livraison est visible, soit rien. Le paramètre `panne`
  simule une coupure : appelez-le après avoir préparé l'écriture et AVANT de la valider ;
  s'il lève une exception, la table doit rester exactement dans son état précédent ;
- publier deux fois la même livraison ne change pas les données (idempotence) ;
- on peut annuler une publication erronée en revenant au snapshot précédent.

Critères de réussite (tests/test_b3_publication.py) : contenu final identique à une
référence naïve après 30 livraisons ; table inchangée après une panne ; le legacy, lui,
perd des mois avec la même panne.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass

import duckdb
import pyarrow as pa
from pyiceberg.table import Table

from nimbus_lake.b3_iceberg import SCHEMA_ARROW


def _rien() -> None:
    return None


def dedoublonner(livraison: pa.Table) -> pa.Table:
    """B1. Une ligne par (ic_id, date) : la version de plus grand `recu_a`.

    Résultat au SCHEMA_ARROW (sans `recu_a`), trié par (ic_id, date). DuckDB et QUALIFY
    conviennent bien (la table Arrow se lit directement par son nom de variable).
    """
    raise NotImplementedError("Bloc 3 B1 — à compléter")


@dataclass(frozen=True, slots=True)
class Publication:
    snapshot_avant: int | None
    snapshot_apres: int | None
    lignes_inserees: int
    lignes_mises_a_jour: int


def publier(table: Table, livraison: pa.Table, panne: Callable[[], None] = _rien) -> Publication:
    """B2. Upsert de la livraison dédoublonnée sur la clé (ic_id, date), dans UNE transaction.

    Ouvrez `table.transaction()`, faites l'upsert dedans, appelez `panne()`, puis laissez la
    transaction se valider en sortie du bloc `with`.
    """
    raise NotImplementedError("Bloc 3 B2 — à compléter")


def publier_tout(table: Table, lots: Iterable[pa.Table]) -> list[Publication]:
    """Fourni. Publie les livraisons dans l'ordre."""
    return [publier(table, lot) for lot in lots]


def annuler(table: Table, snapshot_id: int) -> None:
    """B3. Revenir à `snapshot_id` (rollback) : le snapshot courant de la branche main redevient
    `snapshot_id`. Les snapshots plus récents restent dans l'historique."""
    raise NotImplementedError("Bloc 3 B3 — à compléter")


def publier_avec_audit(table: Table, livraison: pa.Table, controle: Callable[[pa.Table], bool]) -> bool:
    """Bonus. Write-Audit-Publish avec une branche.

    1. créer la branche "audit" sur le snapshot courant ;
    2. y publier la livraison (upsert sur la branche, `main` ne bouge pas) ;
    3. lire la branche et appliquer `controle` à son contenu ;
    4. si le contrôle passe, faire pointer `main` sur le snapshot de la branche ; sinon `main`
       reste inchangé ;
    5. supprimer la branche dans les deux cas. Renvoie le résultat du contrôle.
    Suppose une table qui a déjà au moins un snapshot.
    """
    raise NotImplementedError("Bloc 3 Bonus — à compléter")
