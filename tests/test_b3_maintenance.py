"""Bloc 3, partie C — compaction, expiration, partition evolution."""

from pathlib import Path

import pytest

from nimbus_lake.b3_iceberg import SCHEMA_ARROW, catalogue, creer_journaliere, etat
from nimbus_lake.b3_maintenance import compacter, expirer, passer_a_l_annee, specs_des_fichiers, taille_metadonnees
from nimbus_lake.donnees import livraisons

CLE = [("ic_id", "ascending"), ("date", "ascending")]


@pytest.fixture()
def table_publiee(entrepot: Path, publier_reference):  # type: ignore[no-untyped-def]
    table = creer_journaliere(catalogue(entrepot))
    publier_reference(table, livraisons(graine=8, n_stations=6, n_jours=40))     # juin et juillet
    return table


def test_c1_compaction(table_publiee) -> None:  # type: ignore[no-untyped-def]
    avant = etat(table_publiee)
    contenu = table_publiee.scan().to_arrow().sort_by(CLE)
    n = compacter(table_publiee)
    assert n <= 2 < avant.n_fichiers                             # un fichier par mois au plus
    assert table_publiee.scan().to_arrow().sort_by(CLE).equals(contenu)
    apres = etat(table_publiee)
    assert {"delete", "overwrite"} & set(apres.operations[avant.n_snapshots:])   # une seule transaction


def test_c2_expiration(table_publiee) -> None:  # type: ignore[no-untyped-def]
    ancien = sorted(table_publiee.snapshots(), key=lambda s: s.timestamp_ms)[0].snapshot_id
    n = len(table_publiee.snapshots())
    taille = taille_metadonnees(table_publiee)
    assert expirer(table_publiee, 3) == n - 3
    assert len(table_publiee.snapshots()) == 3
    assert table_publiee.snapshot_by_id(ancien) is None
    assert taille_metadonnees(table_publiee) >= 0 and taille > 0
    assert expirer(table_publiee, 10) == 0


def test_c3_partition_evolution(table_publiee) -> None:  # type: ignore[no-untyped-def]
    contenu = table_publiee.scan().to_arrow().sort_by(CLE)
    passer_a_l_annee(table_publiee)
    assert [f.name for f in table_publiee.spec().fields] == ["annee"] and len(table_publiee.specs()) == 2
    assert table_publiee.scan().to_arrow().sort_by(CLE).equals(contenu)
    assert set(specs_des_fichiers(table_publiee)) == {0}                 # l'existant n'est pas réécrit
    table_publiee.append(contenu.slice(0, 10).cast(SCHEMA_ARROW))
    assert set(specs_des_fichiers(table_publiee)) == {0, 1}
