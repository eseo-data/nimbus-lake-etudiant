"""Bloc 3, défi ★★★ — lister les fichiers d'un snapshot sans bibliothèque Iceberg ni Avro."""

import ast
from pathlib import Path

import pytest

import nimbus_lake.b3_defi_avro as defi
from nimbus_lake.b3_iceberg import catalogue, creer_journaliere
from nimbus_lake.donnees import livraisons

INTERDITS = {"pyiceberg", "fastavro", "avro", "pyarrow", "duckdb", "polars", "pandas"}


def test_d0_bibliotheque_standard_seulement() -> None:
    for noeud in ast.walk(ast.parse(Path(defi.__file__).read_text(encoding="utf-8"))):
        if isinstance(noeud, ast.Import):
            racines = {a.name.split(".")[0] for a in noeud.names}
        elif isinstance(noeud, ast.ImportFrom):
            racines = {(noeud.module or "").split(".")[0]}
        else:
            continue
        assert not racines & INTERDITS, racines & INTERDITS


def test_d3_fichiers_du_snapshot_courant_et_passe(entrepot: Path, publier_reference) -> None:  # type: ignore[no-untyped-def]
    table = creer_journaliere(catalogue(entrepot))
    publier_reference(table, livraisons(graine=9, n_stations=4, n_jours=35))
    metadata = Path(table.metadata_location.removeprefix("file://"))
    snapshots = sorted(table.snapshots(), key=lambda s: s.timestamp_ms)
    try:
        courant = defi.fichiers_du_snapshot(metadata)
    except NotImplementedError:
        pytest.skip("défi non traité")
    attendu = sorted(t.file.file_path for t in table.scan().plan_files())
    assert courant == attendu
    for s in (snapshots[0], snapshots[len(snapshots) // 2]):
        assert defi.fichiers_du_snapshot(metadata, s.snapshot_id) == sorted(
            t.file.file_path for t in table.scan(snapshot_id=s.snapshot_id).plan_files())
    with pytest.raises(KeyError):
        defi.fichiers_du_snapshot(metadata, 123)


def test_d2_conteneur_avro(entrepot: Path, publier_reference) -> None:  # type: ignore[no-untyped-def]
    table = creer_journaliere(catalogue(entrepot))
    publier_reference(table, livraisons(graine=9, n_stations=3, n_jours=3))
    snap = table.current_snapshot()
    assert snap is not None
    try:
        manifests = defi.lire_conteneur(Path(snap.manifest_list.removeprefix("file://")))
    except NotImplementedError:
        pytest.skip("défi non traité")
    assert [m["manifest_path"] for m in manifests] == [m.manifest_path for m in snap.manifests(table.io)]
    assert all(isinstance(m["added_snapshot_id"], int) for m in manifests)
