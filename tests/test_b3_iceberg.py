"""Bloc 3, partie A — table Iceberg, snapshots, time travel, schema evolution."""

import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.transforms import MonthTransform

from nimbus_lake.b3_iceberg import (
    NOM_TABLE,
    SCHEMA_ARROW,
    catalogue,
    creer_journaliere,
    etat,
    faire_evoluer_schema,
    lire_snapshot,
    snapshot_au,
)
from nimbus_lake.donnees import journalier_synthetique


def _donnees() -> pa.Table:
    return journalier_synthetique(graine=4, n_stations=6, n_annees=1).cast(SCHEMA_ARROW)


def test_a1_a2_table_creee(entrepot: Path) -> None:
    cat = catalogue(entrepot)
    assert (entrepot / "catalogue.db").exists()
    table = creer_journaliere(cat)
    assert cat.table_exists(NOM_TABLE)
    champ = table.spec().fields[0]
    assert isinstance(champ.transform, MonthTransform) and champ.name == "mois"
    assert table.schema().find_field(champ.source_id).name == "date"
    assert [table.schema().find_field(f.source_id).name for f in table.sort_order().fields] == ["ic_id", "date"]
    assert creer_journaliere(catalogue(entrepot)).metadata_location == table.metadata_location   # rechargée, pas recréée


def test_a3_etat_lu_dans_les_metadonnees(entrepot: Path) -> None:
    table = creer_journaliere(catalogue(entrepot))
    assert etat(table) == etat(table).__class__(0, 0, 0, ())
    donnees = _donnees()
    table.append(donnees.slice(0, 1000))
    table.append(donnees.slice(1000))
    e = etat(table)
    assert e.n_snapshots == 2 and e.operations == ("append", "append")
    assert e.n_lignes == donnees.num_rows
    assert e.n_fichiers >= 12                        # au moins un fichier par mois


def test_a4_time_travel(entrepot: Path) -> None:
    table = creer_journaliere(catalogue(entrepot))
    donnees = _donnees()
    table.append(donnees.slice(0, 500))
    premier = table.current_snapshot()
    assert premier is not None
    time.sleep(0.01)
    table.append(donnees.slice(500))
    ancien = lire_snapshot(table, premier.snapshot_id)
    assert ancien.num_rows == 500
    assert ancien.equals(donnees.slice(0, 500).sort_by([("ic_id", "ascending"), ("date", "ascending")]))
    assert snapshot_au(table, premier.timestamp_ms) == premier.snapshot_id
    assert snapshot_au(table, premier.timestamp_ms - 60_000) is None


def test_a5_schema_evolution_sans_reecriture(entrepot: Path) -> None:
    table = creer_journaliere(catalogue(entrepot))
    table.append(_donnees())
    premier = table.current_snapshot()
    assert premier is not None
    fichiers_avant = set(table.inspect.files()["file_path"].to_pylist())
    faire_evoluer_schema(table)
    noms = [f.name for f in table.schema().fields]
    assert noms == ["ic_id", "date", "tn", "tx", "rr_mm", "n_obs"]
    assert set(table.inspect.files()["file_path"].to_pylist()) == fichiers_avant      # rien réécrit
    courant = table.scan().to_arrow()
    assert courant["n_obs"].null_count == courant.num_rows
    assert pc.sum(courant["rr_mm"]).as_py() is not None
    assert "rr" in table.scan(snapshot_id=premier.snapshot_id).to_arrow().column_names
