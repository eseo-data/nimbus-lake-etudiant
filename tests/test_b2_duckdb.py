"""Bloc 2, partie A — DuckDB, profils, agrégation mensuelle."""

from collections import defaultdict
from datetime import date
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from nimbus_lake.b2_duckdb import (
    SCHEMA_MENSUEL,
    connecter,
    gold_mensuel,
    lire_parquet,
    profiler,
    reglage,
    requete_a2,
)


def test_a1_reglages(tmp_path: Path) -> None:
    con = connecter("300MB", threads=1, temp=tmp_path)
    assert reglage(con, "threads") == "1"
    assert reglage(con, "memory_limit").replace(" ", "") in {"286.1MiB", "300.0MB", "300MB"}
    assert Path(reglage(con, "temp_directory")) == tmp_path


def test_a2_a3_profil_d_une_requete(journalier_parquet: Path) -> None:
    con = connecter()
    sql = requete_a2(lire_parquet(journalier_parquet))
    p = profiler(con, sql)
    attendu = con.sql(sql).fetchall()
    assert p.lignes_resultat == len(attendu)
    assert "TABLE_SCAN" in p.operateurs and "HASH_GROUP_BY" in p.operateurs
    assert p.operateurs[0] == "ORDER_BY"
    assert p.lignes_scannees == pq.ParquetFile(journalier_parquet).metadata.num_rows
    assert p.fichiers_lus == 1
    assert p.latence_s > 0 and p.pic_memoire_octets >= 0 and p.pic_temp_octets == 0
    assert attendu == sorted(attendu, key=lambda r: (-r[1], r[0]))


def test_a3_partition_pruning_visible_dans_le_profil(silver_massif_petit: Path) -> None:
    con = connecter()
    source = lire_parquet(silver_massif_petit / "**" / "*.parquet")
    tout = profiler(con, f"SELECT count(*) FROM {source}")
    une_annee = profiler(con, f"SELECT count(*) FROM {source} WHERE annee = 2017")
    assert tout.fichiers_lus == 2 and une_annee.fichiers_lus == 1
    assert une_annee.lignes_scannees < tout.lignes_scannees


def _reference(table: pa.Table) -> list[dict[str, object]]:
    mois: dict[tuple[date, str], list[dict[str, object]]] = defaultdict(list)
    for r in table.to_pylist():
        mois[(r["date"].replace(day=1), r["ic_id"])].append(r)
    sortie = []
    for (m, ic_id), jours in sorted(mois.items()):
        tns = [j for j in jours if j["tn"] is not None]
        txs = [j for j in jours if j["tx"] is not None]
        tms = [(j["tn"] + j["tx"]) / 2 for j in jours if j["tn"] is not None and j["tx"] is not None]
        rrs = [j["rr"] for j in jours if j["rr"] is not None]
        tnn = min((j["tn"] for j in tns), default=None)
        txx = max((j["tx"] for j in txs), default=None)
        sortie.append({
            "ic_id": ic_id, "mois": m,
            "tnm": sum(j["tn"] for j in tns) / len(tns) if tns else None,
            "txm": sum(j["tx"] for j in txs) / len(txs) if txs else None,
            "tmm": sum(tms) / len(tms) if tms else None,
            "rr": sum(rrs) if rrs else None,
            "tnn": tnn, "tnn_date": min((j["date"] for j in tns if j["tn"] == tnn), default=None),
            "txx": txx, "txx_date": min((j["date"] for j in txs if j["tx"] == txx), default=None),
            "nb_jours_gel": sum(1 for j in tns if j["tn"] < 0), "n_jours": len(jours),
        })
    return sortie


def test_a4_gold_mensuel_conforme(journalier_parquet: Path) -> None:
    resultat = gold_mensuel(connecter(), lire_parquet(journalier_parquet))
    assert resultat.schema.equals(SCHEMA_MENSUEL)
    attendu = _reference(pq.read_table(journalier_parquet))
    obtenu = resultat.to_pylist()
    assert len(obtenu) == len(attendu)
    for o, a in zip(obtenu, attendu):
        for champ, valeur in a.items():
            assert o[champ] == (pytest.approx(valeur) if isinstance(valeur, float) else valeur), (a["ic_id"], a["mois"], champ)


def test_a4_egalites_et_nulls() -> None:
    con = connecter()
    jours = pa.table({
        "ic_id": ["S1"] * 4 + ["S2"],
        "date": [date(2024, 1, 3), date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 4), date(2024, 2, 1)],
        "tn": [-2.0, -2.0, 1.0, None, None],
        "tx": [8.0, 5.0, 8.0, 3.0, None],
        "rr": [None, None, None, None, None],
    })
    con.register("jours", jours)
    r = gold_mensuel(con, "jours").to_pylist()
    assert r[0]["tnn_date"] == date(2024, 1, 1)          # égalité : la date la plus ancienne
    assert r[0]["txx_date"] == date(2024, 1, 2)
    assert r[0]["nb_jours_gel"] == 2 and r[0]["n_jours"] == 4 and r[0]["rr"] is None
    assert r[1]["tnn"] is None and r[1]["tnn_date"] is None and r[1]["txx_date"] is None
