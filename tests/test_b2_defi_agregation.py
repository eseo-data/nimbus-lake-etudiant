"""Bloc 2, défi ★★★ — agrégation hors mémoire écrite à la main."""

import ast
from collections.abc import Iterator
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest

import nimbus_lake.b2_defi_agregation as defi
from nimbus_lake.b2_defi_agregation import SCHEMA_ENTREE, Budget, agreger_hors_memoire
from nimbus_lake.donnees import silver_massif


def _entree(n_stations: int) -> pa.Table:
    t = pa.concat_tables(silver_massif(graine=2, n_stations=n_stations, n_annees=1))
    return pa.table({"ic_id": t["ic_id"], "date": t["ts"].cast(pa.date32()), "t": t["t_c"]}, schema=SCHEMA_ENTREE)


def _lancer(table: pa.Table, budget: Budget) -> pa.Table:
    def lots() -> Iterator[pa.RecordBatch]:
        yield from table.to_batches(max_chunksize=1000)
    try:
        return pa.Table.from_batches(list(agreger_hors_memoire(lots(), budget)), schema=defi.SCHEMA_SORTIE)
    except NotImplementedError:
        pytest.skip("défi non traité")


def _duckdb(table: pa.Table) -> list[tuple[object, ...]]:
    con = duckdb.connect()
    con.register("e", table)
    return con.execute("SELECT ic_id, date, min(t), max(t), count(t) FROM e GROUP BY ALL ORDER BY ALL").fetchall()


def test_d0_pas_de_raccourci() -> None:
    arbre = ast.parse(Path(defi.__file__).read_text(encoding="utf-8"))
    for noeud in ast.walk(arbre):
        if isinstance(noeud, (ast.Import, ast.ImportFrom)):
            noms = {a.name for a in noeud.names} | {getattr(noeud, "module", None) or ""}
            assert not any(n.split(".")[0] in {"duckdb", "polars", "pandas"} for n in noms)
        if isinstance(noeud, ast.Attribute):
            assert noeud.attr not in {"group_by", "concat_tables", "read_all"}, noeud.attr


def test_d1_sans_debordement_quand_tout_tient() -> None:
    table = _entree(3)
    budget = Budget(10_000)
    resultat = _lancer(table, budget)
    assert sorted(resultat.to_pylist(), key=lambda r: (r["ic_id"], r["date"])) and budget.courant == 0
    assert sorted(tuple(r.values()) for r in resultat.to_pylist()) == _duckdb(table)
    assert budget.pic == len(_duckdb(table))


def test_d1_deborde_et_respecte_le_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    ecrites: list[int] = []
    origine = defi.Deversoir.ecrire

    def espion(self: defi.Deversoir, p: int, lot: pa.RecordBatch) -> None:
        ecrites.append(lot.num_rows)
        origine(self, p, lot)

    monkeypatch.setattr(defi.Deversoir, "ecrire", espion)
    table = _entree(6)                                    # 6 × 365 = 2 190 groupes
    budget = Budget(300)
    resultat = _lancer(table, budget)
    assert budget.pic <= 300 and budget.courant == 0
    assert sum(ecrites) > 0
    assert sorted(tuple(r.values()) for r in resultat.to_pylist()) == _duckdb(table)


def test_d1_budget_impossible() -> None:
    try:
        list(agreger_hors_memoire(iter(_entree(1).to_batches()), Budget(0)))
    except NotImplementedError:
        pytest.skip("défi non traité")
    except RuntimeError:
        return
    raise AssertionError("un budget nul doit lever RuntimeError")
