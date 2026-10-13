"""Bloc 1, partie B — reprise du script hérité."""

import importlib
import json
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

import nimbus_lake.b1_pipeline as pipeline
from nimbus_lake import b1_legacy
from nimbus_lake.b1_pipeline import (
    SCHEMA_GOLD,
    SCHEMA_REF,
    canoniser_mf,
    executer_pipeline,
    gold_journalier,
    lire_bronze_static,
    lire_referentiel,
    silver_par_mois,
)
from nimbus_lake.donnees import BORNES_T, SCHEMA_SILVER

Cle = tuple[str, str]


def _reference(bronze: Path) -> dict[Cle, dict[str, object]]:
    """Implémentation de référence, volontairement naïve : la spécification exécutable."""
    crosswalk: dict[str, str] = {}
    for ligne in (bronze / "ref" / "stations.csv").read_text(encoding="utf-8").splitlines()[1:]:
        champs = ligne.split(",")
        if champs[1]:
            crosswalk[champs[1]] = champs[0]
    obs: dict[tuple[str, datetime], tuple[float | None, float | None]] = {}
    for chemin in sorted((bronze / "mf").glob("*.csv")):
        for ligne in chemin.read_text(encoding="utf-8").splitlines()[1:]:
            poste, horo, t, _u, _p, rr = ligne.split(";")
            ts = datetime.strptime(horo, "%Y%m%d%H").replace(tzinfo=UTC)
            obs[(crosswalk[poste], ts)] = (float(t) if t else None, float(rr) if rr else None)
    for chemin in sorted((bronze / "static").glob("*.jsonl")):
        for ligne in chemin.read_text(encoding="utf-8").splitlines():
            o = json.loads(ligne)
            ts = datetime.fromisoformat(o["dh_utc"]).astimezone(UTC)
            k = o["air_temperature"]
            obs[(o["ic_id"], ts)] = (k - 273.15 if k is not None else None,
                                     o["precip_1h"] if ts.minute == 0 else None)
    jours: dict[Cle, dict[str, list[float]]] = defaultdict(lambda: {"t": [], "rej": [], "rr": []})
    for (ic_id, ts), (t, rr) in obs.items():
        j = jours[(ic_id, ts.date().isoformat())]
        if t is not None:
            (j["t"] if BORNES_T[0] <= t <= BORNES_T[1] else j["rej"]).append(t)
        if rr is not None:
            j["rr"].append(rr)
    resultat: dict[Cle, dict[str, object]] = {}
    for cle, j in jours.items():
        r = len(j["t"])
        resultat[cle] = {
            "tn": min(j["t"]) if j["t"] else None, "tx": max(j["t"]) if j["t"] else None,
            "rr": sum(j["rr"]) if j["rr"] else None, "n_obs": r + len(j["rej"]),
            "n_obs_rejetees": len(j["rej"]),
            "statut_extremes": "quotidien" if r >= 8 else "partiel" if r >= 2
            else "releve_unique" if r == 1 else None,
        }
    return resultat


@pytest.fixture(scope="module")
def lake(bronze: Path, tmp_path_factory: pytest.TempPathFactory) -> Path:
    racine = tmp_path_factory.mktemp("lake")
    executer_pipeline(bronze, racine)
    return racine


@pytest.fixture(scope="module")
def gold(lake: Path) -> pa.Table:
    return pq.read_table(lake / "gold" / "journaliere.parquet")


@pytest.fixture(scope="module")
def legacy(bronze: Path) -> dict[Cle, dict[str, object]]:
    module = importlib.reload(b1_legacy)          # le script hérité accumule dans des globales
    return {(d["ic_id"], d["date"]): d for d in module.executer(str(bronze))}


def _par_cle(table: pa.Table) -> dict[Cle, dict[str, object]]:
    return {(d["ic_id"], d["date"].isoformat()): d for d in table.to_pylist()}


# --- B1 à B3 ----------------------------------------------------------------

def test_b1_referentiel(bronze: Path) -> None:
    ref = lire_referentiel(bronze / "ref" / "stations.csv")
    assert ref.schema.equals(SCHEMA_REF)
    assert ref["mfid"].null_count == 3                       # les 3 stations participatives
    assert ref["dh_ouverture"].null_count >= 1               # sentinelle 0000-00-00 -> null
    assert all(len(i) == 5 for i in ref.filter(pa.compute.equal(ref["genre"], "mf_principale"))
               ["ic_id"].to_pylist())


def test_b2_canoniser_mf(bronze: Path) -> None:
    ref = lire_referentiel(bronze / "ref" / "stations.csv")
    crosswalk = {m: i for m, i in zip(ref["mfid"].to_pylist(), ref["ic_id"].to_pylist()) if m}
    silver = canoniser_mf(pipeline.lire_bronze_mf(bronze / "mf" / "202402.csv"), crosswalk)
    assert silver.schema.equals(SCHEMA_SILVER)
    assert silver["ic_id"].null_count == 0
    assert set(silver["ic_id"].to_pylist()) <= set(crosswalk.values())
    premier = silver["ts"][0].as_py()
    assert premier.tzinfo is not None and premier.utcoffset().total_seconds() == 0


def test_b3_static_kelvin_et_cumul_horaire(bronze: Path) -> None:
    silver = lire_bronze_static(bronze / "static" / "202402.jsonl")
    assert silver.schema.equals(SCHEMA_SILVER)
    lignes = silver.to_pylist()
    assert all(r["rr1_mm"] is None for r in lignes if r["ts"].minute != 0)
    assert any(r["rr1_mm"] is not None for r in lignes if r["ts"].minute == 0)
    t = [r["t_c"] for r in lignes if r["t_c"] is not None]
    assert -40 < sorted(t)[len(t) // 2] < 40                 # médiane en °C, pas en kelvin
    assert min(t) == pytest.approx(-273.15)                  # le capteur à 0 K reste visible au Silver


# --- B4 : flux et dédoublonnage ------------------------------------------------

def test_b4_un_mois_a_la_fois(bronze: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    lus: list[str] = []
    vrai_mf, vrai_static = pipeline.lire_bronze_mf, pipeline.lire_bronze_static

    def espion_mf(chemin: Path) -> pa.Table:
        lus.append(chemin.stem)
        return vrai_mf(chemin)

    def espion_static(chemin: Path) -> pa.Table:
        lus.append(chemin.stem)
        return vrai_static(chemin)

    monkeypatch.setattr(pipeline, "lire_bronze_mf", espion_mf)
    monkeypatch.setattr(pipeline, "lire_bronze_static", espion_static)
    flux = silver_par_mois(bronze)
    premier = next(flux)
    assert lus and all(s.startswith("202401") for s in lus), f"fichiers lus trop tôt : {lus}"
    assert {premier["ts"][0].as_py().month, premier["ts"][-1].as_py().month} == {1}


def test_b4_sans_doublon_et_trie(bronze: Path) -> None:
    for silver in silver_par_mois(bronze):
        assert silver.schema.equals(SCHEMA_SILVER)
        cles = list(zip(silver["ic_id"].to_pylist(), silver["ts"].to_pylist()))
        assert len(cles) == len(set(cles))
        assert cles == sorted(cles)


def test_b4_rattrapages_presents(bronze: Path) -> None:
    assert list((bronze / "mf").glob("*_r1.csv")), "le jeu de test doit contenir un rattrapage"


# --- B5 / B6 : critères de réussite -------------------------------------------

def test_b6_schema_et_ordre(gold: pa.Table) -> None:
    assert gold.schema.equals(SCHEMA_GOLD)
    cles = list(zip(gold["date"].to_pylist(), gold["ic_id"].to_pylist()))
    assert cles == sorted(cles)


def test_b6_tn_tx_identiques_au_legacy(gold: pa.Table, legacy: dict[Cle, dict[str, object]]) -> None:
    nouveau = _par_cle(gold)
    assert nouveau.keys() == legacy.keys()
    for cle, d in legacy.items():
        for champ in ("tn", "tx"):
            assert nouveau[cle][champ] == pytest.approx(d[champ], abs=1e-9), (cle, champ)


def test_b6_conforme_a_la_specification(gold: pa.Table, bronze: Path) -> None:
    attendu, nouveau = _reference(bronze), _par_cle(gold)
    assert nouveau.keys() == attendu.keys()
    for cle, d in attendu.items():
        for champ, valeur in d.items():
            assert nouveau[cle][champ] == pytest.approx(valeur, abs=1e-6), (cle, champ)


def test_b6_le_legacy_surestime_la_pluie_participative(gold: pa.Table,
                                                        legacy: dict[Cle, dict[str, object]]) -> None:
    nouveau = _par_cle(gold)
    rapports = [legacy[c]["rr"] / n["rr"] for c, n in nouveau.items()  # type: ignore[operator]
                if n["mfid"] is None and n["rr"] and n["rr"] > 2]
    assert rapports and 3 < sorted(rapports)[len(rapports) // 2] < 5


def test_b6_mfid_et_statuts(gold: pa.Table) -> None:
    lignes = gold.to_pylist()
    assert {r["mfid"] is None for r in lignes} == {True, False}
    assert {r["statut_extremes"] for r in lignes} >= {"quotidien", "partiel"}
    assert any(r["n_obs_rejetees"] > 0 for r in lignes)


def test_b6_silver_partitionne_par_annee(lake: Path) -> None:
    fichiers = sorted(p.relative_to(lake).as_posix() for p in (lake / "silver").rglob("*.parquet"))
    assert fichiers == ["silver/observations/annee=2024/202401.parquet",
                        "silver/observations/annee=2024/202402.parquet",
                        "silver/observations/annee=2024/202403.parquet"]
    assert pq.ParquetFile(lake / "gold" / "journaliere.parquet").metadata.num_row_groups == 3


@settings(max_examples=30, deadline=None)
@given(st.lists(st.tuples(st.integers(0, 3), st.integers(0, 200),
                          st.one_of(st.none(), st.floats(-120, 80, allow_nan=False)),
                          st.one_of(st.none(), st.floats(0, 30, allow_nan=False))),
                min_size=1, max_size=80, unique_by=lambda x: (x[0], x[1])))
def test_b5_proprietes(obs: list[tuple[int, int, float | None, float | None]]) -> None:
    """Propriétés : tn <= tm <= tx, statut cohérent avec les comptes, une ligne par (station, jour)."""
    base = datetime(2024, 5, 1, tzinfo=UTC).timestamp()
    silver = pa.table({
        "ic_id": [f"S{s}" for s, *_ in obs],
        "ts": pa.array([int((base + h * 3600) * 1e6) for _, h, *_ in obs], pa.timestamp("us", tz="UTC")),
        "t_c": [t for *_, t, _ in obs], "u_pct": [None] * len(obs),
        "rr1_mm": [r for *_, r in obs], "source": ["test"] * len(obs),
    }, schema=SCHEMA_SILVER)
    ref = pa.table({n: pa.nulls(1, f.type) for n, f in zip(SCHEMA_REF.names, SCHEMA_REF)},
                   schema=SCHEMA_REF)
    gold = gold_journalier(silver, ref)
    cles = list(zip(gold["ic_id"].to_pylist(), gold["date"].to_pylist()))
    assert len(cles) == len(set(cles))
    for r in gold.to_pylist():
        retenues = r["n_obs"] - r["n_obs_rejetees"]
        assert retenues >= 0
        if retenues == 0:
            assert r["tn"] is None and r["statut_extremes"] is None
        else:
            assert r["tn"] <= r["tm"] <= r["tx"]
            assert BORNES_T[0] <= r["tn"] and r["tx"] <= BORNES_T[1]
