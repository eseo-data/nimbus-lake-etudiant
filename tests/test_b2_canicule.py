"""Bloc 2, partie B — indicateurs de chaleur en SQL."""

from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from nimbus_lake import b2_legacy
from nimbus_lake.b2_canicule import SCHEMA_CHALEUR, indicateurs_chaleur, plus_longs_episodes, requete_chaleur
from nimbus_lake.b2_duckdb import connecter, lire_parquet, profiler


def _reference(lignes: list[dict[str, object]]) -> list[dict[str, object]]:
    par_cle: dict[tuple[str, int], dict[date, dict[str, object]]] = defaultdict(dict)
    for r in lignes:
        par_cle[(r["ic_id"], r["date"].year)][r["date"]] = r  # type: ignore[union-attr,index]
    sortie = []
    for (ic_id, annee), jours in sorted(par_cle.items()):
        chauds = {d for d, r in jours.items() if r["tx"] is not None and r["tx"] >= 30}  # type: ignore[operator]
        episodes = []
        for d in sorted(chauds):
            if d - timedelta(days=1) in chauds:
                continue
            n = 1
            while d + timedelta(days=n) in chauds:
                n += 1
            if n >= 3:
                episodes.append((n, d))
        plus_long = max(episodes, key=lambda e: (e[0], -e[1].toordinal()), default=(0, None))
        sortie.append({
            "ic_id": ic_id, "annee": annee, "n_jours_tx30": len(chauds),
            "n_nuits_tropicales": sum(1 for r in jours.values() if r["tn"] is not None and r["tn"] >= 20),  # type: ignore[operator]
            "n_episodes": len(episodes), "plus_long_episode": plus_long[0], "debut_plus_long": plus_long[1],
        })
    return sortie


def test_b1_conforme_a_la_specification(journalier_parquet: Path) -> None:
    resultat = indicateurs_chaleur(connecter(), lire_parquet(journalier_parquet))
    assert resultat.schema.equals(SCHEMA_CHALEUR)
    attendu = _reference(pq.read_table(journalier_parquet).to_pylist())
    assert resultat.to_pylist() == attendu
    assert sum(r["n_episodes"] for r in attendu) > 0          # le jeu de test contient des épisodes


def test_b2_identique_au_legacy_sans_jour_manquant(journalier_parquet: Path) -> None:
    nouveau = {(r["ic_id"], r["annee"]): r for r in
               indicateurs_chaleur(connecter(), lire_parquet(journalier_parquet)).to_pylist()}
    ancien = {(r["ic_id"], r["annee"]): r for r in b2_legacy.executer(journalier_parquet)}
    assert nouveau.keys() == ancien.keys()
    impairs = [c for c in ancien if int(c[0][2:]) % 2 == 1]   # stations impaires : aucun jour manquant
    assert impairs and all(nouveau[c] == ancien[c] for c in impairs)


def test_b2_le_legacy_fusionne_les_episodes_autour_d_un_trou() -> None:
    debut = date(2024, 7, 1)
    jours = [debut + timedelta(days=i) for i in range(8) if i != 4]          # le 5 juillet manque
    chemin_table = pa.table({"ic_id": ["S1"] * 7, "date": jours, "tn": [18.0] * 7, "tx": [33.0] * 7, "rr": [0.0] * 7})
    con = connecter()
    con.register("j", chemin_table)
    r = indicateurs_chaleur(con, "j").to_pylist()[0]
    assert (r["n_episodes"], r["plus_long_episode"], r["debut_plus_long"]) == (2, 4, debut)


def test_b3_une_requete_avec_window(journalier_parquet: Path) -> None:
    sql = requete_chaleur(lire_parquet(journalier_parquet))
    assert sql.strip().upper().startswith(("WITH", "SELECT"))
    assert ";" not in sql.strip().rstrip(";")
    assert "WINDOW" in profiler(connecter(), sql).operateurs


@settings(max_examples=40, deadline=None)
@given(st.lists(st.tuples(st.integers(0, 40), st.one_of(st.none(), st.sampled_from([25.0, 30.0, 31.5])),
                          st.sampled_from([15.0, 20.0, 22.0])),
                min_size=1, max_size=40, unique_by=lambda x: x[0]))
def test_b1_proprietes(jours: list[tuple[int, float | None, float]]) -> None:
    debut = date(2023, 12, 20)                       # à cheval sur deux années
    lignes = [{"ic_id": "S", "date": debut + timedelta(days=d), "tn": tn, "tx": tx, "rr": 0.0} for d, tx, tn in jours]
    con = connecter()
    con.register("j", pa.table({k: [r[k] for r in lignes] for k in ("ic_id", "date", "tn", "tx", "rr")},
                               schema=pa.schema([("ic_id", pa.string()), ("date", pa.date32()), ("tn", pa.float64()),
                                                 ("tx", pa.float64()), ("rr", pa.float64())])))
    assert indicateurs_chaleur(con, "j").to_pylist() == _reference(lignes)


def test_bonus_plus_longs_episodes(journalier_parquet: Path) -> None:
    try:
        top = plus_longs_episodes(connecter(), lire_parquet(journalier_parquet), 5)
    except NotImplementedError:
        pytest.skip("bonus non traité")
    lignes = top.to_pylist()
    assert top.column_names == ["ic_id", "debut", "fin", "duree", "tx_max"]
    assert len(lignes) == 5
    assert [r["duree"] for r in lignes] == sorted((r["duree"] for r in lignes), reverse=True)
    meilleur = max((r["plus_long_episode"] for r in
                    _reference(pq.read_table(journalier_parquet).to_pylist())))
    assert lignes[0]["duree"] == meilleur
    assert all((r["fin"] - r["debut"]).days + 1 == r["duree"] and r["tx_max"] >= 30 for r in lignes)
