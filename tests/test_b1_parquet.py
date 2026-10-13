"""Bloc 1, partie A — Parquet de l'intérieur."""

from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from nimbus_lake.b1_parquet import (
    SCHEMA_MF_BRUT,
    bornes_row_groups,
    ecrire_parquet,
    effet_du_tri,
    inspecter,
    lire_bronze_mf,
    row_groups_candidats,
    tailles_par_format,
)


def _mf(bronze: Path) -> pa.Table:
    return pa.concat_tables(lire_bronze_mf(c) for c in sorted((bronze / "mf").glob("*.csv")))


# --- A1 ---------------------------------------------------------------------

def test_a1_schema_explicite(bronze: Path) -> None:
    table = lire_bronze_mf(bronze / "mf" / "202401.csv")
    assert table.schema.equals(SCHEMA_MF_BRUT)
    assert table.num_rows > 0


def test_a1_num_poste_garde_son_zero_initial(bronze: Path) -> None:
    postes = set(lire_bronze_mf(bronze / "mf" / "202401.csv")["NUM_POSTE"].to_pylist())
    assert all(len(p) == 8 for p in postes)
    assert any(p.startswith("0") for p in postes), "le département 01 doit rester « 01… »"


def test_a1_cellule_vide_devient_null(bronze: Path) -> None:
    table = _mf(bronze)
    assert table["T"].null_count > 0          # températures manquantes
    assert table["RR1"].null_count > 0        # stations secondaires sans pluviomètre


# --- A2 / A3 ----------------------------------------------------------------

def test_a2_a3_ecrire_puis_inspecter(bronze: Path, tmp_path: Path) -> None:
    table = _mf(bronze)
    taille = ecrire_parquet(table, tmp_path / "mf.parquet", "zstd", 5_000)
    assert taille == (tmp_path / "mf.parquet").stat().st_size
    infos = inspecter(tmp_path / "mf.parquet")
    assert infos.n_lignes == table.num_rows
    assert infos.n_row_groups == -(-table.num_rows // 5_000)
    assert sum(infos.lignes_par_row_group) == table.num_rows
    assert infos.colonnes == tuple(SCHEMA_MF_BRUT.names)
    assert infos.codecs == frozenset({"ZSTD"})
    assert 0 < infos.taille_footer < taille


def test_a2_sans_compression(bronze: Path, tmp_path: Path) -> None:
    ecrire_parquet(_mf(bronze), tmp_path / "brut.parquet", None, 10_000)
    assert inspecter(tmp_path / "brut.parquet").codecs == frozenset({"UNCOMPRESSED"})


# --- A4 / A5 ----------------------------------------------------------------

@settings(max_examples=25, deadline=None)
@given(valeurs=st.lists(st.integers(-10**12, 10**12), min_size=1, max_size=400),
       taille=st.integers(1, 50))
def test_a4_bornes_conformes_aux_donnees(tmp_path_factory: pytest.TempPathFactory,
                                          valeurs: list[int], taille: int) -> None:
    chemin = tmp_path_factory.mktemp("a4") / "x.parquet"
    pq.write_table(pa.table({"x": pa.array(valeurs, pa.int64())}), chemin, row_group_size=taille)
    attendu = [(min(valeurs[i:i + taille]), max(valeurs[i:i + taille]))
               for i in range(0, len(valeurs), taille)]
    assert bornes_row_groups(chemin, "x") == attendu


@settings(max_examples=25, deadline=None)
@given(valeurs=st.lists(st.integers(0, 1000), min_size=1, max_size=300),
       taille=st.integers(1, 40), cible=st.integers(0, 1000))
def test_a5_aucun_faux_negatif(tmp_path_factory: pytest.TempPathFactory,
                               valeurs: list[int], taille: int, cible: int) -> None:
    """Un row group qui contient la valeur n'est JAMAIS écarté."""
    chemin = tmp_path_factory.mktemp("a5") / "x.parquet"
    pq.write_table(pa.table({"x": pa.array(valeurs, pa.int64())}), chemin, row_group_size=taille)
    candidats = set(row_groups_candidats(chemin, "x", cible))
    for i in range(0, len(valeurs), taille):
        if cible in valeurs[i:i + taille]:
            assert i // taille in candidats


def test_a5_ecarte_les_row_groups_hors_bornes(tmp_path: Path) -> None:
    chemin = tmp_path / "x.parquet"
    pq.write_table(pa.table({"x": pa.array(range(1000), pa.int64())}), chemin, row_group_size=100)
    assert row_groups_candidats(chemin, "x", 250) == [2]
    assert row_groups_candidats(chemin, "x", 5000) == []


# --- A6 / A7 ----------------------------------------------------------------

def test_a6_parquet_plus_compact_que_csv(bronze: Path, tmp_path: Path) -> None:
    tailles = tailles_par_format(_mf(bronze), tmp_path)
    assert set(tailles) == {"csv", "parquet_none", "parquet_snappy", "parquet_zstd"}
    assert tailles["parquet_zstd"] < tailles["parquet_snappy"] < tailles["parquet_none"] < tailles["csv"]


def test_a7_le_tri_rend_le_pruning_efficace(tmp_path: Path) -> None:
    rng = np.random.default_rng(0)
    table = pa.table({"station": pa.array(rng.integers(0, 500, 200_000), pa.int64()),
                      "t": rng.normal(12, 6, 200_000)})
    sans_tri, avec_tri = effet_du_tri(table, "station", 123, tmp_path, 10_000)
    assert sans_tri == 20          # valeurs mélangées : chaque row group couvre [0, 499]
    assert avec_tri <= 2           # données triées : un seul row group (deux à la frontière)
