"""Bloc 1, défi ★★★ — footer Parquet décodé à la main.

Les tests vérifient le résultat (comparé à pyarrow) ET la façon de l'obtenir :
pas de bibliothèque Parquet importée, lecture limitée à la fin du fichier.
"""

import ast
import io
from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

import nimbus_lake.b1_defi_footer as defi

INTERDITS = {"pyarrow", "duckdb", "polars", "fastparquet", "pandas", "thrift", "parquet"}


def _ou_skip[T](f: Callable[[], T]) -> T:
    try:
        return f()
    except NotImplementedError:
        pytest.skip("défi non traité")


class Compteur(io.RawIOBase):
    """Fichier binaire qui compte les octets réellement lus."""

    def __init__(self, contenu: bytes) -> None:
        self.f = io.BytesIO(contenu)
        self.lus = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def seek(self, pos: int, whence: int = 0) -> int:
        return self.f.seek(pos, whence)

    def tell(self) -> int:
        return self.f.tell()

    def read(self, n: int = -1) -> bytes:
        data = self.f.read(n)
        self.lus += len(data)
        return data

    def readinto(self, b: bytearray) -> int:  # type: ignore[override]
        data = self.read(len(b))
        b[:len(data)] = data
        return len(data)


def _parquet(table: pa.Table, taille_rg: int) -> bytes:
    tampon = io.BytesIO()
    pq.write_table(table, tampon, row_group_size=taille_rg, compression="zstd")
    return tampon.getvalue()


def test_d0_aucune_bibliotheque_parquet() -> None:
    arbre = ast.parse(Path(defi.__file__).read_text(encoding="utf-8"))
    for noeud in ast.walk(arbre):
        if isinstance(noeud, ast.Import):
            racines = {a.name.split(".")[0] for a in noeud.names}
        elif isinstance(noeud, ast.ImportFrom):
            racines = {(noeud.module or "").split(".")[0]}
        else:
            continue
        assert not racines & INTERDITS, f"import interdit : {racines & INTERDITS}"


def test_d1_signature_et_lecture_minimale() -> None:
    contenu = _parquet(pa.table({"x": pa.array(range(200_000), pa.int64())}), 20_000)
    f = Compteur(contenu)
    footer = _ou_skip(lambda: defi.lire_octets_footer(f))  # type: ignore[arg-type]
    taille = int.from_bytes(contenu[-8:-4], "little")
    assert footer == contenu[-8 - taille:-8]
    assert f.lus <= taille + 8 + 64, f"{f.lus} octets lus pour un fichier de {len(contenu)}"
    with pytest.raises(ValueError):
        defi.lire_octets_footer(io.BytesIO(b"PAR1 pas du tout un parquet"))


@settings(max_examples=30, deadline=None)
@given(valeurs=st.lists(st.one_of(st.none(), st.integers(-2**63, 2**63 - 1)), min_size=1, max_size=300),
       taille=st.integers(1, 64))
def test_d3_footer_conforme_a_pyarrow(valeurs: list[int | None], taille: int) -> None:
    table = pa.table({"x": pa.array(valeurs, pa.int64()),
                      "nom": pa.array([f"s{i % 7}" for i in range(len(valeurs))])})
    contenu = _parquet(table, taille)
    meta = _ou_skip(lambda: defi.lire_footer(io.BytesIO(contenu)))
    attendu = pq.ParquetFile(io.BytesIO(contenu)).metadata
    assert meta.num_rows == attendu.num_rows
    assert meta.colonnes == ("x", "nom")
    assert len(meta.row_groups) == attendu.num_row_groups
    for i, rg in enumerate(meta.row_groups):
        ref = attendu.row_group(i)
        assert rg.num_rows == ref.num_rows
        stats = ref.column(0).statistics
        assert rg.colonnes["x"].null_count == stats.null_count
        if stats.has_min_max:
            assert int.from_bytes(rg.colonnes["x"].min_value or b"", "little", signed=True) == stats.min
            assert int.from_bytes(rg.colonnes["x"].max_value or b"", "little", signed=True) == stats.max


def test_d4_row_groups_a_lire_sur_des_timestamps(tmp_path: Path) -> None:
    ts = pa.array(range(0, 1_000_000_000, 1_000_000), pa.timestamp("us", tz="UTC"))
    contenu = _parquet(pa.table({"ts": ts}), 100)
    meta = _ou_skip(lambda: defi.lire_footer(io.BytesIO(contenu)))
    assert _ou_skip(lambda: defi.row_groups_a_lire(meta, "ts", 250_000_000, 260_000_000)) == [2]
    assert defi.row_groups_a_lire(meta, "ts", -10, -1) == []
    assert defi.row_groups_a_lire(meta, "ts", 0, 10**12) == list(range(10))


def test_d2_decodeur_sur_un_vrai_fichier_de_cours(bronze: Path, tmp_path: Path) -> None:
    """Le décodeur générique doit traverser tout le footer d'un fichier réel (champs inconnus compris)."""
    table = pacsv.read_csv(bronze / "mf" / "202402.csv", parse_options=pacsv.ParseOptions(delimiter=";"))
    pq.write_table(table, tmp_path / "mf.parquet", row_group_size=500)
    contenu = (tmp_path / "mf.parquet").read_bytes()
    lecteur = defi.LecteurCompact(_ou_skip(lambda: defi.lire_octets_footer(io.BytesIO(contenu))))
    champs = _ou_skip(lecteur.lire_struct)
    assert lecteur.pos == len(lecteur.octets)          # tout le footer est consommé
    assert champs[3] == table.num_rows
