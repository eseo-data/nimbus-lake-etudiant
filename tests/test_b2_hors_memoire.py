"""Bloc 2, partie C — plus gros que la mémoire (taille réduite pour les tests)."""

from pathlib import Path

import duckdb
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from nimbus_lake.b2_hors_memoire import ecrire_journalier, mesurer, plus_petite_memoire, requete_journaliere
from nimbus_lake.b2_duckdb import connecter, lire_parquet


def test_c1_journalier(silver_massif_petit: Path, tmp_path: Path) -> None:
    n = ecrire_journalier(connecter(), silver_massif_petit, tmp_path / "j.parquet")
    table = pq.read_table(tmp_path / "j.parquet")
    assert n == table.num_rows == 30 * 731
    assert table.column_names == ["ic_id", "date", "tn", "tx", "rr", "n_obs"]
    silver = ds.dataset(silver_massif_petit, format="parquet", partitioning="hive").to_table()
    assert pc.sum(table["n_obs"]).as_py() == silver["t_c"].length() - silver["t_c"].null_count
    assert pc.min(table["tn"]).as_py() == pc.min(silver["t_c"]).as_py()
    assert pq.ParquetFile(tmp_path / "j.parquet").metadata.row_group(0).column(0).compression == "ZSTD"


def test_c1_pas_d_order_by(silver_massif_petit: Path) -> None:
    assert "ORDER BY" not in requete_journaliere(silver_massif_petit).upper()


def test_c2_debordement_sous_contrainte(silver_massif_petit: Path, tmp_path: Path) -> None:
    source = lire_parquet(silver_massif_petit / "**" / "*.parquet")
    sql = f"SELECT ic_id, ts, min(t_c) AS a, max(t_c) AS b, sum(rr1_mm) AS c, count(*) AS n FROM {source} GROUP BY ALL"
    large = mesurer(sql, "2GB", 1, tmp_path)
    assert large.pic_temp_octets == 0 and large.lignes_resultat == 30 * 731 * 8
    debordes = []
    for limite in ("16MB", "20MB", "24MB", "32MB"):       # le seuil exact dépend de la machine
        try:
            p = mesurer(sql, limite, 1, tmp_path)
        except duckdb.OutOfMemoryException:
            continue
        assert p.lignes_resultat == large.lignes_resultat
        debordes.append(p.pic_temp_octets)
    assert any(t > 0 for t in debordes), "aucune limite n'a provoqué de débordement sur disque"


def test_c3_plus_petite_memoire(silver_massif_petit: Path, tmp_path: Path) -> None:
    sql = f"SELECT * FROM {lire_parquet(silver_massif_petit / '**' / '*.parquet')} ORDER BY t_c DESC, ts"
    candidats = ["1MB", "4MB", "16MB", "256MB", "2GB"]
    choix = plus_petite_memoire(sql, candidats, 1, tmp_path)
    assert choix in candidats and choix != "2GB"
    i = candidats.index(choix)
    if i > 0:
        con = connecter(candidats[i - 1], 1, tmp_path)
        con.execute("SET preserve_insertion_order = false")
        try:
            con.execute(sql).fetchall()
            raise AssertionError("la limite précédente suffisait déjà")
        except duckdb.OutOfMemoryException:
            pass
    assert plus_petite_memoire(sql, ["1KB"], 1, tmp_path) is None
