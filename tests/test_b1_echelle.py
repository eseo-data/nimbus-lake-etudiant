"""Bloc 1, partie C — partitioning à l'échelle (taille réduite pour les tests)."""

from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from nimbus_lake.b1_echelle import (
    CLES,
    SCHEMA_PARTITIONNABLE,
    ajouter_colonnes_partition,
    compacter,
    compter_fichiers,
    ecrire_partitionne,
    fichiers_lus,
    gold_mensuel_duckdb,
    moyenne_station_annee,
    ouvrir,
)
from nimbus_lake.donnees import silver_massif

N_STATIONS, N_ANNEES = 12, 3


def _source() -> list[pa.Table]:
    return list(silver_massif(graine=1, n_stations=N_STATIONS, n_annees=N_ANNEES))


@pytest.fixture(scope="module")
def datasets(tmp_path_factory: pytest.TempPathFactory) -> dict[str, tuple[Path, int]]:
    racine = tmp_path_factory.mktemp("echelle")
    return {cle: (racine / cle, ecrire_partitionne(iter(_source()), racine / cle, cle)) for cle in CLES}


def test_c1_colonnes_de_partition() -> None:
    t = ajouter_colonnes_partition(next(silver_massif(graine=1, n_stations=1, n_annees=2)))
    assert t.schema.equals(SCHEMA_PARTITIONNABLE)
    assert set(t["annee"].to_pylist()) == {2016, 2017}
    assert set(t["mois"].to_pylist()) == set(range(1, 13))


def test_c2_nombre_de_fichiers(datasets: dict[str, tuple[Path, int]]) -> None:
    n = {cle: nb for cle, (_, nb) in datasets.items()}
    assert n["aucune"] == 1
    assert n["annee"] == N_ANNEES
    assert n["annee_mois"] == 12 * N_ANNEES
    assert n["ic_id"] == N_STATIONS
    assert n["ic_id_annee"] == N_STATIONS * N_ANNEES
    for cle, (racine, nb) in datasets.items():
        assert compter_fichiers(racine) == nb, cle


def test_c2_dossiers_hive(datasets: dict[str, tuple[Path, int]]) -> None:
    racine, _ = datasets["annee_mois"]
    assert (racine / "annee=2017" / "mois=3").is_dir()
    racine, _ = datasets["ic_id_annee"]
    assert (racine / "ic_id=ST0004" / "annee=2018").is_dir()


def test_c2_aucune_ligne_perdue(datasets: dict[str, tuple[Path, int]]) -> None:
    attendu = sum(t.num_rows for t in _source())
    for cle, (racine, _) in datasets.items():
        assert ouvrir(racine).count_rows() == attendu, cle


def test_c3_partition_pruning(datasets: dict[str, tuple[Path, int]]) -> None:
    une_annee = pc.field("annee") == 2017
    assert fichiers_lus(datasets["aucune"][0], une_annee) == 1          # pas de pruning possible
    assert fichiers_lus(datasets["annee"][0], une_annee) == 1
    assert fichiers_lus(datasets["annee_mois"][0], une_annee) == 12
    assert fichiers_lus(datasets["ic_id"][0], une_annee) == N_STATIONS  # la clé ne sert à rien ici
    assert fichiers_lus(datasets["ic_id_annee"][0], une_annee) == N_STATIONS


def test_c4_meme_reponse_quelle_que_soit_la_cle(datasets: dict[str, tuple[Path, int]]) -> None:
    t = next(iter(t for t in _source() if t["ic_id"][0].as_py() == "ST0005"))
    annee = t.filter(pc.equal(pc.year(t["ts"]), 2018))["t_c"]
    attendu = (len(annee), pc.mean(annee).as_py())
    for cle, (racine, _) in datasets.items():
        n, moyenne = moyenne_station_annee(racine, "ST0005", 2018)
        assert n == attendu[0], cle
        assert moyenne == pytest.approx(attendu[1]), cle


def test_c5_compacter_les_petits_fichiers(datasets: dict[str, tuple[Path, int]], tmp_path: Path) -> None:
    source, avant = datasets["ic_id_annee"]
    apres = compacter(source, tmp_path / "compact", "annee", 30_000)
    lignes = ouvrir(source).count_rows()
    assert apres < avant
    assert apres <= N_ANNEES * (-(-lignes // N_ANNEES // 30_000) + 1)
    compact = ouvrir(tmp_path / "compact")
    assert compact.count_rows() == lignes
    assert pc.sum(compact.to_table(columns=["t_c"])["t_c"]).as_py() == pytest.approx(
        pc.sum(ouvrir(source).to_table(columns=["t_c"])["t_c"]).as_py())


def test_bonus_duckdb(datasets: dict[str, tuple[Path, int]]) -> None:
    racine, _ = datasets["annee_mois"]
    try:
        resultat = gold_mensuel_duckdb(racine)
    except NotImplementedError:
        pytest.skip("bonus non traité")
    assert resultat.column_names == ["ic_id", "annee", "mois", "t_moy", "n"]
    assert resultat.num_rows == N_STATIONS * N_ANNEES * 12
    table = ouvrir(racine).to_table(columns=["ic_id", "annee", "mois", "t_c"])
    attendu = table.group_by(["ic_id", "annee", "mois"]).aggregate([("t_c", "mean")]).sort_by(
        [("ic_id", "ascending"), ("annee", "ascending"), ("mois", "ascending")])
    assert resultat["ic_id"].to_pylist() == attendu["ic_id"].to_pylist()
    assert resultat["t_moy"].to_pylist() == pytest.approx(attendu["t_c_mean"].to_pylist())
