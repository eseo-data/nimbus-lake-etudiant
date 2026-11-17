from datetime import date
from pathlib import Path

import pytest

from nimbus_lake.donnees import ecrire_bronze


@pytest.fixture(scope="session")
def bronze(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Petit Bronze : 9 stations, 70 jours à cheval sur 3 mois (rattrapages compris)."""
    racine = tmp_path_factory.mktemp("bronze")
    ecrire_bronze(racine, graine=7, debut=date(2024, 1, 20), n_jours=70)
    return racine


@pytest.fixture(scope="session")
def journalier_parquet(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Bloc 2 : climatologie journalière synthétique, 12 stations × 3 ans, jours manquants compris."""
    import pyarrow.parquet as pq

    from nimbus_lake.donnees import journalier_synthetique

    chemin = tmp_path_factory.mktemp("journalier") / "journalier.parquet"
    pq.write_table(journalier_synthetique(graine=3, n_stations=12, n_annees=3, taux_trous=0.03), chemin)
    return chemin


@pytest.fixture(scope="session")
def silver_massif_petit(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Bloc 2 : Silver massif réduit (30 stations × 2 ans, 175 000 lignes), partitionné par année."""
    from nimbus_lake.donnees import ecrire_silver_massif

    racine = tmp_path_factory.mktemp("silver_massif")
    ecrire_silver_massif(racine, n_stations=30, n_annees=2, graine=5)
    return racine


@pytest.fixture()
def entrepot(tmp_path: Path) -> Path:
    """Bloc 3 : un dossier d'entrepôt Iceberg neuf par test."""
    return tmp_path / "entrepot"


@pytest.fixture()
def publier_reference():  # type: ignore[no-untyped-def]
    """Bloc 3 : publie des livraisons avec pyiceberg seul (indépendant de votre partie B)."""
    import duckdb

    from nimbus_lake.b3_iceberg import SCHEMA_ARROW

    def publier(table, lots):  # type: ignore[no-untyped-def]
        for lot in lots:
            con = duckdb.connect()
            con.register("l", lot)
            propre = con.sql("SELECT ic_id, date, tn, tx, rr FROM l QUALIFY row_number() OVER "
                             "(PARTITION BY ic_id, date ORDER BY recu_a DESC) = 1").to_arrow_table()
            table.upsert(propre.cast(SCHEMA_ARROW), join_cols=["ic_id", "date"])
        table.refresh()

    return publier
