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
