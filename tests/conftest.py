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
