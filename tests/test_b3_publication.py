"""Bloc 3, partie B — publication atomique et idempotente."""

from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from nimbus_lake import b3_legacy
from nimbus_lake.b3_iceberg import SCHEMA_ARROW, catalogue, creer_journaliere, etat
from nimbus_lake.b3_publication import annuler, dedoublonner, publier, publier_avec_audit, publier_tout
from nimbus_lake.donnees import livraisons

CLE = [("ic_id", "ascending"), ("date", "ascending")]


class Panne(Exception):
    pass


def _panne() -> None:
    raise Panne("coupure")


def _reference(lots: list[pa.Table]) -> pa.Table:
    lignes: dict[tuple[object, object], dict[str, object]] = {}
    for lot in lots:
        for r in lot.to_pylist():
            cle = (r["ic_id"], r["date"])
            if cle not in lignes or r["recu_a"] >= lignes[cle]["recu_a"]:
                lignes[cle] = r
    return pa.Table.from_pylist([{k: v for k, v in r.items() if k != "recu_a"} for _, r in sorted(lignes.items())],
                                schema=SCHEMA_ARROW)


def _contenu(table: object) -> pa.Table:
    return table.scan().to_arrow().sort_by(CLE)  # type: ignore[attr-defined]


def test_b1_dedoublonner() -> None:
    lots = livraisons(graine=1, n_stations=10, n_jours=15)
    reduits = 0
    for lot in lots:
        propre = dedoublonner(lot)
        assert propre.schema.equals(SCHEMA_ARROW)
        assert propre.equals(_reference([lot]))
        reduits += propre.num_rows < lot.num_rows
    assert reduits > 0                                       # certaines livraisons contiennent des doublons


def test_b2_trente_livraisons_conformes(entrepot: Path) -> None:
    lots = livraisons(graine=2, n_stations=8, n_jours=30)
    table = creer_journaliere(catalogue(entrepot))
    publications = publier_tout(table, lots)
    assert _contenu(table).equals(_reference(lots))
    assert sum(p.lignes_mises_a_jour for p in publications) > 0     # des corrections ont été appliquées
    assert etat(table).n_snapshots >= 30


def test_b2_idempotence(entrepot: Path) -> None:
    lots = livraisons(graine=3, n_stations=5, n_jours=10)
    table = creer_journaliere(catalogue(entrepot))
    publier_tout(table, lots)
    avant = _contenu(table)
    p = publier(table, lots[-1])
    assert p.lignes_inserees == 0 and p.lignes_mises_a_jour == 0
    assert _contenu(table).equals(avant)


def test_b2_atomicite_sous_panne(entrepot: Path) -> None:
    lots = livraisons(graine=4, n_stations=6, n_jours=12)
    table = creer_journaliere(catalogue(entrepot))
    publier_tout(table, lots[:-1])
    avant, snap = _contenu(table), table.current_snapshot()
    with pytest.raises(Panne):
        publier(table, lots[-1], panne=_panne)
    table.refresh()
    assert table.current_snapshot() == snap
    assert _contenu(table).equals(avant)


def test_b2_le_legacy_perd_des_mois_sous_panne(tmp_path: Path) -> None:
    lots = livraisons(graine=4, n_stations=6, n_jours=45)          # juin et juillet
    racine = str(tmp_path / "gold")
    for lot in lots[:-1]:
        b3_legacy.publier(racine, lot)
    avant = b3_legacy.lire(racine).num_rows
    with pytest.raises(Panne):
        b3_legacy.publier(racine, lots[-1], panne=_panne)
    apres = b3_legacy.lire(racine)
    assert apres is None or apres.num_rows < avant


def test_b2_lecteur_isole_pendant_la_publication(entrepot: Path) -> None:
    lots = livraisons(graine=5, n_stations=5, n_jours=8)
    table = creer_journaliere(catalogue(entrepot))
    publier_tout(table, lots[:-1])
    lecture = table.scan()                                   # un lecteur fige sa vue
    vue = lecture.to_arrow().num_rows
    publier(table, lots[-1])
    assert lecture.to_arrow().num_rows == vue
    assert table.scan().to_arrow().num_rows > vue


def test_b3_annuler_une_livraison_erronee(entrepot: Path) -> None:
    lots = livraisons(graine=6, n_stations=5, n_jours=10)
    table = creer_journaliere(catalogue(entrepot))
    publier_tout(table, lots)
    bon, snap = _contenu(table), table.current_snapshot()
    assert snap is not None
    erronee = lots[-1].set_column(3, "tx", pc.add(lots[-1]["tx"], 100.0))
    erronee = erronee.set_column(5, "recu_a", pc.add(erronee["recu_a"], pa.scalar(3_600_000_000, pa.duration("us"))))
    publier(table, erronee)
    assert pc.max(_contenu(table)["tx"]).as_py() > 100
    annuler(table, snap.snapshot_id)
    assert _contenu(table).equals(bon)
    assert len(table.snapshots()) >= 11                        # l'historique est conservé


def test_bonus_write_audit_publish(entrepot: Path, publier_reference) -> None:  # type: ignore[no-untyped-def]
    lots = livraisons(graine=7, n_stations=4, n_jours=6)
    table = creer_journaliere(catalogue(entrepot))
    publier_reference(table, lots[:-1])
    avant = _contenu(table)

    def plausible(t: pa.Table) -> bool:
        return bool(pc.max(t["tx"]).as_py() < 60)

    erronee = lots[-1].set_column(3, "tx", pc.add(lots[-1]["tx"], 100.0))
    try:
        refuse = publier_avec_audit(table, erronee, plausible)
    except NotImplementedError:
        pytest.skip("bonus non traité")
    assert refuse is False and _contenu(table).equals(avant) and list(table.refs()) == ["main"]
    assert publier_avec_audit(table, lots[-1], plausible) is True
    assert _contenu(table).equals(_reference(lots)) and list(table.refs()) == ["main"]
