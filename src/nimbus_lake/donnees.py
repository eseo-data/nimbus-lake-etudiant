"""Générateur de données Nimbus-Lake (fourni, rien à compléter).

Le générateur reproduit, en petit et sur des données synthétiques, la forme des
données d'un vrai lakehouse climatologique (portail.chom.engineering) :

- un référentiel de stations avec trois identifiants (ic_id canonique, mfid =
  NUM_POSTE Météo-France, libellés bruts « TEST MF CSV… » qui désignent de vraies
  stations, sentinelle « 0000-00-00 00:00:00 » pour une date inconnue) ;
- des observations Météo-France au format d'origine (CSV « ; », AAAAMMJJHH, °C),
  stations principales horaires et secondaires à 3 relevés par jour ;
- des observations du réseau participatif au format canonique SI (JSON Lines,
  kelvin, `precip_1h` cumul glissant publié toutes les 15 minutes).

Défauts volontaires : valeurs manquantes, valeurs physiquement impossibles,
fichiers de rattrapage qui relivrent des lignes déjà livrées (doublons exacts),
deux écritures d'un même instant (`Z` et `+00:00`).

Tout est déterministe : même graine, mêmes fichiers. Aucun accès réseau.

Usage :
    uv run python -m nimbus_lake.donnees data/bronze            # 2 ans, 9 stations
"""

from __future__ import annotations

import json
import math
import random
import sys
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import numpy as np
import pyarrow as pa

SENTINELLE = "0000-00-00 00:00:00"
BORNES_T = (-90.0, 60.0)          # bornes de plausibilité physique (°C)
EN_TETE_MF = "NUM_POSTE;AAAAMMJJHH;T;U;PMER;RR1"
EN_TETE_REF = "ic_id,mfid,genre,libelle_source,latitude,longitude,altitude,departement,dh_ouverture"
HEURES_SECONDAIRES = (6, 12, 18)

_VILLES = ("Ambérieu", "Brest", "Lyon-Bron", "Mende", "Trappes", "Pontarlier", "Rennes",
           "Toulouse", "Nancy", "Bastia", "Gap", "Lille", "Caen", "Tours", "Pau", "Nice")


@dataclass(frozen=True, slots=True)
class Station:
    ic_id: str
    mfid: str | None
    genre: str                    # "mf_principale" | "mf_secondaire" | "static"
    libelle_source: str
    latitude: float
    longitude: float
    altitude: int
    departement: str
    dh_ouverture: str             # "AAAA-MM-JJ HH:MM:SS" ou SENTINELLE
    t_moy: float
    amplitude: float
    biais_hum: float


def referentiel(graine: int = 42, n_principales: int = 4, n_secondaires: int = 2,
                n_static: int = 3) -> tuple[Station, ...]:
    """Fonction pure : le référentiel des stations."""
    rng = random.Random(f"ref-{graine}")
    stations: list[Station] = []
    omm_pris: set[str] = set()
    for k in range(n_principales + n_secondaires + n_static):
        ville = _VILLES[k % len(_VILLES)]
        dep = 1 if k == 0 else rng.randint(1, 95)          # le département 01 force un zéro initial
        climat = (rng.uniform(7.0, 15.0), rng.uniform(4.0, 10.0), rng.uniform(-8.0, 10.0))
        ouverture = SENTINELLE if rng.random() < 0.2 else \
            f"{rng.randint(1950, 2010)}-{rng.randint(1, 12):02d}-01 00:00:00"
        coord = (round(rng.uniform(42.0, 51.0), 4), round(rng.uniform(-4.5, 8.0), 4), rng.randint(2, 1400))
        if k < n_principales + n_secondaires:
            mfid = f"{dep:02d}{rng.randint(1, 999):03d}{rng.randint(1, 9):03d}"
            if k < n_principales:
                genre = "mf_principale"
                omm = f"07{rng.randint(0, 999):03d}"
                while omm in omm_pris:
                    omm = f"07{rng.randint(0, 999):03d}"
                omm_pris.add(omm)
                ic_id = omm
            else:
                genre, ic_id = "mf_secondaire", mfid
            source = f"TEST MF CSV{rng.randint(1, 40)} {ville.upper()}" if k % 2 else ville.upper()
        else:
            genre, mfid = "static", None
            n = k - n_principales - n_secondaires
            ic_id = (f"STATIC{253 + n:04d}", f"{6 + n:05d}", f"MNWERO{90 + n:03d}")[n % 3]
            source = ville
        stations.append(Station(ic_id, mfid, genre, source, *coord, f"{dep:02d}", ouverture, *climat))
    return tuple(stations)


# ---------------------------------------------------------------------------
# Vérité terrain : série continue au pas de 15 minutes
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class Quart:
    """Un pas de 15 minutes. `pluie` = pluie tombée dans le quart d'heure qui finit à `ts`."""
    ts: datetime
    t_c: float
    u_pct: float
    pmer: float
    pluie: float


def serie_verite(station: Station, debut: date, n_jours: int, graine: int = 42) -> Iterator[Quart]:
    """Générateur pur : la série « vraie » d'une station, sans défaut."""
    rng = random.Random(f"obs-{graine}-{station.ic_id}")
    anomalie, pression = 0.0, 1013.0
    t0 = datetime(debut.year, debut.month, debut.day, tzinfo=UTC)
    for j in range(n_jours):
        jour = debut + timedelta(days=j)
        saison = math.cos(2 * math.pi * (jour.timetuple().tm_yday - 200) / 365.25)
        anomalie = 0.7 * anomalie + rng.gauss(0, 1.8)
        pression += -0.15 * (pression - 1013.0) + rng.gauss(0, 4.0)
        t_jour = station.t_moy + station.amplitude * saison + anomalie
        humide = 72 + station.biais_hum - 1.1 * (pression - 1013)
        jour_pluvieux = rng.random() < 1 / (1 + math.exp(-(0.12 * (humide - 78) - 0.1 * (pression - 1013))))
        for h in range(24):
            averse = jour_pluvieux and rng.random() < 0.3
            cumul = rng.expovariate(1 / 1.2) if averse else 0.0
            parts = [rng.random() for _ in range(4)]
            total = sum(parts)
            for q in range(4):
                minutes = 60 * h + 15 * q + 15
                heure = minutes / 60
                t = t_jour + 4.5 * math.cos(2 * math.pi * (heure - 15) / 24) + rng.gauss(0, 0.3)
                u = min(100.0, max(15.0, humide - 12 * math.cos(2 * math.pi * (heure - 15) / 24)
                                   + rng.gauss(0, 3)))
                yield Quart(t0 + timedelta(days=j, minutes=minutes), t, u, pression,
                            round(cumul * parts[q] / total, 2))


# ---------------------------------------------------------------------------
# Bronze : les fichiers tels que les sources les livrent
# ---------------------------------------------------------------------------

def _fmt(x: float, nd: int) -> str:
    return f"{x:.{nd}f}"


def lignes_mf(station: Station, debut: date, n_jours: int, graine: int = 42) -> Iterator[tuple[str, str]]:
    """(AAAAMM, ligne CSV) pour une station Météo-France, défauts compris."""
    assert station.mfid is not None
    rng = random.Random(f"mf-{graine}-{station.ic_id}")
    fenetre: list[float] = []
    for q in serie_verite(station, debut, n_jours, graine):
        fenetre = (fenetre + [q.pluie])[-4:]
        if q.ts.minute != 0:
            continue
        if station.genre == "mf_secondaire" and q.ts.hour not in HEURES_SECONDAIRES:
            continue
        r = rng.random()
        t = "" if r < 0.02 else (rng.choice(("85.0", "-95.0")) if r < 0.025 else _fmt(q.t_c, 1))
        rr = "" if station.genre == "mf_secondaire" else _fmt(round(sum(fenetre), 2), 1)
        yield (q.ts.strftime("%Y%m"),
               f"{station.mfid};{q.ts.strftime('%Y%m%d%H')};{t};{q.u_pct:.0f};{q.pmer:.1f};{rr}")


def lignes_static(station: Station, debut: date, n_jours: int, graine: int = 42) -> Iterator[tuple[str, str]]:
    """(AAAAMM, ligne JSON) pour une station du réseau participatif, défauts compris."""
    rng = random.Random(f"st-{graine}-{station.ic_id}")
    fenetre: list[float] = []
    for q in serie_verite(station, debut, n_jours, graine):
        fenetre = (fenetre + [q.pluie])[-4:]
        r = rng.random()
        if r < 0.02:
            continue                                            # relevé perdu
        temp: float | None = round(q.t_c + 273.15, 2)
        if r < 0.03:
            temp = None
        elif r < 0.033:
            temp = 0.0                                          # capteur en défaut : 0 K
        precip: float | None = None if rng.random() < 0.005 else round(sum(fenetre), 2)
        dh = q.ts.strftime("%Y-%m-%dT%H:%M:%S") + ("+00:00" if rng.random() < 0.05 else "Z")
        yield (q.ts.strftime("%Y%m"),
               json.dumps({"ic_id": station.ic_id, "dh_utc": dh, "air_temperature": temp,
                           "relative_humidity": round(q.u_pct), "precip_1h": precip}))


@dataclass(frozen=True, slots=True)
class RapportBronze:
    fichiers: tuple[Path, ...]
    n_lignes_mf: int
    n_lignes_static: int


def ecrire_bronze(racine: Path, graine: int = 42, debut: date = date(2024, 1, 1), n_jours: int = 731,
                  n_principales: int = 4, n_secondaires: int = 2, n_static: int = 3) -> RapportBronze:
    """Coquille impérative : écrit bronze/{ref,mf,static}. Un fichier par mois et par source.

    Les mois tirés au sort reçoivent un fichier de rattrapage `AAAAMM_r1.csv` qui
    relivre environ 15 % des lignes du mois (doublons exacts).
    """
    stations = referentiel(graine, n_principales, n_secondaires, n_static)
    (racine / "ref").mkdir(parents=True, exist_ok=True)
    (racine / "mf").mkdir(exist_ok=True)
    (racine / "static").mkdir(exist_ok=True)
    fichiers: list[Path] = []

    ref = racine / "ref" / "stations.csv"
    with ref.open("w", encoding="utf-8") as f:
        f.write(EN_TETE_REF + "\n")
        for s in stations:
            f.write(f"{s.ic_id},{s.mfid or ''},{s.genre},{s.libelle_source},{s.latitude},{s.longitude},"
                    f"{s.altitude},{s.departement},{s.dh_ouverture}\n")
    fichiers.append(ref)

    par_mois_mf: dict[str, list[str]] = {}
    par_mois_st: dict[str, list[str]] = {}
    for s in stations:
        source = lignes_mf if s.genre != "static" else lignes_static
        cible = par_mois_mf if s.genre != "static" else par_mois_st
        for mois, ligne in source(s, debut, n_jours, graine):
            cible.setdefault(mois, []).append(ligne)

    rng = random.Random(f"rattrapage-{graine}")
    for mois, lignes in sorted(par_mois_mf.items()):
        chemin = racine / "mf" / f"{mois}.csv"
        chemin.write_text(EN_TETE_MF + "\n" + "\n".join(lignes) + "\n", encoding="utf-8")
        fichiers.append(chemin)
        if rng.random() < 0.35:
            relivre = [ligne for ligne in lignes if rng.random() < 0.15]
            r1 = racine / "mf" / f"{mois}_r1.csv"
            r1.write_text(EN_TETE_MF + "\n" + "\n".join(relivre) + "\n", encoding="utf-8")
            fichiers.append(r1)
    for mois, lignes in sorted(par_mois_st.items()):
        chemin = racine / "static" / f"{mois}.jsonl"
        chemin.write_text("\n".join(lignes) + "\n", encoding="utf-8")
        fichiers.append(chemin)
    return RapportBronze(tuple(fichiers), sum(map(len, par_mois_mf.values())),
                         sum(map(len, par_mois_st.values())))


# ---------------------------------------------------------------------------
# Silver massif (partie C) : généré directement en Arrow, vectorisé avec numpy
# ---------------------------------------------------------------------------

SCHEMA_SILVER = pa.schema([
    pa.field("ic_id", pa.string(), nullable=False),
    pa.field("ts", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("t_c", pa.float64()),
    pa.field("u_pct", pa.float64()),
    pa.field("rr1_mm", pa.float64()),
    pa.field("source", pa.string(), nullable=False),
])


def silver_massif(graine: int = 42, n_stations: int = 200, n_annees: int = 10,
                  obs_par_jour: int = 8, debut: date = date(2016, 1, 1)) -> Iterator[pa.Table]:
    """Générateur paresseux : une table Silver par station (réseau synoptique, 1 obs / 3 h).

    1 000 stations × 10 ans × 8 obs/jour ≈ 29 millions de lignes. Mémoire constante :
    une seule station est en mémoire à la fois.
    """
    n_jours = (date(debut.year + n_annees, debut.month, debut.day) - debut).days
    pas_us = 86_400_000_000 // obs_par_jour
    t0 = int(datetime(debut.year, debut.month, debut.day, tzinfo=UTC).timestamp()) * 1_000_000
    ts = t0 + np.arange(n_jours * obs_par_jour, dtype=np.int64) * pas_us
    jour = np.arange(n_jours * obs_par_jour) // obs_par_jour
    heure = (np.arange(n_jours * obs_par_jour) % obs_par_jour) * (24 / obs_par_jour)
    doy = (np.datetime64(debut.isoformat()) + jour.astype("timedelta64[D]") -
           (np.datetime64(debut.isoformat()) + jour.astype("timedelta64[D]")).astype("datetime64[Y]")
           ).astype(np.int64)
    saison = np.cos(2 * np.pi * (doy - 199) / 365.25)
    diurne = np.cos(2 * np.pi * (heure - 15) / 24)
    for k in range(1, n_stations + 1):
        rng = np.random.default_rng([graine, k])
        t_moy, ampl = rng.uniform(6.0, 17.0), rng.uniform(3.0, 11.0)
        bruit_jour = np.repeat(rng.normal(0, 2.2, n_jours), obs_par_jour)
        t = t_moy + ampl * saison + 4.0 * diurne + bruit_jour + rng.normal(0, 0.4, ts.size)
        u = np.clip(75 - 10 * diurne + rng.normal(0, 8, ts.size), 15, 100)
        pluie = np.where(rng.random(ts.size) < 0.12, rng.exponential(1.5, ts.size), 0.0)
        yield pa.table({
            "ic_id": pa.array([f"ST{k:04d}"] * ts.size, pa.string()),
            "ts": pa.array(ts, pa.timestamp("us", tz="UTC")),
            "t_c": np.round(t, 1),
            "u_pct": np.round(u, 0),
            "rr1_mm": np.round(pluie, 1),
            "source": pa.array(["synop"] * ts.size, pa.string()),
        }, schema=SCHEMA_SILVER)


def main() -> None:  # pragma: no cover - coquille impérative
    args = sys.argv[1:]
    racine = Path(args[0] if args else "data/bronze")
    rapport = ecrire_bronze(racine)
    print(f"Écrit : {racine} ({len(rapport.fichiers)} fichiers, "
          f"{rapport.n_lignes_mf} lignes MF, {rapport.n_lignes_static} lignes participatives)")


if __name__ == "__main__":  # pragma: no cover
    main()
