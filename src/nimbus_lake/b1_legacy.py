# mypy: ignore-errors
"""Bloc 1, partie B — script HÉRITÉ. NE PAS MODIFIER.

Il tourne chaque nuit en cron depuis des années. Il lit tout le Bronze (référentiel,
fichiers Météo-France, fichiers du réseau participatif) et produit la climatologie
journalière par station : tn, tx, rr, n_obs.

Ce que l'on sait de lui :
- les températures (tn, tx) sont justes, les utilisateurs les ont vérifiées ;
- il charge tout en mémoire : à 1 000 stations il ne passe plus sur la machine ;
- on soupçonne la pluie et le nombre d'observations d'être faux sur certaines stations.

Réécrivez-le en pipeline Bronze -> Silver -> Gold dans `b1_pipeline.py`.
"""

import csv
import glob
import json
import os
from datetime import datetime, timezone

STATIONS = {}       # NUM_POSTE -> ic_id
JOURS = {}          # (ic_id, "AAAA-MM-JJ") -> {"temps": [...], "rr": [...], "n_obs": int}


def charger_referentiel(racine):
    with open(os.path.join(racine, "ref", "stations.csv"), encoding="utf-8") as f:
        for ligne in csv.DictReader(f):
            if ligne["mfid"]:
                STATIONS[ligne["mfid"]] = ligne["ic_id"]


def ajouter(ic_id, jour, t, rr):
    cle = (ic_id, jour)
    if cle not in JOURS:
        JOURS[cle] = {"temps": [], "rr": [], "n_obs": 0}
    if t is not None:
        JOURS[cle]["n_obs"] += 1
        if -90 <= t <= 60:
            JOURS[cle]["temps"].append(t)
    if rr is not None:
        JOURS[cle]["rr"].append(rr)


def lire_mf(racine):
    for chemin in sorted(glob.glob(os.path.join(racine, "mf", "*.csv"))):
        with open(chemin, encoding="utf-8") as f:
            lecteur = csv.reader(f, delimiter=";")
            next(lecteur)
            for num_poste, aaaammjjhh, t, _u, _pmer, rr1 in lecteur:
                jour = f"{aaaammjjhh[0:4]}-{aaaammjjhh[4:6]}-{aaaammjjhh[6:8]}"
                ajouter(STATIONS[num_poste], jour, float(t) if t else None, float(rr1) if rr1 else None)


def lire_static(racine):
    for chemin in sorted(glob.glob(os.path.join(racine, "static", "*.jsonl"))):
        with open(chemin, encoding="utf-8") as f:
            for ligne in f:
                obs = json.loads(ligne)
                dh = datetime.fromisoformat(obs["dh_utc"]).astimezone(timezone.utc)
                t = obs["air_temperature"]
                ajouter(obs["ic_id"], dh.date().isoformat(),
                        t - 273.15 if t is not None else None, obs["precip_1h"])


def executer(racine):
    charger_referentiel(racine)
    lire_mf(racine)
    lire_static(racine)
    resultat = []
    for ic_id, jour in sorted(JOURS):
        d = JOURS[(ic_id, jour)]
        resultat.append({
            "ic_id": ic_id,
            "date": jour,
            "tn": min(d["temps"]) if d["temps"] else None,
            "tx": max(d["temps"]) if d["temps"] else None,
            "rr": sum(d["rr"]) if d["rr"] else None,
            "n_obs": d["n_obs"],
        })
    return resultat


def main(racine, sortie):
    lignes = executer(racine)
    with open(sortie, "w", encoding="utf-8", newline="") as f:
        ecrivain = csv.DictWriter(f, fieldnames=["ic_id", "date", "tn", "tx", "rr", "n_obs"])
        ecrivain.writeheader()
        ecrivain.writerows(lignes)
    print(f"{len(lignes)} lignes écrites dans {sortie}")


if __name__ == "__main__":
    import sys
    main(sys.argv[1], sys.argv[2])
