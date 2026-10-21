# mypy: ignore-errors
"""Bloc 2, partie B — script HÉRITÉ. NE PAS MODIFIER.

Il calcule, pour chaque station et chaque année, des indicateurs de chaleur à partir de
la climatologie journalière : jours de tx >= 30 °C, nuits tropicales (tn >= 20 °C), épisodes
chauds (au moins 3 jours de suite à tx >= 30 °C), plus long épisode et sa date de début.

Il a été écrit pour 30 stations. À 1 000 stations sur 10 ans, il met plusieurs minutes,
et un utilisateur signale des épisodes « trop longs » sur certaines stations.

Réécrivez-le en SQL DuckDB dans `b2_canicule.py`.
"""

import pyarrow.parquet as pq

SEUIL_TX = 30.0
SEUIL_TN = 20.0
DUREE_MIN = 3


def executer(chemin_parquet):
    lignes = pq.read_table(chemin_parquet).to_pylist()            # tout en mémoire
    lignes.sort(key=lambda r: (r["ic_id"], r["date"]))
    resultat = []
    courant = None
    for r in lignes:
        cle = (r["ic_id"], r["date"].year)
        if courant is None or courant["cle"] != cle:
            if courant is not None:
                _fermer(courant)
                resultat.append(courant)
            courant = {"cle": cle, "n_jours_tx30": 0, "n_nuits_tropicales": 0, "n_episodes": 0,
                       "plus_long_episode": 0, "debut_plus_long": None, "serie": 0, "debut_serie": None}
        if r["tn"] is not None and r["tn"] >= SEUIL_TN:
            courant["n_nuits_tropicales"] += 1
        if r["tx"] is not None and r["tx"] >= SEUIL_TX:
            courant["n_jours_tx30"] += 1
            if courant["serie"] == 0:
                courant["debut_serie"] = r["date"]
            courant["serie"] += 1                                    # jour suivant dans la liste
        else:
            _fermer(courant)
    if courant is not None:
        _fermer(courant)
        resultat.append(courant)
    return [{"ic_id": c["cle"][0], "annee": c["cle"][1], "n_jours_tx30": c["n_jours_tx30"],
             "n_nuits_tropicales": c["n_nuits_tropicales"], "n_episodes": c["n_episodes"],
             "plus_long_episode": c["plus_long_episode"], "debut_plus_long": c["debut_plus_long"]}
            for c in resultat]


def _fermer(c):
    if c["serie"] >= DUREE_MIN:
        c["n_episodes"] += 1
        if c["serie"] > c["plus_long_episode"]:
            c["plus_long_episode"] = c["serie"]
            c["debut_plus_long"] = c["debut_serie"]
    c["serie"] = 0
    c["debut_serie"] = None
