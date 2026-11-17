# mypy: ignore-errors
"""Bloc 3, partie B — script HÉRITÉ de publication du Gold. NE PAS MODIFIER.

Chaque matin, il intègre la livraison de la veille dans le Gold publié sous forme de
dossier Parquet partitionné par mois (gold/mois=AAAA-MM/part.parquet) :

1. il relit tout le Gold existant ;
2. il fusionne la livraison (pour une même station et une même date, la ligne reçue en
   dernier gagne) ;
3. il SUPPRIME le dossier, puis réécrit tous les mois.

Incidents connus : pendant la réécriture, les tableaux de bord voient des mois manquants ;
une coupure au milieu de l'étape 3 a déjà fait perdre des mois entiers ; impossible de
revenir à la veille après une livraison erronée.

Réécrivez la publication sur une table Iceberg dans `b3_publication.py`.
"""

import os
import shutil

import pyarrow as pa
import pyarrow.parquet as pq

COLONNES = ["ic_id", "date", "tn", "tx", "rr"]


def lire(racine):
    if not os.path.isdir(racine):
        return None
    morceaux = [pq.read_table(os.path.join(racine, d, "part.parquet")) for d in sorted(os.listdir(racine))]
    return pa.concat_tables(morceaux) if morceaux else None


def publier(racine, livraison, panne=None):
    existant = lire(racine)
    lignes = {}
    if existant is not None:
        for r in existant.to_pylist():
            lignes[(r["ic_id"], r["date"])] = (None, r)
    for r in livraison.to_pylist():
        cle = (r["ic_id"], r["date"])
        if cle not in lignes or lignes[cle][0] is None or r["recu_a"] >= lignes[cle][0]:
            lignes[cle] = (r["recu_a"], {c: r[c] for c in COLONNES})
    par_mois = {}
    for (_, jour), (_, r) in lignes.items():
        par_mois.setdefault(f"{jour:%Y-%m}", []).append(r)
    shutil.rmtree(racine, ignore_errors=True)                      # les lecteurs ne voient plus rien
    for i, (mois, rs) in enumerate(sorted(par_mois.items())):
        if panne is not None and i == 1:
            panne()                                                # coupure au milieu de la réécriture
        os.makedirs(os.path.join(racine, f"mois={mois}"))
        rs.sort(key=lambda r: (r["ic_id"], r["date"]))
        pq.write_table(pa.Table.from_pylist(rs), os.path.join(racine, f"mois={mois}", "part.parquet"))
