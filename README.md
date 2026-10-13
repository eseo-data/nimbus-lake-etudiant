# Nimbus-Lake : fil rouge du module Big Data

ESEO, cycle ingénieur, 5e année, spécialité IA & Data. Module E5eV-S09-DAIA-DATA2-BIG, 8 blocs de 3 h 30.

On construit un lakehouse climatologique de bout en bout, du fichier brut livré par une source jusqu'à une API de données. Le modèle réel est le lakehouse d'Infoclimat (<https://portail.chom.engineering>) : 39 milliards de mesures, des tables Iceberg sur S3, un catalogue REST, une API JSON paginée. Ici, les données sont synthétiques et générées en local par une fonction pure à graine fixe. Elles reprennent la forme et les défauts des vraies données. Les TP ne téléchargent rien et tournent sans GPU sur un laptop de 8 Go.

## Installation (10 minutes)

1. Installer uv (il installe aussi Python 3.12 si besoin) :
   - macOS / Linux : `curl -LsSf https://astral.sh/uv/install.sh | sh`
   - Windows (PowerShell) : `powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"`
2. Dans ce dossier :
   ```bash
   uv sync
   uv run pytest          # au départ, les tests échouent par NotImplementedError : c'est normal
   uv run mypy            # doit passer
   ```
3. Générer le Bronze du bloc 1 (environ 30 Mo, quelques secondes) :
   ```bash
   uv run python -m nimbus_lake.donnees data/bronze
   ```

Docker n'est pas utile au bloc 1. Il servira au bloc 5 (Kafka / Redpanda).

## Organisation

| Bloc | Thème | Modules |
|---|---|---|
| 1 | Data lake, lakehouse, medallion. Parquet, compression, partitioning | `b1_parquet.py`, `b1_pipeline.py`, `b1_echelle.py`, `b1_defi_footer.py` |
| 2 | Moteur analytique local : DuckDB, exécution vectorisée, out-of-core | à venir |
| 3 | Tables transactionnelles : Iceberg, snapshots, time travel, schema evolution | à venir |
| 4 | Spark en profondeur : shuffle, data skew, broadcast join, Spark UI | à venir |
| 5 | Kafka au niveau système : partitions, consumer groups, offsets, exactly-once | à venir |
| 6 | Orchestration et data contracts : Dagster, ODCS, tests de données | à venir |
| 7 | Sécurité, gouvernance, coûts | à venir |
| 8 | Serving, exploitation, soutenance du projet | à venir |

## Les données

`nimbus_lake.donnees` (fourni) écrit trois sources Bronze :

| Dossier | Format | Particularités |
|---|---|---|
| `bronze/ref/stations.csv` | CSV « , » | trois identifiants : `ic_id` (canonique), `mfid` (NUM_POSTE Météo-France), libellé brut. Les libellés « TEST MF CSV… » désignent de vraies stations. Une date d'ouverture inconnue s'écrit `0000-00-00 00:00:00`. |
| `bronze/mf/AAAAMM.csv` | CSV « ; », format Météo-France d'origine | NUM_POSTE à 8 chiffres avec zéro initial, horodatage `AAAAMMJJHH` en UTC, °C. Stations principales horaires, secondaires à 3 relevés par jour sans pluviomètre. `AAAAMM_r1.csv` = fichier de rattrapage qui relivre des lignes déjà livrées. |
| `bronze/static/AAAAMM.jsonl` | JSON Lines, unités SI | réseau participatif : température en kelvin, relevé toutes les 15 minutes, `precip_1h` = cumul glissant sur 60 minutes, horodatage `…Z` ou `…+00:00`. |

## Bloc 1 : déroulé du TP (2 h 30)

| Partie | Durée | Fichier | Contenu |
|---|---|---|---|
| A | 35 min | `b1_parquet.py` | lire le Bronze avec un schéma explicite ; écrire en Parquet ; lire le footer ; statistiques min/max par row group ; CSV vs Parquet, snappy vs zstd ; effet du tri sur le pruning |
| B | 70 min | `b1_pipeline.py` | reprendre `b1_legacy.py` (à ne pas modifier) en pipeline Bronze → Silver → Gold, mois par mois |
| C | 35 min | `b1_echelle.py` | 200 puis 1 000 stations × 10 ans : choisir une clé de partitioning, mesurer, compacter les petits fichiers |
| Bonus | | `b1_echelle.py` | la même agrégation avec DuckDB, lecture d'un `EXPLAIN ANALYZE` |
| Défi ★★★ | | `b1_defi_footer.py` | décoder le footer Parquet à la main (Thrift compact protocol), sans bibliothèque Parquet |

```bash
uv run pytest tests/test_b1_parquet.py      # partie A
uv run pytest tests/test_b1_pipeline.py     # partie B
uv run pytest tests/test_b1_echelle.py      # partie C et bonus
uv run pytest tests/test_b1_defi_footer.py  # défi (sauté tant qu'il n'est pas commencé)
uv run python -m nimbus_lake.b1_echelle 200 # banc d'essai de la partie C
```

Partie B, critère de réussite. Le script hérité est juste sur tn et tx, mais faux sur rr et n_obs. Votre Gold doit :
- reproduire ses tn et tx à 1e-9 près ;
- respecter la spécification écrite en tête de `b1_pipeline.py`. Les tests contiennent une implémentation de référence naïve en Python pur ;
- ne jamais lire plus d'un mois de Bronze à la fois. Un test espionne les lectures de fichiers.

Pour les agrégations, l'outil est libre : `pyarrow.compute` ou DuckDB (`duckdb.sql("SELECT … FROM silver").to_arrow_table()` lit directement une table Arrow nommée `silver`).

Partie C, à rendre dans votre compte rendu : le tableau du banc d'essai pour les cinq clés, avec votre choix de clé argumenté pour deux requêtes types. La première demande une station sur 10 ans, la seconde toutes les stations sur un mois.

## Règles du jeu

- Ne modifiez pas les tests fournis, ni `donnees.py`, ni `b1_legacy.py`. Ajoutez vos propres tests dans `tests/`.
- `uv run mypy` doit rester propre.
- Le dossier `data/` n'est pas versionné.
