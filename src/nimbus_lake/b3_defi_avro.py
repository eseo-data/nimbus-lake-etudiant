"""Bloc 3, défi ★★★ — le time travel à la main : de metadata.json aux fichiers de données.

Facultatif. Seulement la bibliothèque standard (json, zlib, struct...) : pas de pyiceberg,
fastavro, avro ni pyarrow dans ce module. Les tests le vérifient.

Chemin à suivre pour un snapshot :
1. metadata.json (JSON) : la liste "snapshots", chacun avec "snapshot-id" et "manifest-list" ;
   "current-snapshot-id" donne le snapshot courant ;
2. la manifest list (fichier Avro) : un enregistrement par manifest, champ "manifest_path" ;
3. chaque manifest (fichier Avro) : un enregistrement par fichier de données, avec "status"
   (0 EXISTING, 1 ADDED, 2 DELETED) et "data_file" -> "file_path".
Les fichiers du snapshot = ceux dont le status n'est pas DELETED.

Format Avro « object container file » :
- "Obj\\x01", puis une map de métadonnées (bytes) : "avro.schema" (JSON) et "avro.codec"
  ("null" ou "deflate"), puis 16 octets de synchronisation ;
- puis des blocs : nombre d'objets (long), taille en octets (long), données (compressées en
  deflate brut si codec = deflate : zlib.decompress(données, -15)), 16 octets de synchro ;
- long et int : varint zigzag ; string et bytes : longueur (long) puis octets ;
  boolean : un octet ; float/double : 4/8 octets little-endian ; fixed : taille du schéma ;
- record : ses champs dans l'ordre ; union : index (long) de la branche, puis la valeur ;
- array et map : des blocs « nombre (long), éléments », terminés par un nombre 0 ; un nombre
  négatif est suivi de la taille du bloc en octets (à lire puis ignorer) ; map : clés string.

Référence : https://avro.apache.org/docs/1.11.1/specification/
"""

from __future__ import annotations

import json
import struct
import zlib
from pathlib import Path
from typing import Any


def _chemin(uri: str) -> Path:
    return Path(uri.removeprefix("file://"))


class LecteurAvro:
    """D1. Décodeur Avro minimal sur un tampon d'octets."""

    def __init__(self, octets: bytes) -> None:
        self.octets = octets
        self.pos = 0

    # Écrivez ici les méthodes du décodeur : varint zigzag, primitifs, record, union, array, map...


def lire_conteneur(chemin: Path) -> list[dict[str, Any]]:
    """D2. Tous les enregistrements d'un fichier conteneur Avro (codecs null et deflate)."""
    raise NotImplementedError("Bloc 3 D2 — à compléter")


def fichiers_du_snapshot(metadata_json: Path, snapshot_id: int | None = None) -> list[str]:
    """D3. Chemins (tels qu'écrits dans les manifests) des fichiers de données du snapshot
    `snapshot_id` (le courant si None), triés. Lève KeyError si le snapshot n'existe pas."""
    raise NotImplementedError("Bloc 3 D3 — à compléter")
