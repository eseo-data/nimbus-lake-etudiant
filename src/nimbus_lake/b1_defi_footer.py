"""Bloc 1, défi ★★★ — lire le footer Parquet à la main.

Facultatif. Aucun import de pyarrow, duckdb, polars ou fastparquet dans ce module :
seulement la bibliothèque standard. Les tests le vérifient, ainsi que le nombre
d'octets lus (vous ne devez lire que la fin du fichier).

Structure d'un fichier Parquet :

    "PAR1" | données des row groups ... | footer (FileMetaData) | longueur du footer (4 octets LE) | "PAR1"

Le footer est une structure Thrift sérialisée en « compact protocol » :
- en-tête de champ : un octet. 4 bits de poids fort = écart avec l'identifiant de
  champ précédent (si 0, l'identifiant suit en varint zigzag sur 16 bits), 4 bits de
  poids faible = type ;
- types : 0 STOP, 1 TRUE, 2 FALSE, 3 i8, 4 i16, 5 i32, 6 i64, 7 double, 8 binary,
  9 list, 10 set, 11 map, 12 struct ;
- entiers : varint (7 bits par octet, bit 8 = « suite ») puis zigzag ((n >> 1) ^ -(n & 1)) ;
- binary : longueur en varint puis les octets ;
- list/set : un octet, taille sur 4 bits de poids fort (15 = la taille suit en varint),
  type des éléments sur 4 bits de poids faible ; dans une liste, un booléen tient sur un octet ;
- map : taille en varint ; si elle est non nulle, un octet (type clé << 4 | type valeur) ;
- double : 8 octets little-endian.

Champs utiles (parquet.thrift) :
- FileMetaData : 2 schema (list<SchemaElement>), 3 num_rows (i64), 4 row_groups (list<RowGroup>)
- SchemaElement : 4 name (string), 5 num_children (i32)
- RowGroup : 1 columns (list<ColumnChunk>), 3 num_rows (i64)
- ColumnChunk : 3 meta_data (ColumnMetaData)
- ColumnMetaData : 3 path_in_schema (list<string>), 12 statistics (Statistics)
- Statistics : 3 null_count (i64), 5 max_value (binary), 6 min_value (binary)

Un int64 ou un timestamp est stocké en min_value/max_value sur 8 octets little-endian signés.

Référence : https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from typing import BinaryIO

Valeur = int | float | bool | bytes | list["Valeur"] | dict[int, "Valeur"]


@dataclass(frozen=True, slots=True)
class StatsColonne:
    min_value: bytes | None
    max_value: bytes | None
    null_count: int | None


@dataclass(frozen=True, slots=True)
class RowGroupMeta:
    num_rows: int
    colonnes: dict[str, StatsColonne]


@dataclass(frozen=True, slots=True)
class MetaParquet:
    num_rows: int
    colonnes: tuple[str, ...]        # colonnes feuilles, sans la racine du schéma
    row_groups: tuple[RowGroupMeta, ...]


def lire_octets_footer(f: BinaryIO) -> bytes:
    """D1. Renvoie les octets du footer (FileMetaData), sans lire le reste du fichier.

    Vérifier la signature « PAR1 » finale ; lever ValueError sinon.
    """
    raise NotImplementedError("Bloc 1 D1 — à compléter")


class LecteurCompact:
    """D2. Décodeur générique du Thrift compact protocol.

    `lire_struct()` renvoie un dict {identifiant de champ: valeur} ; les sous-structures
    sont des dict, les listes des list. Les champs inconnus sont décodés aussi (on ne
    peut pas sauter un champ sans le décoder).
    """

    def __init__(self, octets: bytes) -> None:
        self.octets = octets
        self.pos = 0

    # Écrivez ici les méthodes auxiliaires : _varint, _zigzag, _valeur...

    def lire_struct(self) -> dict[int, Valeur]:
        raise NotImplementedError("Bloc 1 D2 — à compléter")


def lire_footer(f: BinaryIO) -> MetaParquet:
    """D3. Footer -> MetaParquet. Les colonnes sont nommées par leur path_in_schema joint par « . »."""
    raise NotImplementedError("Bloc 1 D3 — à compléter")


def row_groups_a_lire(meta: MetaParquet, colonne: str, bas: int, haut: int) -> list[int]:
    """D4. Row groups qui peuvent contenir une valeur int64 dans [bas, haut].

    Un row group sans statistiques doit être lu (on ne peut pas l'exclure).
    """
    raise NotImplementedError("Bloc 1 D4 — à compléter")
