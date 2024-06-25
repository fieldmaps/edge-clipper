import logging

from psycopg import connect
from psycopg.sql import SQL, Identifier, Literal

from .utils import ADM_JOIN, ADM_LEVELS, DATABASE, get_src_ids, get_wld_ids

logger = logging.getLogger(__name__)

query_1 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        {ids_src},
        {ids_wld},
        a.geom
    FROM {table_in1} AS a
    JOIN {table_in2} AS b
    ON ST_Within(a.geom, b.geom)
    WHERE {join} = {id}
    UNION ALL
    SELECT
        {ids_src},
        {ids_wld},
        ST_Multi(
            ST_CollectionExtract(
                ST_Intersection(a.geom, b.geom)
            , 3)
        )::GEOMETRY(MultiPolygon, 4326) as geom
    FROM {table_in1} AS a
    JOIN {table_in2} AS b
    ON ST_Intersects(a.geom, b.geom)
    AND NOT ST_Within(a.geom, b.geom)
    WHERE {join} = {id};
"""
query_2 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        {ids_src},
        {ids_wld},
        ST_Multi(
            ST_Union(geom)
        )::GEOMETRY(MultiPolygon, 4326) AS geom
    FROM {table_in} AS a
    GROUP BY {ids_src}, {ids_wld};
"""


def main(file):
    name = file.stem
    conn = connect(f"dbname={DATABASE}", autocommit=True)
    conn.execute(
        SQL(query_1).format(
            table_in1=Identifier(f"admx_{name}_1"),
            table_in2=Identifier("adm0_polygons"),
            id=Literal(name),
            join=Identifier("b", ADM_JOIN),
            ids_src=SQL(",").join(map(lambda x: Identifier("a", x), get_src_ids())),
            ids_wld=SQL(",").join(map(lambda x: Identifier("a", x), get_wld_ids())),
            table_out=Identifier(f"adm{ADM_LEVELS}_{name}"),
        )
    )
    for lvl in range(ADM_LEVELS - 1, 0, -1):
        conn.execute(
            SQL(query_2).format(
                table_in=Identifier(f"adm{lvl+1}_{name}"),
                ids_src=SQL(",").join(
                    map(lambda x: Identifier("a", x), get_src_ids(lvl))
                ),
                ids_wld=SQL(",").join(map(lambda x: Identifier("a", x), get_wld_ids())),
                table_out=Identifier(f"adm{lvl}_{name}"),
            )
        )
    conn.close()
    logger.info(name)
