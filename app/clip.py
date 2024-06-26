import logging

from psycopg.sql import SQL, Identifier, Literal

from .utils import ADM0_JOIN, ADM_LEVELS, get_src_ids, get_wld_ids

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
    WHERE {join} = {id};
"""
query_2 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
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
query_3 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT * FROM {table_in1}
    UNION ALL
    SELECT * FROM {table_in2};
"""
drop_tmp = """
    DROP TABLE IF EXISTS {table_tmp1};
    DROP TABLE IF EXISTS {table_tmp2};
"""


def main(conn, file):
    name = file.stem
    for query, index in [(query_1, 1), (query_2, 2)]:
        conn.execute(
            SQL(query).format(
                table_in1=Identifier(f"admx_{name}_1"),
                table_in2=Identifier("adm0_polygons"),
                id=Literal(name),
                join=Identifier("b", ADM0_JOIN),
                ids_src=SQL(",").join(
                    map(lambda x: Identifier("a", x), get_src_ids(conn, name))
                ),
                ids_wld=SQL(",").join(
                    map(lambda x: Identifier("a", x), get_wld_ids(conn))
                ),
                table_out=Identifier(f"adm{ADM_LEVELS}_{name}_{index}"),
            )
        )
    conn.execute(
        SQL(query_3).format(
            table_in1=Identifier(f"adm{ADM_LEVELS}_{name}_1"),
            table_in2=Identifier(f"adm{ADM_LEVELS}_{name}_2"),
            table_out=Identifier(f"adm{ADM_LEVELS}_{name}"),
        )
    )
    conn.execute(
        SQL(drop_tmp).format(
            table_tmp1=Identifier(f"adm{ADM_LEVELS}_{name}_1"),
            table_tmp2=Identifier(f"adm{ADM_LEVELS}_{name}_2"),
        )
    )
