import logging

from psycopg.sql import SQL, Identifier

from .utils import ADM_LEVELS, get_src_ids, get_wld_ids

logger = logging.getLogger(__name__)

query_1 = """
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


def main(conn, file):
    name = file.stem
    for lvl in range(ADM_LEVELS - 1, 0, -1):
        conn.execute(
            SQL(query_1).format(
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
