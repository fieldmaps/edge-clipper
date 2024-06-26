import logging
import subprocess

from psycopg import connect
from psycopg.sql import SQL, Identifier, Literal

from .utils import (
    ADM0_ID,
    ADM_JOIN,
    ADM_LEVELS,
    DATABASE,
    get_adm_id,
    get_src_ids,
    get_wld_ids,
)

logger = logging.getLogger(__name__)

adm4_id = [f"{get_adm_id(4)} = {{adm0_id}}"] if ADM_LEVELS >= 4 else []
adm3_id = [f"{get_adm_id(3)} = {{adm0_id}}"] if ADM_LEVELS >= 3 else []
adm2_id = [f"{get_adm_id(2)} = {{adm0_id}}"] if ADM_LEVELS >= 2 else []
adm1_id = [f"{get_adm_id(1)} = {{adm0_id}}"] if ADM_LEVELS >= 1 else []

query_1 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT * FROM {table_in}
    WHERE {join} NOT IN ({ids});
"""
query_1a = """
    ALTER TABLE {table_out}
    ADD COLUMN IF NOT EXISTS {name} VARCHAR;
"""
query_2 = (
    f"UPDATE {{table_out}} SET {','.join([*adm4_id, *adm3_id, *adm2_id, *adm1_id])};"
)
query_3 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        {ids_src},
        {ids_wld},
        a.geom
    FROM {table_in1} AS a;
"""
drop_tmp = """
    DROP TABLE IF EXISTS {table_tmp1};
"""


def main(admx_files: list):
    join_list = map(lambda x: x.stem, admx_files)
    conn = connect(f"dbname={DATABASE}", autocommit=True)
    for lvl in range(1, ADM_LEVELS + 1):
        conn.execute(
            SQL(drop_tmp).format(table_tmp1=Identifier(f"adm{lvl}_polygons_1"))
        )
    conn.execute(
        SQL(query_1).format(
            table_in=Identifier("adm0_polygons"),
            join=Identifier(ADM_JOIN),
            ids=SQL(",").join(map(Literal, join_list)),
            table_out=Identifier(f"adm{ADM_LEVELS}_polygons_tmp1"),
        )
    )
    for id in get_src_ids():
        conn.execute(
            SQL(query_1a).format(
                name=Identifier(id),
                table_out=Identifier(f"adm{ADM_LEVELS}_polygons_tmp1"),
            )
        )
    conn.execute(
        SQL(query_2).format(
            adm0_id=Identifier(ADM0_ID),
            table_out=Identifier(f"adm{ADM_LEVELS}_polygons_tmp1"),
        )
    )
    conn.execute(
        SQL(query_3).format(
            table_in1=Identifier(f"adm{ADM_LEVELS}_polygons_tmp1"),
            ids_src=SQL(",").join(map(lambda x: Identifier("a", x), get_src_ids())),
            ids_wld=SQL(",").join(map(lambda x: Identifier("a", x), get_wld_ids())),
            id=Identifier(get_adm_id(ADM_LEVELS)),
            table_out=Identifier(f"adm{ADM_LEVELS}_polygons_1"),
        )
    )
    for lvl in range(ADM_LEVELS - 1, 0, -1):
        conn.execute(
            SQL(query_3).format(
                table_in1=Identifier(f"adm{lvl+1}_polygons_1"),
                ids_src=SQL(",").join(
                    map(lambda x: Identifier("a", x), get_src_ids(lvl))
                ),
                ids_wld=SQL(",").join(map(lambda x: Identifier("a", x), get_wld_ids())),
                id=Identifier(f"adm{lvl}_id"),
                table_out=Identifier(f"adm{lvl}_polygons_1"),
            )
        )
    for lvl in range(ADM_LEVELS, 0, -1):
        subprocess.run(
            [
                "ogr2ogr",
                *["--config", "PG_USE_COPY", "YES"],
                "-append",
                *["-f", "PostgreSQL"],
                *["-nln", f"adm{lvl}_polygons"],
                f"PG:dbname={DATABASE}",
                *[f"PG:dbname={DATABASE}", f"adm{lvl}_polygons_1"],
            ]
        )
        conn.execute(
            SQL(drop_tmp).format(table_tmp1=Identifier(f"adm{lvl}_polygons_1"))
        )
    conn.execute(
        SQL(drop_tmp).format(table_tmp1=Identifier(f"adm{ADM_LEVELS}_polygons_tmp1"))
    )
    conn.close()
    logger.info("finished")
