import logging
import subprocess

from psycopg import connect
from psycopg.sql import SQL, Identifier, Literal

from .utils import (
    ADM0_ID,
    ADM0_JOIN,
    ADM_LEVELS,
    DATABASE,
    get_adm_id,
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
query_2 = """
    ALTER TABLE {table_out}
    ADD COLUMN IF NOT EXISTS {name} VARCHAR;
"""
query_3 = (
    f"UPDATE {{table_out}} SET {','.join([*adm4_id, *adm3_id, *adm2_id, *adm1_id])};"
)
query_4 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT
        {ids_src},
        {ids_wld},
        a.geom
    FROM {table_in} AS a;
"""
query_5 = """
    ALTER TABLE {table_in}
    DROP COLUMN fid;
"""
drop_tmp = """
    DROP TABLE IF EXISTS {table_tmp1};
"""


def main(admx_files: list):
    join_list = map(lambda x: x.stem, admx_files)
    conn = connect(f"dbname={DATABASE}", autocommit=True)
    for lvl in range(ADM_LEVELS, 0, -1):
        conn.execute(
            SQL(drop_tmp).format(table_tmp1=Identifier(f"adm{lvl}_polygons_1"))
        )
    conn.execute(
        SQL(query_1).format(
            table_in=Identifier("adm0_polygons"),
            join=Identifier(ADM0_JOIN),
            ids=SQL(",").join(map(Literal, join_list)),
            table_out=Identifier(f"adm{ADM_LEVELS}_polygons_tmp1"),
        )
    )
    for lvl in range(ADM_LEVELS, 0, -1):
        conn.execute(
            SQL(query_2).format(
                name=Identifier(get_adm_id(lvl)),
                table_out=Identifier(f"adm{ADM_LEVELS}_polygons_tmp1"),
            )
        )
    conn.execute(
        SQL(query_3).format(
            adm0_id=Identifier(ADM0_ID),
            table_out=Identifier(f"adm{ADM_LEVELS}_polygons_tmp1"),
        )
    )
    conn.execute(
        SQL(query_4).format(
            table_in1=Identifier(f"adm{ADM_LEVELS}_polygons_tmp1"),
            ids_src=SQL(",").join(
                map(lambda x: Identifier("a", get_adm_id(x)), range(ADM_LEVELS, 0, -1))
            ),
            ids_wld=SQL(",").join(map(lambda x: Identifier("a", x), get_wld_ids(conn))),
            id=Identifier(get_adm_id(ADM_LEVELS)),
            table_out=Identifier(f"adm{ADM_LEVELS}_polygons_1"),
        )
    )
    for lvl in range(ADM_LEVELS - 1, 0, -1):
        conn.execute(
            SQL(query_4).format(
                table_in=Identifier(f"adm{lvl+1}_polygons_1"),
                ids_src=SQL(",").join(
                    map(
                        lambda x: Identifier("a", get_adm_id(x)),
                        range(ADM_LEVELS, 0, -1),
                    )
                ),
                ids_wld=SQL(",").join(
                    map(lambda x: Identifier("a", x), get_wld_ids(conn))
                ),
                table_out=Identifier(f"adm{lvl}_polygons_1"),
            )
        )
    for lvl in range(ADM_LEVELS, 0, -1):
        subprocess.run(
            [
                "ogr2ogr",
                *["--config", "PG_USE_COPY", "YES"],
                "-append",
                *["-nln", f"adm{lvl}_polygons"],
                *["-f", "PostgreSQL"],
                f"PG:dbname={DATABASE}",
                *[f"PG:dbname={DATABASE}", f"adm{lvl}_polygons_1"],
            ]
        )
        conn.execute(SQL(query_5).format(table_in=Identifier(f"adm{lvl}_polygons")))
        conn.execute(
            SQL(drop_tmp).format(table_tmp1=Identifier(f"adm{lvl}_polygons_1"))
        )
    conn.execute(
        SQL(drop_tmp).format(table_tmp1=Identifier(f"adm{ADM_LEVELS}_polygons_tmp1"))
    )
    conn.close()
    logger.info("finished")
