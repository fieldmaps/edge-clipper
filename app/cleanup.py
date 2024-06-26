import logging
from pathlib import Path

from psycopg import connect
from psycopg.sql import SQL, Identifier

from .utils import ADM_LEVELS, DATABASE

logger = logging.getLogger(__name__)

drop_tmp = """
    DROP TABLE IF EXISTS {table_tmp1} CASCADE;
"""


def adm0():
    conn = connect(f"dbname={DATABASE}", autocommit=True)
    conn.execute(SQL(drop_tmp).format(table_tmp1=Identifier("adm0_polygons")))
    conn.execute(SQL(drop_tmp).format(table_tmp1=Identifier("adm0_attributes")))
    conn.close()
    logger.info("adm0_polygons")


def admx(conn, file: Path):
    name = file.stem
    conn = connect(f"dbname={DATABASE}", autocommit=True)
    conn.execute(SQL(drop_tmp).format(table_tmp1=Identifier(f"admx_{name}")))
    conn.execute(SQL(drop_tmp).format(table_tmp1=Identifier(f"admx_{name}_1")))
    for lvl in range(1, ADM_LEVELS + 1):
        conn.execute(SQL(drop_tmp).format(table_tmp1=Identifier(f"adm{lvl}_{name}")))
    conn.close()


def dest_admx(lvl: int, _, __):
    conn = connect(f"dbname={DATABASE}", autocommit=True)
    conn.execute(SQL(drop_tmp).format(table_tmp1=Identifier(f"adm{lvl}_polygons")))
    conn.close()
    logger.info(f"adm{lvl}_polygons")
