import logging
import subprocess
from pathlib import Path

from psycopg import connect
from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier

from .utils import DATABASE

logger = logging.getLogger(__name__)

query_1 = """
    SELECT * FROM {table_in};
"""
query_2 = """
    DROP TABLE IF EXISTS {table_out};
    CREATE TABLE {table_out} AS
    SELECT {cols}
    FROM {table_in};
"""


def create_attr_table(table_in: str, table_out: str):
    conn = connect(f"dbname={DATABASE}", autocommit=True)
    cur = conn.cursor(row_factory=dict_row)
    row = cur.execute(SQL(query_1).format(table_in=Identifier(table_in))).fetchone()
    cols = list(row.keys())
    cols.remove("fid")
    cols.remove("geom")
    conn.execute(
        SQL(query_2).format(
            table_in=Identifier(table_in),
            cols=SQL(",").join(map(Identifier, cols)),
            table_out=Identifier(table_out),
        )
    )
    conn.close()


def adm0(file: Path):
    subprocess.run(
        [
            "ogr2ogr",
            "-makevalid",
            "-overwrite",
            *["-dim", "XY"],
            *["-lco", "FID=fid"],
            *["-lco", "GEOMETRY_NAME=geom"],
            *["-lco", "LAUNDER=NO"],
            *["-nln", "adm0_polygons"],
            *["-nlt", "PROMOTE_TO_MULTI"],
            *["-t_srs", "EPSG:4326"],
            *["-f", "PostgreSQL", f"PG:dbname={DATABASE}"],
            file,
        ]
    )
    create_attr_table("adm0_polygons", "adm0_attributes")
    logger.info(file.stem)


def admx(_, file: Path):
    subprocess.run(
        [
            "ogr2ogr",
            "-makevalid",
            "-overwrite",
            *["-dim", "XY"],
            *["-lco", "FID=fid"],
            *["-lco", "GEOMETRY_NAME=geom"],
            *["-lco", "LAUNDER=NO"],
            *["-nln", f"admx_{file.stem}"],
            *["-nlt", "PROMOTE_TO_MULTI"],
            *["-t_srs", "EPSG:4326"],
            *["-f", "PostgreSQL", f"PG:dbname={DATABASE}"],
            file,
        ]
    )
    create_attr_table(f"admx_{file.stem}", f"admx_{file.stem}_attributes")
