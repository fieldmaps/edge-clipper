import logging
import subprocess
from pathlib import Path

from psycopg import connect
from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier

from .utils import DATABASE

logger = logging.getLogger(__name__)

query_1 = """
    ALTER TABLE adm0_polygons
    DROP COLUMN IF EXISTS fid;
"""
query_2 = """
    DROP TABLE IF EXISTS adm0_attributes;
    CREATE TABLE adm0_attributes AS
    SELECT {cols}
    FROM adm0_polygons;
"""


def adm0(file: Path):
    subprocess.run(
        [
            "ogr2ogr",
            "-overwrite",
            "-makevalid",
            *["-dim", "XY"],
            *["-t_srs", "EPSG:4326"],
            *["-lco", "FID=fid"],
            *["-lco", "GEOMETRY_NAME=geom"],
            *["-lco", "LAUNDER=NO"],
            *["-nln", "adm0_polygons"],
            *["-f", "PostgreSQL", f"PG:dbname={DATABASE}"],
            file,
        ]
    )
    conn = connect(f"dbname={DATABASE}", autocommit=True)
    conn.execute(SQL(query_1))
    cur = conn.cursor(row_factory=dict_row)
    row = cur.execute(SQL("SELECT * FROM adm0_polygons;")).fetchone()
    colnames = list(row.keys())
    colnames.remove("geom")
    conn.execute(SQL(query_2).format(cols=SQL(",").join(map(Identifier, colnames))))
    conn.close()
    logger.info(file.stem)


def admx(file: Path):
    subprocess.run(
        [
            "ogr2ogr",
            "-overwrite",
            "-makevalid",
            *["-dim", "XY"],
            *["-t_srs", "EPSG:4326"],
            *["-nlt", "PROMOTE_TO_MULTI"],
            *["-lco", "FID=fid"],
            *["-lco", "GEOMETRY_NAME=geom"],
            *["-lco", "LAUNDER=NO"],
            *["-nln", f"admx_{file.stem}"],
            *["-f", "PostgreSQL", f"PG:dbname={DATABASE}"],
            file,
        ]
    )
    logger.info(file.stem)
