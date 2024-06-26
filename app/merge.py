import logging
import subprocess

from .utils import DATABASE

logger = logging.getLogger(__name__)


def main(lvl: int, _, admx_files: list):
    for file in admx_files:
        name = file.stem
        subprocess.run(
            [
                "ogr2ogr",
                *["--config", "PG_USE_COPY", "YES"],
                "-append",
                *["-f", "PostgreSQL"],
                *["-nln", f"adm{lvl}_polygons"],
                f"PG:dbname={DATABASE}",
                *[f"PG:dbname={DATABASE}", f"adm{lvl}_{name}"],
            ]
        )
    logger.info(f"adm{lvl}_polygons")
