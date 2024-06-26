import logging
import shutil
import subprocess
from pathlib import Path

from .utils import DATABASE, get_adm_id

logger = logging.getLogger(__name__)
cwd = Path(__file__).parent


def output_ogr(file, lvl):
    opts = (
        [
            *["--config", "OGR_ORGANIZE_POLYGONS", "ONLY_CCW"],
            *["-f", "OpenFileGDB"],
            *["-mapFieldType", "Integer64=Real,Date=DateTime"],
            "-unsetFid",
        ]
        if file.suffix == ".gdb"
        else ["-mapFieldType", "DateTime=Date"]
    )
    subprocess.run(
        [
            "ogr2ogr",
            "-makevalid",
            "-overwrite",
            *opts,
            *[
                "-sql",
                f"SELECT * FROM adm{lvl}_polygons ORDER BY {get_adm_id(lvl)};",
            ],
            *["-nln", file.stem],
            *["-nlt", "MultiPolygon"],
            file,
            f"PG:dbname={DATABASE}",
        ]
    )


def main(lvl, ext, _):
    outputs = cwd / "../outputs"
    output = outputs / f"adm{lvl}_polygons.{ext}"
    if output.is_file():
        output.unlink(missing_ok=True)
    elif output.is_dir():
        shutil.rmtree(output, ignore_errors=True)
    output_ogr(output, lvl)
    logger.info(f"adm{lvl}_polygons.{ext}")
