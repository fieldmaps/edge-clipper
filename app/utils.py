import logging
from os import getenv
from pathlib import Path

from dotenv import load_dotenv
from psycopg import connect
from psycopg.rows import dict_row
from psycopg.sql import SQL

load_dotenv(override=True)

cwd = Path(__file__).parent
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

DATABASE = "app"

ignore_list = [
    "geo_1",
    "geo_2",
    "geo_3",
    "ind_1",
    "mar_1",
    "pak_1",
    "srb_1",
    "srb_2",
    "xkx",
    "xss",
]
special_list = ["geo", "srb"]


def strip_list(items: list):
    return [item.strip() for item in items]


ADM_LEVELS = int(getenv("ADM_LEVELS", 4))
ADM_JOIN = getenv("ADM0_JOIN", "")
ADM0_ID = getenv("ADM0_ID", "")
ADM_ID = getenv("ADMX_ID", "")
ADM_COLUMNS = strip_list(getenv("ADMX_COLUMNS", "").split(","))
ADM_METADATA = strip_list(getenv("ADMX_METADATA", "").split(","))
DOWNLOAD = getenv("DOWNLOAD", "").lower() in ("true", "yes", "on", "1")


def get_adm0_file():
    input_dir = cwd / "../inputs/adm0"
    files = list(input_dir.iterdir())
    file = [f for f in files if not f.name.startswith(".")][0]
    return file


def get_admx_files():
    input_dir = cwd / "../inputs/admx"
    files = list(input_dir.iterdir())
    files = [f for f in files if not f.name.startswith(".")]
    return sorted(files)


def get_adm_id(lvl: int):
    return ADM_ID.replace("{x}", str(lvl))


def get_src_ids(lvl=ADM_LEVELS, attr=True):
    ids = []
    for lvl in range(lvl, 0, -1):
        ids_lvl = [col.replace("{x}", str(lvl)) for col in ADM_COLUMNS]
        ids.extend(ids_lvl)
    if attr is True:
        ids.extend(ADM_METADATA)
    return ids


def get_wld_ids():
    conn = connect(f"dbname={DATABASE}", autocommit=True)
    cur = conn.cursor(row_factory=dict_row)
    row = cur.execute(SQL("SELECT * FROM adm0_attributes;")).fetchone()
    colnames = list(row.keys())
    conn.close()
    return colnames
