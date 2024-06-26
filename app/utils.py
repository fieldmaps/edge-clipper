import logging
from decimal import Decimal
from os import getenv
from pathlib import Path

from dotenv import load_dotenv
from psycopg import connect
from psycopg.rows import dict_row
from psycopg.sql import SQL, Identifier

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


ADM_LEVELS = int(getenv("ADM_LEVELS", "4"))
ADM0_JOIN = getenv("ADM0_JOIN", "")
ADM0_ID = getenv("ADM0_ID", "")
ADMX_ID = getenv("ADMX_ID", "")
ADMX_COLUMNS = strip_list(getenv("ADMX_COLUMNS", "").split(","))
THROTTLE = Decimal(getenv("THROTTLE", "1"))
DOWNLOAD = getenv("DOWNLOAD", "").lower() in ("true", "yes", "on", "1")


def apply_funcs(file: Path, *args):
    conn = connect(f"dbname={DATABASE}", autocommit=True)
    for func in args:
        func(conn, file)
    conn.close()


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
    return ADMX_ID.replace("{x}", str(lvl))


def get_cols(conn, table: str):
    cur = conn.cursor(row_factory=dict_row)
    row = cur.execute(
        SQL("SELECT * FROM {table};").format(table=Identifier(table))
    ).fetchone()
    return list(row.keys())


def get_adm_cols(lvl: int):
    return [get_adm_id(lvl)] + [col.replace("{x}", str(lvl)) for col in ADMX_COLUMNS]


def get_src_ids(conn, name: str, lvl=ADM_LEVELS):
    cols = get_cols(conn, f"admx_{name}_attributes")
    adm0 = get_adm_cols(0)
    cols = [col for col in cols if col not in adm0]
    for x in range(ADM_LEVELS, lvl, -1):
        admx = get_adm_cols(x)
        cols = [col for col in cols if col not in admx]
    for x in range(1, lvl + 1):
        admx = get_adm_cols(x)
        cols = admx + cols
    cols = list(dict.fromkeys(cols))
    return cols


def get_wld_ids(conn):
    return get_cols(conn, "adm0_attributes")
