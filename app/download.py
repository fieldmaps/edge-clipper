import zipfile

import httpx

from .utils import cwd, ignore_list


def download_adm0():
    url = "https://data.fieldmaps.io/adm0/osm/intl/adm0_polygons.gpkg.zip"
    inputs = cwd / "../inputs/adm0"
    zip_file = inputs / "adm0_polygons.gpkg.zip"
    file = inputs / "adm0_polygons.gpkg"
    if not file.exists():
        zip_file.unlink(missing_ok=True)
        with httpx.Client(http2=True) as client:
            with client.stream("GET", url) as r:
                with open(zip_file, "wb") as f:
                    for chunk in r.iter_raw():
                        f.write(chunk)
        with zipfile.ZipFile(zip_file, "r") as z:
            z.extractall(inputs)
        zip_file.unlink()


def download_admx():
    inputs = cwd / "../inputs/admx"
    cods = "https://data.fieldmaps.io/cod.json"
    gb = "https://data.fieldmaps.io/geoboundaries.json"
    results = {}
    for url in [gb, cods]:
        with httpx.Client(http2=True) as client:
            rows = client.get(url).json()
            for row in rows:
                if row["id"] not in ignore_list:
                    results[row["id"]] = row["e_gpkg"]
    for key, value in results.items():
        file = inputs / f"{key.upper()}.gpkg"
        file_lower = inputs / f"{key}.gpkg"
        zip_file = inputs / f"{key}.gpkg.zip"
        if not file.exists():
            zip_file.unlink(missing_ok=True)
            with httpx.Client(http2=True) as client:
                with client.stream("GET", value) as r:
                    with open(zip_file, "wb") as f:
                        for chunk in r.iter_raw():
                            f.write(chunk)
            with zipfile.ZipFile(zip_file, "r") as z:
                z.extractall(inputs)
            file_lower.rename(file)
            zip_file.unlink()


def main():
    download_adm0()
    download_admx()
