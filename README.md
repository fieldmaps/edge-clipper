# Edge Matcher

## Usage

The only requirements are to download [this repository](https://github.com/fieldmaps/edge-matcher/archive/refs/heads/main.zip) and install [Docker Desktop](https://www.docker.com/products/docker-desktop). Add files to the included `inputs` directory, where they'll be processed to the `outputs` directory. Within the `inputs` directory, there are two subdirectories:

- `inputs/adm0`: This is the global admin level 0 file used for clipping.
- `inputs/admx`: Contains all the subnational admin layers in [extended formats](https://github.com/fieldmaps/edge-extender). There can be a mix of different admin levels contained within this directory.

Make sure Docker Desktop is running, and from the command line of the repository's root directory, run the following. Note that `docker compose build` is only required for the first run, or if upgrading from a previous version.

```sh
docker compose build
docker compose up
```

## Configuration

This tool relies on an environment file to configure runtime parameters. Before running this tool, make a copy of the `.env.example` file and rename it to `.env`.

```sh
cp .env.example .env
```

The following options control how the tool runs:

- `ADM_LEVELS`: Configures what admin level to build down to. For example, if set to `4`, it will build out layers for admin levels 1, 2, 3, and 4.
- `ADM0_JOIN`: In the admin level 0 file located at `inputs/adm0`, this is the column used to match polygons used to clip lower level extended files in `inputs/admx`. For example, a column value of `CAN` will specify the polygon used to clip `inputs/admx/CAN.gpkg`.
- `ADM0_ID`: A column containing a unique ID for the admin level 0 layer. This is needed to ensure the ADM0 features are preserved when dissolving, for example from ADM2 to ADM1.
- `ADMX_ID`: A column template for the unique IDs of subnational layers. This is needed to ensure that features are preserved when dissolving, for example from ADM2 to ADM1. Use the placeholder `{x}` to refer to the admin level, for example `adm{x}_id`.
- `ADMX_COLUMNS`: Additional columns specific to each admin level of a layer, comma separated. Use the placeholder `{x}` to refer to the admin level, for example `adm{x}_name`.
- `ADMX_METADATA`: Additional columns for specifying metadata not associated with any particular admin level.
- `THROTTLE`: Subnational boundary processing is done in parallel, running 1 process per CPU core. Doing this may cause Docker to crash if it runs out of RAM by loading too many datasets into memory at the same time. You may see this with errors such as `psycopg.OperationalError: consuming input failed`. Increasing the throttle will decrease the amount of work done in parallel, for example setting the throttle to 1.5 will cause the system to run 1 process per 1.5 CPU cores.
- `DOWNLOAD`: When set to `YES`, the tool downloads data from [FieldMaps](https://fieldmaps.io) that serves as a useful starting point for customizing with external data. Data will be downloaded for [ADM0](https://data.fieldmaps.io/adm0/osm/intl/adm0_polygons.gpkg.zip), as well as downloading all subnational [CODs](https://data.fieldmaps.io/cod.json) and [geoBoundaries](https://data.fieldmaps.io/geoboundaries.json) data. If files with the same name already exist in the `inputs` directory, they will be skipped.
