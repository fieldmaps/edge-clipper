# Edge Matcher

## Usage

The only requirements are to download [this repository](https://github.com/fieldmaps/edge-matcher/archive/refs/heads/main.zip) and install [Docker Desktop](https://www.docker.com/products/docker-desktop). Add files to the included `inputs` directory, where they'll be processed to the `outputs` directory. Make sure Docker Desktop is running, and from the command line of the repository's root directory, run the following. Note that `docker compose build` is only required for the first run, or if upgrading from a previous version.

```sh
docker compose build
docker compose up
```
