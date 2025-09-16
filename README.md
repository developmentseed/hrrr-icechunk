# AWS PDS HRRR Virtualizarr Icechunk Pipeline

This repo contains AWS CDK infrastructure for managing real time updates to a
[NOAA HRRR](https://rapidrefresh.noaa.gov/hrrr/) Virtualizarr Icechunk store.

- https://github.com/zarr-developers/VirtualiZarr
- https://github.com/virtual-zarr/hrrr-parser
- https://registry.opendata.aws/noaa-hrrr-pds/

## Getting started

This project uses `uv` to manage dependencies and virtual environments. To install this, please visit the uv
[installation documentation](https://docs.astral.sh/uv/getting-started/installation/) for instructions.

### Development

Install dependencies for resolving references in your favorite IDE:

```plain
uv venv
source .venv/bin/activate
uv sync --all-groups
nodeenv -m .venv -p --node=22.19.0
hash -r
npm install aws-cdk
uv run pre-commit install
```

### Dotenv

This project uses a `.env` ("dotenv") file to help manage application settings for the environments we support (`dev`
and `prod`).

You can use the `env.sample` as a starting point for populating settings.

### Deployment

```plain
uv run --env-file .env.sample -- npx cdk diff
```

or using an environment variable,

```plain
UV_ENV_FILE=.env.dev uv run npx cdk diff
```
