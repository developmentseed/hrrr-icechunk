import icechunk
import xarray as xr
from hrrr import (
    cache_and_open_virtual_dataset,
    modify_time_encoding,
    sanitize_variables,
)
from hrrrparser import HRRRParser
from hrrrparser.codecs import LEVEL_COORDINATES
from obstore.store import S3Store
from virtualizarr.manifests.store import ObjectStoreRegistry


def initialize():
    parser = HRRRParser(steps=18)

    scheme = "s3://"
    bucket = "noaa-hrrr-bdp-pds"
    prefix = "hrrr.20250710/conus"

    object_store = S3Store(
        bucket=bucket,
        skip_signature=True,
    )
    registry = ObjectStoreRegistry({f"{scheme}{bucket}": object_store})

    url = f"{scheme}{bucket}/{prefix.rstrip('/')}/hrrr.t22z.wrfsfcf16.grib2"
    loadable = LEVEL_COORDINATES + ["time", "step", "latitude", "longitude"]

    vds = cache_and_open_virtual_dataset(
        url=url,
        scheme=scheme,
        bucket=bucket,
        loadable_variables=loadable,
        parser=parser,
        registry=registry,
    )

    sanitize_variables(vds, loadable)
    modify_time_encoding(vds)

    # Icechunk internal chunk store
    s3_chunk_store = icechunk.s3_store(
        region="us-east-1",
    )
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(
            f"{scheme}{bucket}/",
            s3_chunk_store
        )
    )
    credentials = icechunk.containers_credentials(
        {f"{scheme}{bucket}/": icechunk.s3_anonymous_credentials()}
    )

    storage = icechunk.s3_storage(
        bucket="icechunk-hrrr", 
        region="us-east-1",
        prefix="test",
    )

    repo = icechunk.Repository.open_or_create(
        storage=storage,
        config=config,
        authorize_virtual_chunk_access=credentials
        ### needs creds
    )
    session = repo.writable_session("main")

    vds.vz.to_icechunk(session.store, validate_containers=False)

    session.commit("Initial")

    ds = xr.open_zarr(
        session.store,
        group="/",
        zarr_version=3,
        consolidated=False,
    )
    print(ds["tmp_isobar"])


initialize()
