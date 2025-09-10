from datetime import datetime, timedelta, timezone
from typing import cast

import icechunk
import numpy as np
import virtualizarr as vz
import xarray as xr
import zarr
from hrrrparser import HRRRParser
from hrrrparser.codecs import LEVEL_COORDINATES
from icechunk import VirtualChunkSpec
from obstore.store import MemoryStore, S3Store
from virtualizarr.manifests import ManifestArray
from virtualizarr.manifests.store import ObjectStoreRegistry
from zarr.abc.store import Store


def cache_and_open_virtual_dataset(
        url, scheme, bucket, parser, loadable_variables, registry
):
    store, path_in_store = registry.resolve(url)
    memory_store = MemoryStore()
    buffer = store.get(path_in_store).bytes()
    memory_store.put(path_in_store, buffer)
    cached_reg = ObjectStoreRegistry({f"{scheme}{bucket}": memory_store})
    vds = vz.open_virtual_dataset(
        url=url,
        parser=parser,
        registry=cached_reg,
        loadable_variables=loadable_variables,
    )
    return vds


def sanitize_variables(ds, loadable):
    for var in loadable:
        if var in ds:
            del ds[var].encoding["serializer"]
            
    for name, var in ds.variables.items():
        if "reference_date" in var.attrs:
            del var.attrs["reference_date"]
            del var.attrs["forecast_date"]
            del var.attrs["forecast_end_date"]


def modify_time_encoding(vds):
    encoding = vds.time.encoding
    encoding["units"] = "seconds since 1970-01-01"
    encoding["calendar"] = "standard"
    encoding["dtype"] = "int64"
    vds.time.encoding = encoding


def generate_chunk_key(
    index: tuple[int, ...],
    time_index: int,
) -> list[int]:
    index_list = list(index)
    index_list[0] = time_index
    return index_list


def get_time_index(store: Store, time: np.datetime64):
    time_array = zarr.open_array(store, path="time", mode="r")
    epoch = np.datetime64('1970-01-01T00:00:00')
    seconds_since_epoch = (time - epoch) / np.timedelta64(1, 's')
    encoded_time = int(seconds_since_epoch)

    chunk_size = time_array.chunks[0] if time_array.chunks else 1000
    
    for i in range(0, time_array.shape[0], chunk_size):
        end_idx = min(i + chunk_size, time_array.shape[0])
        chunk = time_array[i:end_idx]
        
        # Find encoded value in current chunk
        local_indices = np.where(chunk == encoded_time)[0]
        if len(local_indices) > 0:
            return i + int(local_indices[0])
    
    return None


def extend_time_dimension(store: Store, time: np.datetime64):
    time_array = zarr.open_array(store, path="time", mode="a")
    old_len = time_array.shape[0]
    new_len = old_len + 1
    time_array.resize((new_len,))
    new_index = new_len - 1
    time_array[new_index] = time
    return new_index


def write_virtual_variable_region(
    name: str,
    var: xr.Variable,
    store: Store,
    time_index: int,
    increment_time: bool,
):
    ma = cast(ManifestArray, var.data)
    manifest = ma.manifest

    it = np.nditer(
        [manifest._paths, manifest._offsets, manifest._lengths],  # type: ignore[arg-type]
        flags=[
            "refs_ok",
            "multi_index",
            "c_index",
        ],
        op_flags=[["readonly"]] * 3,  # type: ignore
    )

    if increment_time:
        arr = zarr.open_array(store, path=name, mode="a")
        new_shape = list(arr.shape)
        new_shape[0] += 1
        arr.resize(tuple(new_shape))
        
    last_updated_at = datetime.now(timezone.utc) + timedelta(seconds=1)
    virtual_chunk_spec_list = [
        VirtualChunkSpec(
            index=generate_chunk_key(it.multi_index, time_index=time_index),
            location=path.item(),
            offset=offset.item(),
            length=length.item(),
            last_updated_at_checksum=last_updated_at,
        )
        for path, offset, length in it
        if path
    ]
    
    store.set_virtual_refs(
        array_path=name,
        chunks=virtual_chunk_spec_list,
        validate_containers=False,  # we already validated these before setting any refs
    )


def append_grib(bucket: str, key: str):
    parser = HRRRParser(steps=18)

    scheme = "s3://"
    s3_path = f"{scheme}{bucket}/{key}"

    object_store = S3Store(
        bucket=bucket,
        skip_signature=True,
    )
    registry = ObjectStoreRegistry({f"{scheme}{bucket}": object_store})

    loadable = LEVEL_COORDINATES + ["time", "step", "latitude", "longitude"]

    vds = cache_and_open_virtual_dataset(
        url=s3_path,
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
        from_env=True
    )

    repo = icechunk.Repository.open_or_create(
        storage=storage,
        config=config,
        authorize_virtual_chunk_access=credentials
    )
    session = repo.writable_session("main")

    time_index = get_time_index(store=session.store, time=vds.time[0].values)
    increment_time = False
    if time_index is None:
        time_index = extend_time_dimension(
            store=session.store, time=vds.time[0].values
        )
        increment_time = True

    virtual_variables = {
        name: var
        for name, var in vds.variables.items()
        if isinstance(var.data, ManifestArray)
    }

    for name, var in virtual_variables.items():
        write_virtual_variable_region(
            name=name,
            var=var,
            store=session.store,
            time_index=time_index,
            increment_time=increment_time,
        )

    session.commit(key, rebase_with=icechunk.ConflictDetector())

