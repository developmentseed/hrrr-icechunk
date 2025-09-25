from datetime import datetime, timedelta, timezone
from typing import Any

import icechunk
from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.utilities.typing import LambdaContext

logger = Logger()
tracer = Tracer()


def cleanup() -> None:
    scheme = "s3://"
    bucket = "noaa-hrrr-bdp-pds"

    s3_chunk_store = icechunk.s3_store(
        region="us-east-1",
    )
    config = icechunk.RepositoryConfig.default()
    config.set_virtual_chunk_container(
        icechunk.VirtualChunkContainer(f"{scheme}{bucket}/", s3_chunk_store)
    )
    credentials = icechunk.containers_credentials(
        {f"{scheme}{bucket}/": icechunk.s3_anonymous_credentials()}
    )

    storage = icechunk.s3_storage(
        bucket="icechunk-hrrr",
        region="us-east-1",
        prefix="test",
        from_env=True,
        network_stream_timeout_seconds=80,
    )

    repo = icechunk.Repository.open_or_create(
        storage=storage,
        config=config,
        authorize_virtual_chunk_access=credentials,
    )

    expiry_time = datetime.now(timezone.utc) - timedelta(days=2)
    repo.expire_snapshots(older_than=expiry_time)
    repo.garbage_collect(expiry_time)


@logger.inject_lambda_context()
@tracer.capture_lambda_handler
def handler(event: Any, context: LambdaContext) -> None:
    try:
        cleanup()
        logger.info("Icechunk initialized")
    except Exception as e:
        logger.error(f"Error in custom resource handler: {e}")
