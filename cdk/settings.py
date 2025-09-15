import os
from typing import Any, Literal

from pydantic_settings import BaseSettings

print("STAGE from env:", os.getenv("STAGE"))


def include_trailing_slash(value: Any) -> Any:
    """Make sure the value includes a trailing slash if str"""
    if isinstance(value, str):
        return value.rstrip("/") + "/"
    return value


class StackSettings(BaseSettings):
    STACK_NAME: str = "HrrrSns"
    STAGE: Literal["dev", "prod"]
    ACCOUNT_REGION: str = "us-east-1"
    ICECHUNK_BUCKET_NAME: str = "icechunk-hrrr"
    PROJECT: str = "hrrr-icechunk"
