"""Approved destructive object-storage gateway."""

from __future__ import annotations

from typing import Any

from kohakuhub.utils.s3 import delete_objects_with_prefix


def delete_object(client: Any, **kwargs: Any) -> Any:
    return client.delete_object(**kwargs)


def delete_objects(client: Any, **kwargs: Any) -> Any:
    return client.delete_objects(**kwargs)


async def delete_prefix(bucket: str, prefix: str) -> int:
    return await delete_objects_with_prefix(bucket, prefix)
