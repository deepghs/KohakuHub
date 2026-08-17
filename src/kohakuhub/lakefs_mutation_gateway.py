"""Approved LakeFS mutation gateway."""

from __future__ import annotations

from typing import Any


async def commit(client: Any, **kwargs: Any) -> Any:
    return await client.commit(**kwargs)


async def upload_object(client: Any, **kwargs: Any) -> Any:
    return await client.upload_object(**kwargs)


async def link_physical_address(client: Any, **kwargs: Any) -> Any:
    return await client.link_physical_address(**kwargs)


async def delete_object(client: Any, **kwargs: Any) -> Any:
    return await client.delete_object(**kwargs)


async def create_repository(client: Any, **kwargs: Any) -> Any:
    return await client.create_repository(**kwargs)


async def delete_repository(client: Any, **kwargs: Any) -> Any:
    return await client.delete_repository(**kwargs)


async def create_branch(client: Any, **kwargs: Any) -> Any:
    return await client.create_branch(**kwargs)


async def delete_branch(client: Any, **kwargs: Any) -> Any:
    return await client.delete_branch(**kwargs)


async def create_tag(client: Any, **kwargs: Any) -> Any:
    return await client.create_tag(**kwargs)


async def delete_tag(client: Any, **kwargs: Any) -> Any:
    return await client.delete_tag(**kwargs)


async def revert_branch(client: Any, **kwargs: Any) -> Any:
    return await client.revert_branch(**kwargs)


async def merge_into_branch(client: Any, **kwargs: Any) -> Any:
    return await client.merge_into_branch(**kwargs)


async def hard_reset_branch(client: Any, **kwargs: Any) -> Any:
    return await client.hard_reset_branch(**kwargs)
