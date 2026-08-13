"""
CAS (Content Addressable Storage) reconstruction endpoint.

Returns xorb reconstruction information for hf-xet client.
"""

import hashlib
import json

from fastapi import APIRouter, Depends, HTTPException, Response

from kohakuhub.db import User
from kohakuhub.logger import get_logger
from kohakuhub.utils.lakefs import get_lakefs_client, resolve_lakefs_repo
from kohakuhub.utils.s3 import generate_download_presigned_url, parse_s3_uri
from kohakuhub.auth.dependencies import get_optional_user
from kohakuhub.api.xet.utils.file_lookup import (
    check_file_read_permission,
    lookup_file_by_sha256,
)

router = APIRouter()
logger = get_logger("XET_CAS")

# Maximum chunk size: 64MB
# Note: Must be less than u32::MAX (4GB) for Rust client compatibility
CHUNK_SIZE_BYTES = 64 * 1024 * 1024  # 64 MB


@router.get("/reconstructions/{file_id}")
async def get_reconstruction(
    file_id: str,
    user: User | None = Depends(get_optional_user),
):
    """
    Get CAS reconstruction information for a file.

    Args:
        file_id: SHA256 hash of the file

    Returns:
        JSON with single xorb (no deduplication):
        {
          "xorbs": [{
            "url": "s3_presigned_url",
            "size": file_size,
            "sha256": file_id
          }],
          "reconstruction": [{
            "xorb_index": 0,
            "offset": 0,
            "length": file_size
          }]
        }
    """
    # Lookup file by SHA256
    repo, file_record = lookup_file_by_sha256(file_id)

    # Check read permission
    check_file_read_permission(repo, user)

    # Get LakeFS repository name
    lakefs_repo = resolve_lakefs_repo(repo)

    # Get file from LakeFS (use main branch to get latest physical address)
    client = get_lakefs_client()

    try:
        # Use path_in_repo to get the file from LakeFS
        obj_stat = await client.stat_object(
            repository=lakefs_repo,
            ref="main",  # Use main branch as default
            path=file_record.path_in_repo,
        )
    except Exception as e:
        # If file not found on main, it might be on a different branch
        # For now, return 404 - in future we could search all branches
        raise HTTPException(
            status_code=404, detail={"error": f"File not found in repository: {e}"}
        )

    # Parse S3 physical address
    physical_address = obj_stat["physical_address"]
    bucket, key = parse_s3_uri(physical_address)

    # Generate presigned S3 URL (7 days expiration)
    presigned_url = await generate_download_presigned_url(
        bucket=bucket,
        key=key,
        expires_in=604800,  # 7 days
        filename=file_record.path_in_repo.split("/")[-1],
    )

    # Get file size
    file_size = file_record.size
    file_hash = file_record.sha256

    logger.info(
        f"Generating reconstruction for file {file_hash} (size: {file_size} bytes)"
    )

    # Generate chunked reconstruction response
    result = _generate_chunked_reconstruction(file_hash, file_size, presigned_url)

    logger.debug(f"Generated {len(result['terms'])} chunks for file {file_hash}")

    return Response(
        content=json.dumps(result, ensure_ascii=False), media_type="application/json"
    )


def _generate_chunked_reconstruction(
    file_id: str, file_size: int, presigned_url: str
) -> dict:
    """
    Generate QueryReconstructionResponse with chunking support.

    Splits large files into 64MB chunks to avoid u32 overflow issues in the Rust client.
    Each chunk uses the same S3 presigned URL with different byte ranges.

    Args:
        file_id: SHA256 hash of the file
        file_size: Total file size in bytes
        presigned_url: S3 presigned URL for the file

    Returns:
        Dictionary with terms and fetch_info for reconstruction
    """
    # Calculate number of chunks needed
    if file_size == 0:
        # Empty file - single chunk
        num_chunks = 1
        chunks = [(0, 0)]
    else:
        num_chunks = (file_size + CHUNK_SIZE_BYTES - 1) // CHUNK_SIZE_BYTES
        chunks = []
        for i in range(num_chunks):
            start = i * CHUNK_SIZE_BYTES
            end = min(start + CHUNK_SIZE_BYTES, file_size)
            chunks.append((start, end))

    logger.debug(
        f"Splitting {file_size} bytes into {num_chunks} chunks of {CHUNK_SIZE_BYTES} bytes"
    )

    # Build terms and fetch_info
    terms = []
    fetch_info = {}

    for chunk_idx, (chunk_start, chunk_end) in enumerate(chunks):
        chunk_size = chunk_end - chunk_start

        # Generate valid 64-char hex merkle hash for this chunk
        # Use deterministic hash based on file_id + chunk index for consistency
        if num_chunks == 1:
            # Single chunk: use original file hash
            chunk_hash = file_id
        else:
            # Multiple chunks: generate deterministic hash per chunk
            # Hash format: sha256(file_id + chunk_index)
            chunk_data = f"{file_id}-chunk{chunk_idx}".encode("utf-8")
            chunk_hash = hashlib.sha256(chunk_data).hexdigest()

        # Add term for this chunk
        terms.append(
            {
                "hash": chunk_hash,
                "unpacked_length": chunk_size,
                "range": {"start": chunk_idx, "end": chunk_idx + 1},
            }
        )

        # Add fetch_info for this chunk
        # Client will make HTTP Range request to same presigned URL
        fetch_info[chunk_hash] = [
            {
                "range": {"start": chunk_idx, "end": chunk_idx + 1},
                "url": presigned_url,
                "url_range": {
                    "start": chunk_start,
                    "end": (
                        chunk_end - 1 if chunk_size > 0 else 0
                    ),  # HTTP Range is inclusive
                },
            }
        ]

    return {"offset_into_first_range": 0, "terms": terms, "fetch_info": fetch_info}
