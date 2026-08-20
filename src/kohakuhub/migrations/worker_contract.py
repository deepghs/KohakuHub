"""Stable ledger contracts for the durable worker migration boundary."""

from __future__ import annotations

# Migration 017 records the application schema that existed at its release
# boundary. Later application migrations must add their own ledger records;
# readiness must not recompute this value from the live Peewee models.
APPLICATION_SCHEMA_ADOPTION_CHECKSUM_V1 = (
    "4e38e499aec1d9ccf55323ab3e4fa9ed58d7cd183dbe07d4bd3d8d8b89e663f8"
)

# These values are part of the migration-017 ledger contract. Keep them
# versioned and append new values for later numbered migrations instead of
# deriving an old ledger record from the live worker dependencies.
WORKER_OPERATION_SCHEMA_VERSION_V1 = 8
WORKER_OPERATION_SCHEMA_CHECKSUM_V1 = (
    "3143de9bba6022a7f4372a3be8cb2c8a7bd7c3e2ecb0a1b46ca046652118afd3"
)
WORKER_PROCRASTINATE_SCHEMA_CHECKSUM_V390 = (
    "c70ec4b400a60ad9592787653aae5ae77ae41bf801d56b5bd2712751a07f2009"
)
