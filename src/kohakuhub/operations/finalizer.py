"""Post-LakeFS commit finalization on a caller-owned PostgreSQL transaction."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from psycopg import AsyncConnection


def _owner_id(payload: Mapping[str, Any]) -> int:
    value = payload.get("owner_id")
    if value is None:
        raise ValueError("commit finalization payload is missing owner_id")
    return int(value)


async def finalize_commit_domain(
    connection: AsyncConnection,
    *,
    payload: Mapping[str, Any],
) -> None:
    """Apply Commit and File mutations after LakeFS confirms the commit.

    The caller owns the transaction.  This function deliberately uses the
    psycopg3 connection supplied by the operation service; it must never open
    a second Peewee connection, otherwise the operation ledger could commit
    while the authoritative file view rolls back (or vice versa).
    """

    repository_id = int(payload["repository_id"])
    owner_id = _owner_id(payload)
    commit_id = str(payload["commit_id"])

    await connection.execute(
        """
        INSERT INTO commit
            (commit_id, repository_id, repo_type, branch, author_id, owner_id,
             username, message, description, created_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (commit_id, repository_id) DO UPDATE SET
            repo_type = EXCLUDED.repo_type,
            branch = EXCLUDED.branch,
            author_id = EXCLUDED.author_id,
            owner_id = EXCLUDED.owner_id,
            username = EXCLUDED.username,
            message = EXCLUDED.message,
            description = EXCLUDED.description
        """,
        (
            commit_id,
            repository_id,
            str(payload["repo_type"]),
            str(payload["branch"]),
            int(payload["author_id"]),
            owner_id,
            str(payload["username"]),
            str(payload.get("message", "Commit via API")),
            str(payload.get("description", "")),
        ),
    )

    mutations: Sequence[Mapping[str, Any]] = payload.get("file_mutations", [])
    # ``file`` and the quota counters are the current-main projection.  A
    # feature branch commit is still recorded in ``commit`` but must not
    # overwrite main's file view or billing totals.
    if str(payload["branch"]) == "main":
        # The normal commit preflight rejects duplicate paths, so file
        # mutations can be grouped safely without changing their outcome.  A
        # defensive fallback retains the old ordered behavior for direct
        # callers that supply duplicate paths.
        normalized_mutations = []
        seen_paths: set[str] = set()
        has_duplicate_path = False
        for mutation in mutations:
            path = str(mutation["path"])
            has_duplicate_path = has_duplicate_path or path in seen_paths
            seen_paths.add(path)
            action = mutation.get("action", "upsert")
            if action not in {"delete", "upsert"}:
                raise ValueError(f"unsupported commit file mutation: {action}")
            normalized_mutations.append((path, action, mutation))

        if has_duplicate_path:
            for path, action, mutation in normalized_mutations:
                if action == "delete":
                    await connection.execute(
                        """UPDATE file
                           SET is_deleted = TRUE, updated_at = CURRENT_TIMESTAMP
                           WHERE repository_id = %s AND path_in_repo = %s""",
                        (repository_id, path),
                    )
                else:
                    await connection.execute(
                        """
                        INSERT INTO file
                            (repository_id, path_in_repo, size, sha256, lfs, is_deleted,
                             owner_id, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, FALSE, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                        ON CONFLICT (repository_id, path_in_repo) DO UPDATE SET
                            size = EXCLUDED.size,
                            sha256 = EXCLUDED.sha256,
                            lfs = EXCLUDED.lfs,
                            is_deleted = FALSE,
                            owner_id = EXCLUDED.owner_id,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        (
                            repository_id,
                            path,
                            int(mutation["size"]),
                            str(mutation["sha256"]),
                            bool(mutation["lfs"]),
                            owner_id,
                        ),
                    )
        else:
            delete_paths = [path for path, action, _ in normalized_mutations if action == "delete"]
            upserts = [mutation for _, action, mutation in normalized_mutations if action == "upsert"]
            if delete_paths:
                if len(delete_paths) == 1:
                    await connection.execute(
                        """UPDATE file
                           SET is_deleted = TRUE, updated_at = CURRENT_TIMESTAMP
                           WHERE repository_id = %s AND path_in_repo = %s""",
                        (repository_id, delete_paths[0]),
                    )
                else:
                    await connection.execute(
                        """UPDATE file
                           SET is_deleted = TRUE, updated_at = CURRENT_TIMESTAMP
                           WHERE repository_id = %s AND path_in_repo = ANY(%s)""",
                        (repository_id, delete_paths),
                    )
            if upserts:
                values_sql = ", ".join(
                    "(%s, %s, %s, %s, %s, FALSE, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)"
                    for _ in upserts
                )
                params = []
                for mutation in upserts:
                    params.extend(
                        (
                            repository_id,
                            str(mutation["path"]),
                            int(mutation["size"]),
                            str(mutation["sha256"]),
                            bool(mutation["lfs"]),
                            owner_id,
                        )
                    )
                await connection.execute(
                    f"""
                    INSERT INTO file
                        (repository_id, path_in_repo, size, sha256, lfs, is_deleted,
                         owner_id, created_at, updated_at)
                    VALUES {values_sql}
                    ON CONFLICT (repository_id, path_in_repo) DO UPDATE SET
                        size = EXCLUDED.size,
                        sha256 = EXCLUDED.sha256,
                        lfs = EXCLUDED.lfs,
                        is_deleted = FALSE,
                        owner_id = EXCLUDED.owner_id,
                        updated_at = CURRENT_TIMESTAMP
                    """,
                    tuple(params),
                )

    # Keep the denormalized counters current at the same commit boundary as
    # the authoritative Commit/File rows. Replaying finalization is guarded by
    # the intent state transition, so this delta is applied at most once.
    quota_delta = int(payload.get("quota_delta", 0) or 0)
    if str(payload["branch"]) != "main":
        quota_delta = 0
    if quota_delta:
        is_private = bool(payload.get("is_private", False))
        await connection.execute(
            """UPDATE repository
               SET used_bytes = GREATEST(0, used_bytes + %s)
               WHERE id = %s""",
            (quota_delta, repository_id),
        )
        await connection.execute(
            """UPDATE "user"
               SET private_used_bytes = GREATEST(
                       0, private_used_bytes + CASE WHEN %s THEN %s ELSE 0 END
                   ),
                   public_used_bytes = GREATEST(
                       0, public_used_bytes + CASE WHEN %s THEN 0 ELSE %s END
                   )
               WHERE id = %s""",
            (is_private, quota_delta, is_private, quota_delta, owner_id),
        )


async def mark_commit_intent_committed(
    connection: AsyncConnection,
    *,
    intent_id: Any,
    lakefs_commit_id: str,
    result_json: Mapping[str, Any] | None = None,
) -> None:
    """Record confirmed external success without finalizing business rows."""

    from psycopg.types.json import Jsonb

    await connection.execute(
        """UPDATE khub_commit_intents
           SET state = 'committed', lakefs_commit_id = %s,
               result_json = COALESCE(%s, result_json),
               updated_at = CURRENT_TIMESTAMP, version = version + 1
           WHERE id = %s AND state IN (
               'prepared', 'dispatch_started', 'committed', 'reconciliation_required'
           )""",
        (
            lakefs_commit_id,
            Jsonb(dict(result_json)) if result_json is not None else None,
            intent_id,
        ),
    )
