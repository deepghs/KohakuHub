"""Immutable schema inputs owned by migration 017.

This module is historical migration data. It intentionally does not import the
current Peewee models, operation schema, or Procrastinate schema at runtime.
Future numbered migrations may extend those definitions without changing the
pre-017 bootstrap or the worker schema installed by 017.
"""

from __future__ import annotations

from typing import Any

PRE_017_POSTGRES_APPLICATION_SCHEMA_SQL: tuple[str, ...] = (
    """CREATE TABLE IF NOT EXISTS "user" ("id" SERIAL NOT NULL PRIMARY KEY, "username" VARCHAR(255) NOT NULL, "normalized_name" VARCHAR(255) NOT NULL, "is_org" BOOLEAN NOT NULL, "email" VARCHAR(255), "password_hash" VARCHAR(255), "email_verified" BOOLEAN NOT NULL, "is_active" BOOLEAN NOT NULL, "private_quota_bytes" BIGINT, "public_quota_bytes" BIGINT, "private_used_bytes" BIGINT NOT NULL, "public_used_bytes" BIGINT NOT NULL, "full_name" VARCHAR(255), "bio" TEXT, "description" TEXT, "website" VARCHAR(255), "social_media" TEXT, "avatar" BYTEA, "avatar_updated_at" TIMESTAMP, "created_at" TIMESTAMP NOT NULL)
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "user_username" ON "user" ("username")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "user_normalized_name" ON "user" ("normalized_name")
    """,
    """CREATE INDEX IF NOT EXISTS "user_is_org" ON "user" ("is_org")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "user_email" ON "user" ("email")
    """,
    """CREATE TABLE IF NOT EXISTS "emailverification" ("id" SERIAL NOT NULL PRIMARY KEY, "user_id" INTEGER NOT NULL, "token" VARCHAR(255) NOT NULL, "expires_at" TIMESTAMP NOT NULL, "created_at" TIMESTAMP NOT NULL, FOREIGN KEY ("user_id") REFERENCES "user" ("id") ON DELETE CASCADE)
    """,
    """CREATE INDEX IF NOT EXISTS "emailverification_user_id" ON "emailverification" ("user_id")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "emailverification_token" ON "emailverification" ("token")
    """,
    """CREATE TABLE IF NOT EXISTS "session" ("id" SERIAL NOT NULL PRIMARY KEY, "session_id" VARCHAR(255) NOT NULL, "user_id" INTEGER NOT NULL, "secret" VARCHAR(255) NOT NULL, "expires_at" TIMESTAMP NOT NULL, "created_at" TIMESTAMP NOT NULL, FOREIGN KEY ("user_id") REFERENCES "user" ("id") ON DELETE CASCADE)
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "session_session_id" ON "session" ("session_id")
    """,
    """CREATE INDEX IF NOT EXISTS "session_user_id" ON "session" ("user_id")
    """,
    """CREATE TABLE IF NOT EXISTS "token" ("id" SERIAL NOT NULL PRIMARY KEY, "user_id" INTEGER NOT NULL, "token_hash" VARCHAR(255) NOT NULL, "name" VARCHAR(255) NOT NULL, "last_used" TIMESTAMP, "created_at" TIMESTAMP NOT NULL, FOREIGN KEY ("user_id") REFERENCES "user" ("id") ON DELETE CASCADE)
    """,
    """CREATE INDEX IF NOT EXISTS "token_user_id" ON "token" ("user_id")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "token_token_hash" ON "token" ("token_hash")
    """,
    """CREATE TABLE IF NOT EXISTS "userexternaltoken" ("id" SERIAL NOT NULL PRIMARY KEY, "user_id" INTEGER NOT NULL, "url" VARCHAR(255) NOT NULL, "encrypted_token" TEXT NOT NULL, "created_at" TIMESTAMP NOT NULL, "updated_at" TIMESTAMP NOT NULL, FOREIGN KEY ("user_id") REFERENCES "user" ("id") ON DELETE CASCADE)
    """,
    """CREATE INDEX IF NOT EXISTS "userexternaltoken_user_id" ON "userexternaltoken" ("user_id")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "userexternaltoken_user_id_url" ON "userexternaltoken" ("user_id", "url")
    """,
    """CREATE TABLE IF NOT EXISTS "repository" ("id" SERIAL NOT NULL PRIMARY KEY, "repo_type" VARCHAR(255) NOT NULL, "namespace" VARCHAR(255) NOT NULL, "name" VARCHAR(255) NOT NULL, "full_id" VARCHAR(255) NOT NULL, "lakefs_repo" VARCHAR(255), "private" BOOLEAN NOT NULL, "owner_id" INTEGER NOT NULL, "quota_bytes" BIGINT, "used_bytes" BIGINT NOT NULL, "lfs_threshold_bytes" INTEGER, "lfs_keep_versions" INTEGER, "lfs_suffix_rules" TEXT, "downloads" INTEGER NOT NULL, "likes_count" INTEGER NOT NULL, "created_at" TIMESTAMP NOT NULL, FOREIGN KEY ("owner_id") REFERENCES "user" ("id") ON DELETE CASCADE)
    """,
    """CREATE INDEX IF NOT EXISTS "repository_repo_type" ON "repository" ("repo_type")
    """,
    """CREATE INDEX IF NOT EXISTS "repository_namespace" ON "repository" ("namespace")
    """,
    """CREATE INDEX IF NOT EXISTS "repository_name" ON "repository" ("name")
    """,
    """CREATE INDEX IF NOT EXISTS "repository_full_id" ON "repository" ("full_id")
    """,
    """CREATE INDEX IF NOT EXISTS "repository_lakefs_repo" ON "repository" ("lakefs_repo")
    """,
    """CREATE INDEX IF NOT EXISTS "repository_owner_id" ON "repository" ("owner_id")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "repository_repo_type_namespace_name" ON "repository" ("repo_type", "namespace", "name")
    """,
    """CREATE TABLE IF NOT EXISTS "file" ("id" SERIAL NOT NULL PRIMARY KEY, "repository_id" INTEGER NOT NULL, "path_in_repo" VARCHAR(255) NOT NULL, "size" BIGINT NOT NULL, "sha256" VARCHAR(255) NOT NULL, "lfs" BOOLEAN NOT NULL, "is_deleted" BOOLEAN NOT NULL, "owner_id" INTEGER NOT NULL, "created_at" TIMESTAMP NOT NULL, "updated_at" TIMESTAMP NOT NULL, FOREIGN KEY ("repository_id") REFERENCES "repository" ("id") ON DELETE CASCADE, FOREIGN KEY ("owner_id") REFERENCES "user" ("id") ON DELETE CASCADE)
    """,
    """CREATE INDEX IF NOT EXISTS "file_repository_id" ON "file" ("repository_id")
    """,
    """CREATE INDEX IF NOT EXISTS "file_path_in_repo" ON "file" ("path_in_repo")
    """,
    """CREATE INDEX IF NOT EXISTS "file_sha256" ON "file" ("sha256")
    """,
    """CREATE INDEX IF NOT EXISTS "file_is_deleted" ON "file" ("is_deleted")
    """,
    """CREATE INDEX IF NOT EXISTS "file_owner_id" ON "file" ("owner_id")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "file_repository_id_path_in_repo" ON "file" ("repository_id", "path_in_repo")
    """,
    """CREATE TABLE IF NOT EXISTS "stagingupload" ("id" SERIAL NOT NULL PRIMARY KEY, "repository_id" INTEGER NOT NULL, "repo_type" VARCHAR(255) NOT NULL, "revision" VARCHAR(255) NOT NULL, "path_in_repo" VARCHAR(255) NOT NULL, "sha256" VARCHAR(255) NOT NULL, "size" BIGINT NOT NULL, "upload_id" VARCHAR(255), "storage_key" VARCHAR(255) NOT NULL, "lfs" BOOLEAN NOT NULL, "uploader_id" INTEGER, "created_at" TIMESTAMP NOT NULL, FOREIGN KEY ("repository_id") REFERENCES "repository" ("id") ON DELETE CASCADE, FOREIGN KEY ("uploader_id") REFERENCES "user" ("id") ON DELETE SET NULL)
    """,
    """CREATE INDEX IF NOT EXISTS "stagingupload_repository_id" ON "stagingupload" ("repository_id")
    """,
    """CREATE INDEX IF NOT EXISTS "stagingupload_repo_type" ON "stagingupload" ("repo_type")
    """,
    """CREATE INDEX IF NOT EXISTS "stagingupload_revision" ON "stagingupload" ("revision")
    """,
    """CREATE INDEX IF NOT EXISTS "stagingupload_uploader_id" ON "stagingupload" ("uploader_id")
    """,
    """CREATE TABLE IF NOT EXISTS "userorganization" ("id" SERIAL NOT NULL PRIMARY KEY, "user_id" INTEGER NOT NULL, "organization_id" INTEGER NOT NULL, "role" VARCHAR(255) NOT NULL, "created_at" TIMESTAMP NOT NULL, FOREIGN KEY ("user_id") REFERENCES "user" ("id") ON DELETE CASCADE, FOREIGN KEY ("organization_id") REFERENCES "user" ("id") ON DELETE CASCADE)
    """,
    """CREATE INDEX IF NOT EXISTS "userorganization_user_id" ON "userorganization" ("user_id")
    """,
    """CREATE INDEX IF NOT EXISTS "userorganization_organization_id" ON "userorganization" ("organization_id")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "userorganization_user_id_organization_id" ON "userorganization" ("user_id", "organization_id")
    """,
    """CREATE TABLE IF NOT EXISTS "commit" ("id" SERIAL NOT NULL PRIMARY KEY, "commit_id" VARCHAR(255) NOT NULL, "repository_id" INTEGER NOT NULL, "repo_type" VARCHAR(255) NOT NULL, "branch" VARCHAR(255) NOT NULL, "author_id" INTEGER NOT NULL, "owner_id" INTEGER NOT NULL, "username" VARCHAR(255) NOT NULL, "message" TEXT NOT NULL, "description" TEXT NOT NULL, "created_at" TIMESTAMP NOT NULL, FOREIGN KEY ("repository_id") REFERENCES "repository" ("id") ON DELETE CASCADE, FOREIGN KEY ("author_id") REFERENCES "user" ("id") ON DELETE CASCADE, FOREIGN KEY ("owner_id") REFERENCES "user" ("id") ON DELETE CASCADE)
    """,
    """CREATE INDEX IF NOT EXISTS "commit_commit_id" ON "commit" ("commit_id")
    """,
    """CREATE INDEX IF NOT EXISTS "commit_repository_id" ON "commit" ("repository_id")
    """,
    """CREATE INDEX IF NOT EXISTS "commit_repo_type" ON "commit" ("repo_type")
    """,
    """CREATE INDEX IF NOT EXISTS "commit_branch" ON "commit" ("branch")
    """,
    """CREATE INDEX IF NOT EXISTS "commit_author_id" ON "commit" ("author_id")
    """,
    """CREATE INDEX IF NOT EXISTS "commit_owner_id" ON "commit" ("owner_id")
    """,
    """CREATE INDEX IF NOT EXISTS "commit_username" ON "commit" ("username")
    """,
    """CREATE INDEX IF NOT EXISTS "commit_repository_id_branch" ON "commit" ("repository_id", "branch")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "commit_commit_id_repository_id" ON "commit" ("commit_id", "repository_id")
    """,
    """CREATE TABLE IF NOT EXISTS "lfsobjecthistory" ("id" SERIAL NOT NULL PRIMARY KEY, "repository_id" INTEGER NOT NULL, "path_in_repo" VARCHAR(255) NOT NULL, "sha256" VARCHAR(255) NOT NULL, "size" BIGINT NOT NULL, "commit_id" VARCHAR(255) NOT NULL, "file_id" INTEGER, "created_at" TIMESTAMP NOT NULL, FOREIGN KEY ("repository_id") REFERENCES "repository" ("id") ON DELETE CASCADE, FOREIGN KEY ("file_id") REFERENCES "file" ("id") ON DELETE SET NULL)
    """,
    """CREATE INDEX IF NOT EXISTS "lfsobjecthistory_repository_id" ON "lfsobjecthistory" ("repository_id")
    """,
    """CREATE INDEX IF NOT EXISTS "lfsobjecthistory_path_in_repo" ON "lfsobjecthistory" ("path_in_repo")
    """,
    """CREATE INDEX IF NOT EXISTS "lfsobjecthistory_sha256" ON "lfsobjecthistory" ("sha256")
    """,
    """CREATE INDEX IF NOT EXISTS "lfsobjecthistory_commit_id" ON "lfsobjecthistory" ("commit_id")
    """,
    """CREATE INDEX IF NOT EXISTS "lfsobjecthistory_file_id" ON "lfsobjecthistory" ("file_id")
    """,
    """CREATE INDEX IF NOT EXISTS "lfsobjecthistory_repository_id_path_in_repo" ON "lfsobjecthistory" ("repository_id", "path_in_repo")
    """,
    """CREATE INDEX IF NOT EXISTS "lfsobjecthistory_sha256" ON "lfsobjecthistory" ("sha256")
    """,
    """CREATE TABLE IF NOT EXISTS "sshkey" ("id" SERIAL NOT NULL PRIMARY KEY, "user_id" INTEGER NOT NULL, "key_type" VARCHAR(255) NOT NULL, "public_key" TEXT NOT NULL, "fingerprint" VARCHAR(255) NOT NULL, "title" VARCHAR(255) NOT NULL, "last_used" TIMESTAMP, "created_at" TIMESTAMP NOT NULL, FOREIGN KEY ("user_id") REFERENCES "user" ("id") ON DELETE CASCADE)
    """,
    """CREATE INDEX IF NOT EXISTS "sshkey_user_id" ON "sshkey" ("user_id")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "sshkey_fingerprint" ON "sshkey" ("fingerprint")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "sshkey_user_id_fingerprint" ON "sshkey" ("user_id", "fingerprint")
    """,
    """CREATE TABLE IF NOT EXISTS "invitation" ("id" SERIAL NOT NULL PRIMARY KEY, "token" VARCHAR(255) NOT NULL, "action" VARCHAR(255) NOT NULL, "parameters" TEXT NOT NULL, "created_by_id" INTEGER, "expires_at" TIMESTAMP NOT NULL, "max_usage" INTEGER, "usage_count" INTEGER NOT NULL, "used_at" TIMESTAMP, "used_by_id" INTEGER, "created_at" TIMESTAMP NOT NULL, FOREIGN KEY ("created_by_id") REFERENCES "user" ("id") ON DELETE CASCADE, FOREIGN KEY ("used_by_id") REFERENCES "user" ("id") ON DELETE SET NULL)
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "invitation_token" ON "invitation" ("token")
    """,
    """CREATE INDEX IF NOT EXISTS "invitation_action" ON "invitation" ("action")
    """,
    """CREATE INDEX IF NOT EXISTS "invitation_created_by_id" ON "invitation" ("created_by_id")
    """,
    """CREATE INDEX IF NOT EXISTS "invitation_used_by_id" ON "invitation" ("used_by_id")
    """,
    """CREATE INDEX IF NOT EXISTS "invitation_action_created_by_id" ON "invitation" ("action", "created_by_id")
    """,
    """CREATE TABLE IF NOT EXISTS "repositorylike" ("id" SERIAL NOT NULL PRIMARY KEY, "repository_id" INTEGER NOT NULL, "user_id" INTEGER NOT NULL, "created_at" TIMESTAMP NOT NULL, FOREIGN KEY ("repository_id") REFERENCES "repository" ("id") ON DELETE CASCADE, FOREIGN KEY ("user_id") REFERENCES "user" ("id") ON DELETE CASCADE)
    """,
    """CREATE INDEX IF NOT EXISTS "repositorylike_repository_id" ON "repositorylike" ("repository_id")
    """,
    """CREATE INDEX IF NOT EXISTS "repositorylike_user_id" ON "repositorylike" ("user_id")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "repositorylike_repository_id_user_id" ON "repositorylike" ("repository_id", "user_id")
    """,
    """CREATE TABLE IF NOT EXISTS "downloadsession" ("id" SERIAL NOT NULL PRIMARY KEY, "repository_id" INTEGER NOT NULL, "user_id" INTEGER, "session_id" VARCHAR(255) NOT NULL, "time_bucket" INTEGER NOT NULL, "file_count" INTEGER NOT NULL, "first_file" VARCHAR(255) NOT NULL, "first_download_at" TIMESTAMP NOT NULL, "last_download_at" TIMESTAMP NOT NULL, FOREIGN KEY ("repository_id") REFERENCES "repository" ("id") ON DELETE CASCADE, FOREIGN KEY ("user_id") REFERENCES "user" ("id") ON DELETE SET NULL)
    """,
    """CREATE INDEX IF NOT EXISTS "downloadsession_repository_id" ON "downloadsession" ("repository_id")
    """,
    """CREATE INDEX IF NOT EXISTS "downloadsession_user_id" ON "downloadsession" ("user_id")
    """,
    """CREATE INDEX IF NOT EXISTS "downloadsession_session_id" ON "downloadsession" ("session_id")
    """,
    """CREATE INDEX IF NOT EXISTS "downloadsession_time_bucket" ON "downloadsession" ("time_bucket")
    """,
    """CREATE INDEX IF NOT EXISTS "downloadsession_first_download_at" ON "downloadsession" ("first_download_at")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "downloadsession_repository_id_session_id_time_bucket" ON "downloadsession" ("repository_id", "session_id", "time_bucket")
    """,
    """CREATE TABLE IF NOT EXISTS "dailyrepostats" ("id" SERIAL NOT NULL PRIMARY KEY, "repository_id" INTEGER NOT NULL, "date" DATE NOT NULL, "download_sessions" INTEGER NOT NULL, "authenticated_downloads" INTEGER NOT NULL, "anonymous_downloads" INTEGER NOT NULL, "total_files" INTEGER NOT NULL, "created_at" TIMESTAMP NOT NULL, FOREIGN KEY ("repository_id") REFERENCES "repository" ("id") ON DELETE CASCADE)
    """,
    """CREATE INDEX IF NOT EXISTS "dailyrepostats_repository_id" ON "dailyrepostats" ("repository_id")
    """,
    """CREATE INDEX IF NOT EXISTS "dailyrepostats_date" ON "dailyrepostats" ("date")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "dailyrepostats_repository_id_date" ON "dailyrepostats" ("repository_id", "date")
    """,
    """CREATE TABLE IF NOT EXISTS "fallbacksource" ("id" SERIAL NOT NULL PRIMARY KEY, "namespace" VARCHAR(255) NOT NULL, "url" VARCHAR(255) NOT NULL, "token" VARCHAR(255), "priority" INTEGER NOT NULL, "name" VARCHAR(255) NOT NULL, "source_type" VARCHAR(255) NOT NULL, "enabled" BOOLEAN NOT NULL, "created_at" TIMESTAMP NOT NULL, "updated_at" TIMESTAMP NOT NULL)
    """,
    """CREATE INDEX IF NOT EXISTS "fallbacksource_namespace" ON "fallbacksource" ("namespace")
    """,
    """CREATE INDEX IF NOT EXISTS "fallbacksource_priority" ON "fallbacksource" ("priority")
    """,
    """CREATE INDEX IF NOT EXISTS "fallbacksource_enabled" ON "fallbacksource" ("enabled")
    """,
    """CREATE INDEX IF NOT EXISTS "fallbacksource_namespace_priority" ON "fallbacksource" ("namespace", "priority")
    """,
    """CREATE INDEX IF NOT EXISTS "fallbacksource_enabled_priority" ON "fallbacksource" ("enabled", "priority")
    """,
    """CREATE TABLE IF NOT EXISTS "confirmationtoken" ("id" SERIAL NOT NULL PRIMARY KEY, "token" VARCHAR(255) NOT NULL, "action_type" VARCHAR(255) NOT NULL, "action_data" TEXT NOT NULL, "created_at" TIMESTAMP NOT NULL, "expires_at" TIMESTAMP NOT NULL)
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "confirmationtoken_token" ON "confirmationtoken" ("token")
    """,
    """CREATE INDEX IF NOT EXISTS "confirmationtoken_action_type" ON "confirmationtoken" ("action_type")
    """,
    """CREATE INDEX IF NOT EXISTS "confirmationtoken_expires_at" ON "confirmationtoken" ("expires_at")
    """,
    """CREATE INDEX IF NOT EXISTS "confirmationtoken_action_type_expires_at" ON "confirmationtoken" ("action_type", "expires_at")
    """,
)

PRE_017_SQLITE_APPLICATION_SCHEMA_SQL: tuple[str, ...] = (
    """CREATE TABLE IF NOT EXISTS "user" ("id" INTEGER NOT NULL PRIMARY KEY, "username" VARCHAR(255) NOT NULL, "normalized_name" VARCHAR(255) NOT NULL, "is_org" INTEGER NOT NULL, "email" VARCHAR(255), "password_hash" VARCHAR(255), "email_verified" INTEGER NOT NULL, "is_active" INTEGER NOT NULL, "private_quota_bytes" INTEGER, "public_quota_bytes" INTEGER, "private_used_bytes" INTEGER NOT NULL, "public_used_bytes" INTEGER NOT NULL, "full_name" VARCHAR(255), "bio" TEXT, "description" TEXT, "website" VARCHAR(255), "social_media" TEXT, "avatar" BLOB, "avatar_updated_at" DATETIME, "created_at" DATETIME NOT NULL)
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "user_username" ON "user" ("username")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "user_normalized_name" ON "user" ("normalized_name")
    """,
    """CREATE INDEX IF NOT EXISTS "user_is_org" ON "user" ("is_org")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "user_email" ON "user" ("email")
    """,
    """CREATE TABLE IF NOT EXISTS "emailverification" ("id" INTEGER NOT NULL PRIMARY KEY, "user_id" INTEGER NOT NULL, "token" VARCHAR(255) NOT NULL, "expires_at" DATETIME NOT NULL, "created_at" DATETIME NOT NULL, FOREIGN KEY ("user_id") REFERENCES "user" ("id") ON DELETE CASCADE)
    """,
    """CREATE INDEX IF NOT EXISTS "emailverification_user_id" ON "emailverification" ("user_id")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "emailverification_token" ON "emailverification" ("token")
    """,
    """CREATE TABLE IF NOT EXISTS "session" ("id" INTEGER NOT NULL PRIMARY KEY, "session_id" VARCHAR(255) NOT NULL, "user_id" INTEGER NOT NULL, "secret" VARCHAR(255) NOT NULL, "expires_at" DATETIME NOT NULL, "created_at" DATETIME NOT NULL, FOREIGN KEY ("user_id") REFERENCES "user" ("id") ON DELETE CASCADE)
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "session_session_id" ON "session" ("session_id")
    """,
    """CREATE INDEX IF NOT EXISTS "session_user_id" ON "session" ("user_id")
    """,
    """CREATE TABLE IF NOT EXISTS "token" ("id" INTEGER NOT NULL PRIMARY KEY, "user_id" INTEGER NOT NULL, "token_hash" VARCHAR(255) NOT NULL, "name" VARCHAR(255) NOT NULL, "last_used" DATETIME, "created_at" DATETIME NOT NULL, FOREIGN KEY ("user_id") REFERENCES "user" ("id") ON DELETE CASCADE)
    """,
    """CREATE INDEX IF NOT EXISTS "token_user_id" ON "token" ("user_id")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "token_token_hash" ON "token" ("token_hash")
    """,
    """CREATE TABLE IF NOT EXISTS "userexternaltoken" ("id" INTEGER NOT NULL PRIMARY KEY, "user_id" INTEGER NOT NULL, "url" VARCHAR(255) NOT NULL, "encrypted_token" TEXT NOT NULL, "created_at" DATETIME NOT NULL, "updated_at" DATETIME NOT NULL, FOREIGN KEY ("user_id") REFERENCES "user" ("id") ON DELETE CASCADE)
    """,
    """CREATE INDEX IF NOT EXISTS "userexternaltoken_user_id" ON "userexternaltoken" ("user_id")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "userexternaltoken_user_id_url" ON "userexternaltoken" ("user_id", "url")
    """,
    """CREATE TABLE IF NOT EXISTS "repository" ("id" INTEGER NOT NULL PRIMARY KEY, "repo_type" VARCHAR(255) NOT NULL, "namespace" VARCHAR(255) NOT NULL, "name" VARCHAR(255) NOT NULL, "full_id" VARCHAR(255) NOT NULL, "lakefs_repo" VARCHAR(255), "private" INTEGER NOT NULL, "owner_id" INTEGER NOT NULL, "quota_bytes" INTEGER, "used_bytes" INTEGER NOT NULL, "lfs_threshold_bytes" INTEGER, "lfs_keep_versions" INTEGER, "lfs_suffix_rules" TEXT, "downloads" INTEGER NOT NULL, "likes_count" INTEGER NOT NULL, "created_at" DATETIME NOT NULL, FOREIGN KEY ("owner_id") REFERENCES "user" ("id") ON DELETE CASCADE)
    """,
    """CREATE INDEX IF NOT EXISTS "repository_repo_type" ON "repository" ("repo_type")
    """,
    """CREATE INDEX IF NOT EXISTS "repository_namespace" ON "repository" ("namespace")
    """,
    """CREATE INDEX IF NOT EXISTS "repository_name" ON "repository" ("name")
    """,
    """CREATE INDEX IF NOT EXISTS "repository_full_id" ON "repository" ("full_id")
    """,
    """CREATE INDEX IF NOT EXISTS "repository_lakefs_repo" ON "repository" ("lakefs_repo")
    """,
    """CREATE INDEX IF NOT EXISTS "repository_owner_id" ON "repository" ("owner_id")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "repository_repo_type_namespace_name" ON "repository" ("repo_type", "namespace", "name")
    """,
    """CREATE TABLE IF NOT EXISTS "file" ("id" INTEGER NOT NULL PRIMARY KEY, "repository_id" INTEGER NOT NULL, "path_in_repo" VARCHAR(255) NOT NULL, "size" INTEGER NOT NULL, "sha256" VARCHAR(255) NOT NULL, "lfs" INTEGER NOT NULL, "is_deleted" INTEGER NOT NULL, "owner_id" INTEGER NOT NULL, "created_at" DATETIME NOT NULL, "updated_at" DATETIME NOT NULL, FOREIGN KEY ("repository_id") REFERENCES "repository" ("id") ON DELETE CASCADE, FOREIGN KEY ("owner_id") REFERENCES "user" ("id") ON DELETE CASCADE)
    """,
    """CREATE INDEX IF NOT EXISTS "file_repository_id" ON "file" ("repository_id")
    """,
    """CREATE INDEX IF NOT EXISTS "file_path_in_repo" ON "file" ("path_in_repo")
    """,
    """CREATE INDEX IF NOT EXISTS "file_sha256" ON "file" ("sha256")
    """,
    """CREATE INDEX IF NOT EXISTS "file_is_deleted" ON "file" ("is_deleted")
    """,
    """CREATE INDEX IF NOT EXISTS "file_owner_id" ON "file" ("owner_id")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "file_repository_id_path_in_repo" ON "file" ("repository_id", "path_in_repo")
    """,
    """CREATE TABLE IF NOT EXISTS "stagingupload" ("id" INTEGER NOT NULL PRIMARY KEY, "repository_id" INTEGER NOT NULL, "repo_type" VARCHAR(255) NOT NULL, "revision" VARCHAR(255) NOT NULL, "path_in_repo" VARCHAR(255) NOT NULL, "sha256" VARCHAR(255) NOT NULL, "size" INTEGER NOT NULL, "upload_id" VARCHAR(255), "storage_key" VARCHAR(255) NOT NULL, "lfs" INTEGER NOT NULL, "uploader_id" INTEGER, "created_at" DATETIME NOT NULL, FOREIGN KEY ("repository_id") REFERENCES "repository" ("id") ON DELETE CASCADE, FOREIGN KEY ("uploader_id") REFERENCES "user" ("id") ON DELETE SET NULL)
    """,
    """CREATE INDEX IF NOT EXISTS "stagingupload_repository_id" ON "stagingupload" ("repository_id")
    """,
    """CREATE INDEX IF NOT EXISTS "stagingupload_repo_type" ON "stagingupload" ("repo_type")
    """,
    """CREATE INDEX IF NOT EXISTS "stagingupload_revision" ON "stagingupload" ("revision")
    """,
    """CREATE INDEX IF NOT EXISTS "stagingupload_uploader_id" ON "stagingupload" ("uploader_id")
    """,
    """CREATE TABLE IF NOT EXISTS "userorganization" ("id" INTEGER NOT NULL PRIMARY KEY, "user_id" INTEGER NOT NULL, "organization_id" INTEGER NOT NULL, "role" VARCHAR(255) NOT NULL, "created_at" DATETIME NOT NULL, FOREIGN KEY ("user_id") REFERENCES "user" ("id") ON DELETE CASCADE, FOREIGN KEY ("organization_id") REFERENCES "user" ("id") ON DELETE CASCADE)
    """,
    """CREATE INDEX IF NOT EXISTS "userorganization_user_id" ON "userorganization" ("user_id")
    """,
    """CREATE INDEX IF NOT EXISTS "userorganization_organization_id" ON "userorganization" ("organization_id")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "userorganization_user_id_organization_id" ON "userorganization" ("user_id", "organization_id")
    """,
    """CREATE TABLE IF NOT EXISTS "commit" ("id" INTEGER NOT NULL PRIMARY KEY, "commit_id" VARCHAR(255) NOT NULL, "repository_id" INTEGER NOT NULL, "repo_type" VARCHAR(255) NOT NULL, "branch" VARCHAR(255) NOT NULL, "author_id" INTEGER NOT NULL, "owner_id" INTEGER NOT NULL, "username" VARCHAR(255) NOT NULL, "message" TEXT NOT NULL, "description" TEXT NOT NULL, "created_at" DATETIME NOT NULL, FOREIGN KEY ("repository_id") REFERENCES "repository" ("id") ON DELETE CASCADE, FOREIGN KEY ("author_id") REFERENCES "user" ("id") ON DELETE CASCADE, FOREIGN KEY ("owner_id") REFERENCES "user" ("id") ON DELETE CASCADE)
    """,
    """CREATE INDEX IF NOT EXISTS "commit_commit_id" ON "commit" ("commit_id")
    """,
    """CREATE INDEX IF NOT EXISTS "commit_repository_id" ON "commit" ("repository_id")
    """,
    """CREATE INDEX IF NOT EXISTS "commit_repo_type" ON "commit" ("repo_type")
    """,
    """CREATE INDEX IF NOT EXISTS "commit_branch" ON "commit" ("branch")
    """,
    """CREATE INDEX IF NOT EXISTS "commit_author_id" ON "commit" ("author_id")
    """,
    """CREATE INDEX IF NOT EXISTS "commit_owner_id" ON "commit" ("owner_id")
    """,
    """CREATE INDEX IF NOT EXISTS "commit_username" ON "commit" ("username")
    """,
    """CREATE INDEX IF NOT EXISTS "commit_repository_id_branch" ON "commit" ("repository_id", "branch")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "commit_commit_id_repository_id" ON "commit" ("commit_id", "repository_id")
    """,
    """CREATE TABLE IF NOT EXISTS "lfsobjecthistory" ("id" INTEGER NOT NULL PRIMARY KEY, "repository_id" INTEGER NOT NULL, "path_in_repo" VARCHAR(255) NOT NULL, "sha256" VARCHAR(255) NOT NULL, "size" INTEGER NOT NULL, "commit_id" VARCHAR(255) NOT NULL, "file_id" INTEGER, "created_at" DATETIME NOT NULL, FOREIGN KEY ("repository_id") REFERENCES "repository" ("id") ON DELETE CASCADE, FOREIGN KEY ("file_id") REFERENCES "file" ("id") ON DELETE SET NULL)
    """,
    """CREATE INDEX IF NOT EXISTS "lfsobjecthistory_repository_id" ON "lfsobjecthistory" ("repository_id")
    """,
    """CREATE INDEX IF NOT EXISTS "lfsobjecthistory_path_in_repo" ON "lfsobjecthistory" ("path_in_repo")
    """,
    """CREATE INDEX IF NOT EXISTS "lfsobjecthistory_sha256" ON "lfsobjecthistory" ("sha256")
    """,
    """CREATE INDEX IF NOT EXISTS "lfsobjecthistory_commit_id" ON "lfsobjecthistory" ("commit_id")
    """,
    """CREATE INDEX IF NOT EXISTS "lfsobjecthistory_file_id" ON "lfsobjecthistory" ("file_id")
    """,
    """CREATE INDEX IF NOT EXISTS "lfsobjecthistory_repository_id_path_in_repo" ON "lfsobjecthistory" ("repository_id", "path_in_repo")
    """,
    """CREATE INDEX IF NOT EXISTS "lfsobjecthistory_sha256" ON "lfsobjecthistory" ("sha256")
    """,
    """CREATE TABLE IF NOT EXISTS "sshkey" ("id" INTEGER NOT NULL PRIMARY KEY, "user_id" INTEGER NOT NULL, "key_type" VARCHAR(255) NOT NULL, "public_key" TEXT NOT NULL, "fingerprint" VARCHAR(255) NOT NULL, "title" VARCHAR(255) NOT NULL, "last_used" DATETIME, "created_at" DATETIME NOT NULL, FOREIGN KEY ("user_id") REFERENCES "user" ("id") ON DELETE CASCADE)
    """,
    """CREATE INDEX IF NOT EXISTS "sshkey_user_id" ON "sshkey" ("user_id")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "sshkey_fingerprint" ON "sshkey" ("fingerprint")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "sshkey_user_id_fingerprint" ON "sshkey" ("user_id", "fingerprint")
    """,
    """CREATE TABLE IF NOT EXISTS "invitation" ("id" INTEGER NOT NULL PRIMARY KEY, "token" VARCHAR(255) NOT NULL, "action" VARCHAR(255) NOT NULL, "parameters" TEXT NOT NULL, "created_by_id" INTEGER, "expires_at" DATETIME NOT NULL, "max_usage" INTEGER, "usage_count" INTEGER NOT NULL, "used_at" DATETIME, "used_by_id" INTEGER, "created_at" DATETIME NOT NULL, FOREIGN KEY ("created_by_id") REFERENCES "user" ("id") ON DELETE CASCADE, FOREIGN KEY ("used_by_id") REFERENCES "user" ("id") ON DELETE SET NULL)
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "invitation_token" ON "invitation" ("token")
    """,
    """CREATE INDEX IF NOT EXISTS "invitation_action" ON "invitation" ("action")
    """,
    """CREATE INDEX IF NOT EXISTS "invitation_created_by_id" ON "invitation" ("created_by_id")
    """,
    """CREATE INDEX IF NOT EXISTS "invitation_used_by_id" ON "invitation" ("used_by_id")
    """,
    """CREATE INDEX IF NOT EXISTS "invitation_action_created_by_id" ON "invitation" ("action", "created_by_id")
    """,
    """CREATE TABLE IF NOT EXISTS "repositorylike" ("id" INTEGER NOT NULL PRIMARY KEY, "repository_id" INTEGER NOT NULL, "user_id" INTEGER NOT NULL, "created_at" DATETIME NOT NULL, FOREIGN KEY ("repository_id") REFERENCES "repository" ("id") ON DELETE CASCADE, FOREIGN KEY ("user_id") REFERENCES "user" ("id") ON DELETE CASCADE)
    """,
    """CREATE INDEX IF NOT EXISTS "repositorylike_repository_id" ON "repositorylike" ("repository_id")
    """,
    """CREATE INDEX IF NOT EXISTS "repositorylike_user_id" ON "repositorylike" ("user_id")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "repositorylike_repository_id_user_id" ON "repositorylike" ("repository_id", "user_id")
    """,
    """CREATE TABLE IF NOT EXISTS "downloadsession" ("id" INTEGER NOT NULL PRIMARY KEY, "repository_id" INTEGER NOT NULL, "user_id" INTEGER, "session_id" VARCHAR(255) NOT NULL, "time_bucket" INTEGER NOT NULL, "file_count" INTEGER NOT NULL, "first_file" VARCHAR(255) NOT NULL, "first_download_at" DATETIME NOT NULL, "last_download_at" DATETIME NOT NULL, FOREIGN KEY ("repository_id") REFERENCES "repository" ("id") ON DELETE CASCADE, FOREIGN KEY ("user_id") REFERENCES "user" ("id") ON DELETE SET NULL)
    """,
    """CREATE INDEX IF NOT EXISTS "downloadsession_repository_id" ON "downloadsession" ("repository_id")
    """,
    """CREATE INDEX IF NOT EXISTS "downloadsession_user_id" ON "downloadsession" ("user_id")
    """,
    """CREATE INDEX IF NOT EXISTS "downloadsession_session_id" ON "downloadsession" ("session_id")
    """,
    """CREATE INDEX IF NOT EXISTS "downloadsession_time_bucket" ON "downloadsession" ("time_bucket")
    """,
    """CREATE INDEX IF NOT EXISTS "downloadsession_first_download_at" ON "downloadsession" ("first_download_at")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "downloadsession_repository_id_session_id_time_bucket" ON "downloadsession" ("repository_id", "session_id", "time_bucket")
    """,
    """CREATE TABLE IF NOT EXISTS "dailyrepostats" ("id" INTEGER NOT NULL PRIMARY KEY, "repository_id" INTEGER NOT NULL, "date" DATE NOT NULL, "download_sessions" INTEGER NOT NULL, "authenticated_downloads" INTEGER NOT NULL, "anonymous_downloads" INTEGER NOT NULL, "total_files" INTEGER NOT NULL, "created_at" DATETIME NOT NULL, FOREIGN KEY ("repository_id") REFERENCES "repository" ("id") ON DELETE CASCADE)
    """,
    """CREATE INDEX IF NOT EXISTS "dailyrepostats_repository_id" ON "dailyrepostats" ("repository_id")
    """,
    """CREATE INDEX IF NOT EXISTS "dailyrepostats_date" ON "dailyrepostats" ("date")
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "dailyrepostats_repository_id_date" ON "dailyrepostats" ("repository_id", "date")
    """,
    """CREATE TABLE IF NOT EXISTS "fallbacksource" ("id" INTEGER NOT NULL PRIMARY KEY, "namespace" VARCHAR(255) NOT NULL, "url" VARCHAR(255) NOT NULL, "token" VARCHAR(255), "priority" INTEGER NOT NULL, "name" VARCHAR(255) NOT NULL, "source_type" VARCHAR(255) NOT NULL, "enabled" INTEGER NOT NULL, "created_at" DATETIME NOT NULL, "updated_at" DATETIME NOT NULL)
    """,
    """CREATE INDEX IF NOT EXISTS "fallbacksource_namespace" ON "fallbacksource" ("namespace")
    """,
    """CREATE INDEX IF NOT EXISTS "fallbacksource_priority" ON "fallbacksource" ("priority")
    """,
    """CREATE INDEX IF NOT EXISTS "fallbacksource_enabled" ON "fallbacksource" ("enabled")
    """,
    """CREATE INDEX IF NOT EXISTS "fallbacksource_namespace_priority" ON "fallbacksource" ("namespace", "priority")
    """,
    """CREATE INDEX IF NOT EXISTS "fallbacksource_enabled_priority" ON "fallbacksource" ("enabled", "priority")
    """,
    """CREATE TABLE IF NOT EXISTS "confirmationtoken" ("id" INTEGER NOT NULL PRIMARY KEY, "token" VARCHAR(255) NOT NULL, "action_type" VARCHAR(255) NOT NULL, "action_data" TEXT NOT NULL, "created_at" DATETIME NOT NULL, "expires_at" DATETIME NOT NULL)
    """,
    """CREATE UNIQUE INDEX IF NOT EXISTS "confirmationtoken_token" ON "confirmationtoken" ("token")
    """,
    """CREATE INDEX IF NOT EXISTS "confirmationtoken_action_type" ON "confirmationtoken" ("action_type")
    """,
    """CREATE INDEX IF NOT EXISTS "confirmationtoken_expires_at" ON "confirmationtoken" ("expires_at")
    """,
    """CREATE INDEX IF NOT EXISTS "confirmationtoken_action_type_expires_at" ON "confirmationtoken" ("action_type", "expires_at")
    """,
)

# These historical operation contracts are copied from the released durable
# worker schema. Do not import them from the current operation models: a later
# numbered migration is allowed to change those models.
HISTORICAL_OPERATION_TABLE_COLUMNS_V2 = {
    "khub_repository_operations": (
        "cancel_requested_at",
        "created_at",
        "dispatch_started_at",
        "error_code",
        "error_summary",
        "expected_head",
        "finished_at",
        "handler_version",
        "heartbeat_at",
        "id",
        "idempotency_key",
        "kind",
        "observe_not_before",
        "phase",
        "progress_current",
        "progress_message",
        "progress_total",
        "remote_deadline_at",
        "repository_id",
        "request_hash",
        "requested_by_user_id",
        "resource_key",
        "result_json",
        "started_at",
        "state",
        "trigger",
        "updated_at",
        "version",
    ),
    "khub_operation_steps": (
        "artifact_checksum",
        "artifact_key",
        "artifact_length",
        "attempt",
        "checkpoint_json",
        "created_at",
        "delivery_key",
        "error_code",
        "error_summary",
        "expected_source",
        "expected_target",
        "external_marker",
        "finished_at",
        "heartbeat_at",
        "id",
        "input_json",
        "operation_id",
        "procrastinate_job_id",
        "sequence",
        "started_at",
        "state",
        "step_name",
        "step_version",
        "updated_at",
    ),
}

HISTORICAL_OPERATION_TABLE_COLUMNS_V3 = {
    **HISTORICAL_OPERATION_TABLE_COLUMNS_V2,
    "khub_commit_intents": (
        "base_head",
        "created_at",
        "error_code",
        "error_summary",
        "finalized_at",
        "id",
        "lakefs_commit_id",
        "marker",
        "operation_id",
        "payload_hash",
        "payload_json",
        "ref",
        "repository_id",
        "result_json",
        "state",
        "updated_at",
        "version",
    ),
}

OPERATION_TABLE_COLUMNS_V8 = {'khub_commit_intents': ('base_head',
                         'created_at',
                         'dispatch_started_at',
                         'error_code',
                         'error_summary',
                         'finalized_at',
                         'id',
                         'lakefs_commit_id',
                         'marker',
                         'idempotency_key',
                         'operation_id',
                         'observation_operation_id',
                         'observe_not_before',
                         'observation_head',
                         'observation_cursor',
                         'payload_hash',
                         'payload_json',
                         'ref',
                         'repository_id',
                         'result_json',
                         'request_hash',
                         'requested_by_user_id',
                         'prepared_deadline_at',
                         'remote_deadline_at',
                         'state',
                         'updated_at',
                         'version'),
 'khub_operation_steps': ('artifact_checksum',
                          'artifact_key',
                          'artifact_length',
                          'attempt',
                          'checkpoint_json',
                          'created_at',
                          'delivery_key',
                          'error_code',
                          'error_summary',
                          'expected_source',
                          'expected_target',
                          'external_marker',
                          'finished_at',
                          'heartbeat_at',
                          'id',
                          'input_json',
                          'operation_id',
                          'procrastinate_job_id',
                          'sequence',
                          'started_at',
                          'state',
                          'step_name',
                          'step_version',
                          'updated_at'),
 'khub_quota_reservations': ('created_at',
                             'finished_at',
                             'id',
                             'intent_id',
                             'is_private',
                             'reserved_bytes',
                             'scope_id',
                             'scope_type',
                             'state',
                             'updated_at'),
 'khub_repository_operations': ('cancel_requested_at',
                                'created_at',
                                'dispatch_started_at',
                                'error_code',
                                'error_summary',
                                'expected_head',
                                'finished_at',
                                'handler_version',
                                'heartbeat_at',
                                'id',
                                'idempotency_key',
                                'kind',
                                'observe_not_before',
                                'observation_cursor',
                                'phase',
                                'progress_current',
                                'progress_message',
                                'progress_total',
                                'remote_deadline_at',
                                'repository_id',
                                'request_hash',
                                'requested_by_user_id',
                                'resource_key',
                                'result_json',
                                'started_at',
                                'state',
                                'trigger',
                                'updated_at',
                                'version')}

OPERATION_COLUMN_CONTRACT_V8 = {'khub_commit_intents': {'base_head': ('text', False, None),
                         'created_at': ('timestamp with time zone',
                                        False,
                                        'CURRENT_TIMESTAMP'),
                         'dispatch_started_at': ('timestamp with time zone',
                                                 True,
                                                 None),
                         'error_code': ('text', True, None),
                         'error_summary': ('text', True, None),
                         'finalized_at': ('timestamp with time zone', True, None),
                         'id': ('uuid', False, None),
                         'idempotency_key': ('text', True, None),
                         'lakefs_commit_id': ('text', True, None),
                         'marker': ('text', False, None),
                         'observation_cursor': ('text', True, None),
                         'observation_head': ('text', True, None),
                         'observation_operation_id': ('uuid', True, None),
                         'observe_not_before': ('timestamp with time zone', True, None),
                         'operation_id': ('uuid', True, None),
                         'payload_hash': ('text', False, None),
                         'payload_json': ('jsonb', False, "'{}'::jsonb"),
                         'prepared_deadline_at': ('timestamp with time zone',
                                                  True,
                                                  None),
                         'ref': ('text', False, None),
                         'remote_deadline_at': ('timestamp with time zone', True, None),
                         'repository_id': ('bigint', False, None),
                         'request_hash': ('text', True, None),
                         'requested_by_user_id': ('bigint', True, None),
                         'result_json': ('jsonb', True, None),
                         'state': ('text', False, None),
                         'updated_at': ('timestamp with time zone',
                                        False,
                                        'CURRENT_TIMESTAMP'),
                         'version': ('bigint', False, '0')},
 'khub_operation_steps': {'artifact_checksum': ('text', True, None),
                          'artifact_key': ('text', True, None),
                          'artifact_length': ('bigint', True, None),
                          'attempt': ('integer', False, '0'),
                          'checkpoint_json': ('jsonb', False, "'{}'::jsonb"),
                          'created_at': ('timestamp with time zone',
                                         False,
                                         'CURRENT_TIMESTAMP'),
                          'delivery_key': ('text', False, None),
                          'error_code': ('text', True, None),
                          'error_summary': ('text', True, None),
                          'expected_source': ('text', True, None),
                          'expected_target': ('text', True, None),
                          'external_marker': ('text', True, None),
                          'finished_at': ('timestamp with time zone', True, None),
                          'heartbeat_at': ('timestamp with time zone', True, None),
                          'id': ('bigint',
                                 False,
                                 "nextval('khub_operation_steps_id_seq'::regclass)"),
                          'input_json': ('jsonb', False, "'{}'::jsonb"),
                          'operation_id': ('uuid', False, None),
                          'procrastinate_job_id': ('bigint', True, None),
                          'sequence': ('integer', False, None),
                          'started_at': ('timestamp with time zone', True, None),
                          'state': ('text', False, None),
                          'step_name': ('text', False, None),
                          'step_version': ('text', False, None),
                          'updated_at': ('timestamp with time zone',
                                         False,
                                         'CURRENT_TIMESTAMP')},
 'khub_quota_reservations': {'created_at': ('timestamp with time zone',
                                            False,
                                            'CURRENT_TIMESTAMP'),
                             'finished_at': ('timestamp with time zone', True, None),
                             'id': ('uuid', False, None),
                             'intent_id': ('uuid', False, None),
                             'is_private': ('boolean', False, None),
                             'reserved_bytes': ('bigint', False, None),
                             'scope_id': ('bigint', False, None),
                             'scope_type': ('text', False, None),
                             'state': ('text', False, None),
                             'updated_at': ('timestamp with time zone',
                                            False,
                                            'CURRENT_TIMESTAMP')},
 'khub_repository_operations': {'cancel_requested_at': ('timestamp with time zone',
                                                        True,
                                                        None),
                                'created_at': ('timestamp with time zone',
                                               False,
                                               'CURRENT_TIMESTAMP'),
                                'dispatch_started_at': ('timestamp with time zone',
                                                        True,
                                                        None),
                                'error_code': ('text', True, None),
                                'error_summary': ('text', True, None),
                                'expected_head': ('text', True, None),
                                'finished_at': ('timestamp with time zone', True, None),
                                'handler_version': ('text', False, None),
                                'heartbeat_at': ('timestamp with time zone',
                                                 True,
                                                 None),
                                'id': ('uuid', False, None),
                                'idempotency_key': ('text', True, None),
                                'kind': ('text', False, None),
                                'observation_cursor': ('text', True, None),
                                'observe_not_before': ('timestamp with time zone',
                                                       True,
                                                       None),
                                'phase': ('text', False, None),
                                'progress_current': ('bigint', False, '0'),
                                'progress_message': ('text', True, None),
                                'progress_total': ('bigint', True, None),
                                'remote_deadline_at': ('timestamp with time zone',
                                                       True,
                                                       None),
                                'repository_id': ('bigint', True, None),
                                'request_hash': ('text', False, None),
                                'requested_by_user_id': ('bigint', True, None),
                                'resource_key': ('text', False, None),
                                'result_json': ('jsonb', True, None),
                                'started_at': ('timestamp with time zone', True, None),
                                'state': ('text', False, None),
                                'trigger': ('text', False, None),
                                'updated_at': ('timestamp with time zone',
                                               False,
                                               'CURRENT_TIMESTAMP'),
                                'version': ('bigint', False, '0')}}

EXPECTED_OPERATION_INDEX_DEFINITIONS_V8 = {'khub_commit_intent_marker_uidx': 'CREATE UNIQUE INDEX khub_commit_intent_marker_uidx '
                                   'ON public.khub_commit_intents USING btree (marker)',
 'khub_commit_intent_observation_operation_uidx': 'CREATE UNIQUE INDEX '
                                                  'khub_commit_intent_observation_operation_uidx '
                                                  'ON public.khub_commit_intents USING '
                                                  'btree (observation_operation_id) '
                                                  'WHERE (observation_operation_id IS '
                                                  'NOT NULL)',
 'khub_commit_intent_operation_uidx': 'CREATE UNIQUE INDEX '
                                      'khub_commit_intent_operation_uidx ON '
                                      'public.khub_commit_intents USING btree '
                                      '(operation_id) WHERE (operation_id IS NOT NULL)',
 'khub_commit_intent_recovery_idx': 'CREATE INDEX khub_commit_intent_recovery_idx ON '
                                    'public.khub_commit_intents USING btree (state, '
                                    'updated_at)',
 'khub_commit_intent_repo_ref_idx': 'CREATE INDEX khub_commit_intent_repo_ref_idx ON '
                                    'public.khub_commit_intents USING btree '
                                    '(repository_id, ref, created_at DESC)',
 'khub_commit_intent_request_uidx': 'CREATE UNIQUE INDEX '
                                    'khub_commit_intent_request_uidx ON '
                                    'public.khub_commit_intents USING btree '
                                    '(requested_by_user_id, repository_id, ref, '
                                    'idempotency_key) WHERE (idempotency_key IS NOT '
                                    'NULL)',
 'khub_operation_active_resource_uidx': 'CREATE UNIQUE INDEX '
                                        'khub_operation_active_resource_uidx ON '
                                        'public.khub_repository_operations USING btree '
                                        '(resource_key) WHERE (state = ANY '
                                        "(ARRAY['accepted'::text, 'running'::text, "
                                        "'cancel_requested'::text, "
                                        "'dispatch_started'::text, "
                                        "'uncertain'::text]))",
 'khub_operation_idempotency_uidx': 'CREATE UNIQUE INDEX '
                                    'khub_operation_idempotency_uidx ON '
                                    'public.khub_repository_operations USING btree '
                                    '(requested_by_user_id, idempotency_key) WHERE '
                                    '(idempotency_key IS NOT NULL)',
 'khub_operation_owner_idx': 'CREATE INDEX khub_operation_owner_idx ON '
                             'public.khub_repository_operations USING btree '
                             '(requested_by_user_id, created_at DESC)',
 'khub_operation_repository_idx': 'CREATE INDEX khub_operation_repository_idx ON '
                                  'public.khub_repository_operations USING btree '
                                  '(repository_id, created_at DESC)',
 'khub_operation_steps_operation_idx': 'CREATE INDEX '
                                       'khub_operation_steps_operation_idx ON '
                                       'public.khub_operation_steps USING btree '
                                       '(operation_id, sequence)',
 'khub_operation_steps_state_idx': 'CREATE INDEX khub_operation_steps_state_idx ON '
                                   'public.khub_operation_steps USING btree (state, '
                                   'updated_at)',
 'khub_quota_reservation_active_idx': 'CREATE INDEX khub_quota_reservation_active_idx '
                                      'ON public.khub_quota_reservations USING btree '
                                      '(scope_type, scope_id, is_private, state)',
 'khub_quota_reservation_intent_idx': 'CREATE INDEX khub_quota_reservation_intent_idx '
                                      'ON public.khub_quota_reservations USING btree '
                                      '(intent_id, state)'}

EXPECTED_OPERATION_CONSTRAINT_DEFINITIONS_V8 = {'khub_commit_intent_marker_ck': ('c', "CHECK (marker ~~ 'khub:v1:%'::text)"),
 'khub_commit_intent_payload_hash_ck': ('c',
                                        'CHECK (payload_hash ~ '
                                        "'^[0-9a-f]{64}$'::text)"),
 'khub_commit_intent_state_ck': ('c',
                                 "CHECK (state = ANY (ARRAY['prepared'::text, "
                                 "'dispatch_started'::text, 'committed'::text, "
                                 "'finalized'::text, 'reconciliation_required'::text, "
                                 "'uncertain'::text, 'abandoned'::text]))"),
 'khub_commit_intents_pkey': ('p', 'PRIMARY KEY (id)'),
 'khub_operation_progress_ck': ('c',
                                'CHECK (progress_current >= 0 AND (progress_total IS '
                                'NULL OR progress_total >= progress_current))'),
 'khub_operation_state_ck': ('c',
                             "CHECK (state = ANY (ARRAY['accepted'::text, "
                             "'running'::text, 'cancel_requested'::text, "
                             "'succeeded'::text, 'failed'::text, 'cancelled'::text, "
                             "'dispatch_started'::text, 'uncertain'::text, "
                             "'cleanup_pending'::text]))"),
 'khub_operation_steps_operation_id_fkey': ('f',
                                            'FOREIGN KEY (operation_id) REFERENCES '
                                            'khub_repository_operations(id) ON DELETE '
                                            'CASCADE'),
 'khub_operation_steps_pkey': ('p', 'PRIMARY KEY (id)'),
 'khub_operation_trigger_ck': ('c',
                               "CHECK (trigger = ANY (ARRAY['api'::text, "
                               "'schedule'::text, 'reconcile'::text, "
                               "'system'::text]))"),
 'khub_quota_reservation_bytes_ck': ('c', 'CHECK (reserved_bytes > 0)'),
 'khub_quota_reservation_scope_ck': ('c',
                                     'CHECK (scope_type = ANY '
                                     "(ARRAY['repository'::text, 'namespace'::text]))"),
 'khub_quota_reservation_state_ck': ('c',
                                     "CHECK (state = ANY (ARRAY['reserved'::text, "
                                     "'consumed'::text, 'released'::text]))"),
 'khub_quota_reservation_unique_scope_uidx': ('u', 'UNIQUE (intent_id, scope_type)'),
 'khub_quota_reservations_intent_id_fkey': ('f',
                                            'FOREIGN KEY (intent_id) REFERENCES '
                                            'khub_commit_intents(id) ON DELETE '
                                            'CASCADE'),
 'khub_quota_reservations_pkey': ('p', 'PRIMARY KEY (id)'),
 'khub_repository_operations_pkey': ('p', 'PRIMARY KEY (id)'),
 'khub_step_attempt_ck': ('c', 'CHECK (attempt >= 0)'),
 'khub_step_delivery_uidx': ('u', 'UNIQUE (delivery_key)'),
 'khub_step_sequence_uidx': ('u', 'UNIQUE (operation_id, sequence)'),
 'khub_step_state_ck': ('c',
                        "CHECK (state = ANY (ARRAY['pending'::text, 'running'::text, "
                        "'dispatch_started'::text, 'observing'::text, "
                        "'succeeded'::text, 'failed'::text, 'cancelled'::text, "
                        "'uncertain'::text]))")}

PROCRASTINATE_TABLE_COLUMNS_V390 = {'procrastinate_events': {'id', 'at', 'job_id', 'type'},
 'procrastinate_jobs': {'abort_requested',
                        'args',
                        'attempts',
                        'id',
                        'lock',
                        'priority',
                        'queue_name',
                        'queueing_lock',
                        'scheduled_at',
                        'status',
                        'task_name',
                        'worker_id'},
 'procrastinate_periodic_defers': {'defer_timestamp',
                                   'id',
                                   'job_id',
                                   'periodic_id',
                                   'task_name'},
 'procrastinate_workers': {'last_heartbeat', 'id'}}

PROCRASTINATE_REQUIRED_INDEXES_V390 = {'idx_procrastinate_jobs_worker_not_null',
 'idx_procrastinate_workers_last_heartbeat',
 'procrastinate_events_job_id_fkey_v1',
 'procrastinate_jobs_id_lock_idx_v1',
 'procrastinate_jobs_lock_idx_v1',
 'procrastinate_jobs_priority_idx_v1',
 'procrastinate_jobs_queue_name_idx_v1',
 'procrastinate_jobs_queueing_lock_idx_v1',
 'procrastinate_periodic_defers_job_id_fkey_v1'}

PROCRASTINATE_TYPES_V390 = {'procrastinate_job_event_type': ('enum',
                                  ('deferred',
                                   'started',
                                   'deferred_for_retry',
                                   'failed',
                                   'succeeded',
                                   'cancelled',
                                   'abort_requested',
                                   'aborted',
                                   'scheduled',
                                   'retried')),
 'procrastinate_job_status': ('enum',
                              ('todo',
                               'doing',
                               'succeeded',
                               'failed',
                               'cancelled',
                               'aborting',
                               'aborted')),
 'procrastinate_job_to_defer_v1': ('composite',
                                   (('queue_name', 'character varying'),
                                    ('task_name', 'character varying'),
                                    ('priority', 'integer'),
                                    ('lock', 'text'),
                                    ('queueing_lock', 'text'),
                                    ('args', 'jsonb'),
                                    ('scheduled_at', 'timestamp with time zone')))}
OPERATION_TABLE_COLUMNS_V6 = {'khub_commit_intents': ('base_head',
                         'created_at',
                         'dispatch_started_at',
                         'error_code',
                         'error_summary',
                         'finalized_at',
                         'id',
                         'lakefs_commit_id',
                         'marker',
                         'idempotency_key',
                         'operation_id',
                         'observation_operation_id',
                         'observe_not_before',
                         'observation_head',
                         'payload_hash',
                         'payload_json',
                         'ref',
                         'repository_id',
                         'result_json',
                         'request_hash',
                         'requested_by_user_id',
                         'prepared_deadline_at',
                         'remote_deadline_at',
                         'state',
                         'updated_at',
                         'version'),
 'khub_operation_steps': ('artifact_checksum',
                          'artifact_key',
                          'artifact_length',
                          'attempt',
                          'checkpoint_json',
                          'created_at',
                          'delivery_key',
                          'error_code',
                          'error_summary',
                          'expected_source',
                          'expected_target',
                          'external_marker',
                          'finished_at',
                          'heartbeat_at',
                          'id',
                          'input_json',
                          'operation_id',
                          'procrastinate_job_id',
                          'sequence',
                          'started_at',
                          'state',
                          'step_name',
                          'step_version',
                          'updated_at'),
 'khub_quota_reservations': ('created_at',
                             'finished_at',
                             'id',
                             'intent_id',
                             'is_private',
                             'reserved_bytes',
                             'scope_id',
                             'scope_type',
                             'state',
                             'updated_at'),
 'khub_repository_operations': ('cancel_requested_at',
                                'created_at',
                                'dispatch_started_at',
                                'error_code',
                                'error_summary',
                                'expected_head',
                                'finished_at',
                                'handler_version',
                                'heartbeat_at',
                                'id',
                                'idempotency_key',
                                'kind',
                                'observe_not_before',
                                'observation_cursor',
                                'phase',
                                'progress_current',
                                'progress_message',
                                'progress_total',
                                'remote_deadline_at',
                                'repository_id',
                                'request_hash',
                                'requested_by_user_id',
                                'resource_key',
                                'result_json',
                                'started_at',
                                'state',
                                'trigger',
                                'updated_at',
                                'version')}

OPERATION_TABLE_COLUMNS_V7 = {'khub_commit_intents': ('base_head',
                         'created_at',
                         'dispatch_started_at',
                         'error_code',
                         'error_summary',
                         'finalized_at',
                         'id',
                         'lakefs_commit_id',
                         'marker',
                         'idempotency_key',
                         'operation_id',
                         'observation_operation_id',
                         'observe_not_before',
                         'observation_cursor',
                         'payload_hash',
                         'payload_json',
                         'ref',
                         'repository_id',
                         'result_json',
                         'request_hash',
                         'requested_by_user_id',
                         'prepared_deadline_at',
                         'remote_deadline_at',
                         'state',
                         'updated_at',
                         'version'),
 'khub_operation_steps': ('artifact_checksum',
                          'artifact_key',
                          'artifact_length',
                          'attempt',
                          'checkpoint_json',
                          'created_at',
                          'delivery_key',
                          'error_code',
                          'error_summary',
                          'expected_source',
                          'expected_target',
                          'external_marker',
                          'finished_at',
                          'heartbeat_at',
                          'id',
                          'input_json',
                          'operation_id',
                          'procrastinate_job_id',
                          'sequence',
                          'started_at',
                          'state',
                          'step_name',
                          'step_version',
                          'updated_at'),
 'khub_quota_reservations': ('created_at',
                             'finished_at',
                             'id',
                             'intent_id',
                             'is_private',
                             'reserved_bytes',
                             'scope_id',
                             'scope_type',
                             'state',
                             'updated_at'),
 'khub_repository_operations': ('cancel_requested_at',
                                'created_at',
                                'dispatch_started_at',
                                'error_code',
                                'error_summary',
                                'expected_head',
                                'finished_at',
                                'handler_version',
                                'heartbeat_at',
                                'id',
                                'idempotency_key',
                                'kind',
                                'observe_not_before',
                                'observation_cursor',
                                'phase',
                                'progress_current',
                                'progress_message',
                                'progress_total',
                                'remote_deadline_at',
                                'repository_id',
                                'request_hash',
                                'requested_by_user_id',
                                'resource_key',
                                'result_json',
                                'started_at',
                                'state',
                                'trigger',
                                'updated_at',
                                'version')}
OPERATION_SCHEMA_SQL_V8: str = """
CREATE TABLE IF NOT EXISTS khub_repository_operations (
    id UUID PRIMARY KEY,
    kind TEXT NOT NULL,
    handler_version TEXT NOT NULL,
    repository_id BIGINT,
    resource_key TEXT NOT NULL,
    requested_by_user_id BIGINT,
    trigger TEXT NOT NULL,
    idempotency_key TEXT,
    request_hash TEXT NOT NULL,
    state TEXT NOT NULL,
    phase TEXT NOT NULL,
    progress_current BIGINT NOT NULL DEFAULT 0,
    progress_total BIGINT,
    progress_message TEXT,
    expected_head TEXT,
    cancel_requested_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    dispatch_started_at TIMESTAMPTZ,
    remote_deadline_at TIMESTAMPTZ,
    observe_not_before TIMESTAMPTZ,
    observation_cursor TEXT,
    result_json JSONB,
    error_code TEXT,
    error_summary TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    version BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT khub_operation_state_ck CHECK (
        state IN (
            'accepted', 'running', 'cancel_requested', 'succeeded',
            'failed', 'cancelled', 'dispatch_started', 'uncertain', 'cleanup_pending'
        )
    ),
    CONSTRAINT khub_operation_trigger_ck CHECK (
        trigger IN ('api', 'schedule', 'reconcile', 'system')
    ),
    CONSTRAINT khub_operation_progress_ck CHECK (
        progress_current >= 0
        AND (progress_total IS NULL OR progress_total >= progress_current)
    )
);

ALTER TABLE khub_repository_operations
    ADD COLUMN IF NOT EXISTS observation_cursor TEXT;

ALTER TABLE khub_repository_operations
    DROP CONSTRAINT IF EXISTS khub_operation_state_ck;

ALTER TABLE khub_repository_operations
    ADD CONSTRAINT khub_operation_state_ck CHECK (
        state IN (
            'accepted', 'running', 'cancel_requested', 'succeeded',
            'failed', 'cancelled', 'dispatch_started', 'uncertain', 'cleanup_pending'
        )
    );

CREATE UNIQUE INDEX IF NOT EXISTS khub_operation_idempotency_uidx
    ON khub_repository_operations (requested_by_user_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS khub_operation_active_resource_uidx
    ON khub_repository_operations (resource_key)
    WHERE state IN ('accepted', 'running', 'cancel_requested', 'dispatch_started', 'uncertain');

CREATE INDEX IF NOT EXISTS khub_operation_owner_idx
    ON khub_repository_operations (requested_by_user_id, created_at DESC);

CREATE INDEX IF NOT EXISTS khub_operation_repository_idx
    ON khub_repository_operations (repository_id, created_at DESC);

CREATE TABLE IF NOT EXISTS khub_operation_steps (
    id BIGSERIAL PRIMARY KEY,
    operation_id UUID NOT NULL REFERENCES khub_repository_operations(id) ON DELETE CASCADE,
    sequence INTEGER NOT NULL,
    step_name TEXT NOT NULL,
    step_version TEXT NOT NULL,
    state TEXT NOT NULL,
    attempt INTEGER NOT NULL DEFAULT 0,
    delivery_key TEXT NOT NULL,
    procrastinate_job_id BIGINT,
    input_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    checkpoint_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    external_marker TEXT,
    expected_source TEXT,
    expected_target TEXT,
    artifact_key TEXT,
    artifact_checksum TEXT,
    artifact_length BIGINT,
    error_code TEXT,
    error_summary TEXT,
    started_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT khub_step_state_ck CHECK (
        state IN (
            'pending', 'running', 'dispatch_started', 'observing',
            'succeeded', 'failed', 'cancelled', 'uncertain'
        )
    ),
    CONSTRAINT khub_step_attempt_ck CHECK (attempt >= 0),
    CONSTRAINT khub_step_sequence_uidx UNIQUE (operation_id, sequence),
    CONSTRAINT khub_step_delivery_uidx UNIQUE (delivery_key)
);

CREATE INDEX IF NOT EXISTS khub_operation_steps_operation_idx
    ON khub_operation_steps (operation_id, sequence);

CREATE INDEX IF NOT EXISTS khub_operation_steps_state_idx
    ON khub_operation_steps (state, updated_at);

CREATE TABLE IF NOT EXISTS khub_commit_intents (
    id UUID PRIMARY KEY,
    operation_id UUID,
    observation_operation_id UUID,
    repository_id BIGINT NOT NULL,
    requested_by_user_id BIGINT,
    ref TEXT NOT NULL,
    base_head TEXT NOT NULL,
    marker TEXT NOT NULL,
    idempotency_key TEXT,
    request_hash TEXT,
    prepared_deadline_at TIMESTAMPTZ,
    dispatch_started_at TIMESTAMPTZ,
    remote_deadline_at TIMESTAMPTZ,
    observe_not_before TIMESTAMPTZ,
    observation_head TEXT,
    payload_hash TEXT NOT NULL,
    payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    state TEXT NOT NULL,
    lakefs_commit_id TEXT,
    result_json JSONB,
    error_code TEXT,
    error_summary TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finalized_at TIMESTAMPTZ,
    version BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT khub_commit_intent_state_ck CHECK (
        state IN ('prepared', 'dispatch_started', 'committed', 'finalized', 'reconciliation_required', 'uncertain', 'abandoned')
    ),
    CONSTRAINT khub_commit_intent_marker_ck CHECK (
        marker LIKE 'khub:v1:%'
    ),
    CONSTRAINT khub_commit_intent_payload_hash_ck CHECK (
        payload_hash ~ '^[0-9a-f]{64}$'
    )
);

ALTER TABLE khub_commit_intents
    DROP CONSTRAINT IF EXISTS khub_commit_intent_state_ck;
ALTER TABLE khub_commit_intents
    ADD CONSTRAINT khub_commit_intent_state_ck CHECK (
        state IN ('prepared', 'dispatch_started', 'committed', 'finalized', 'reconciliation_required', 'uncertain', 'abandoned')
    );

CREATE UNIQUE INDEX IF NOT EXISTS khub_commit_intent_marker_uidx
    ON khub_commit_intents (marker);

CREATE UNIQUE INDEX IF NOT EXISTS khub_commit_intent_operation_uidx
    ON khub_commit_intents (operation_id)
    WHERE operation_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS khub_commit_intent_recovery_idx
    ON khub_commit_intents (state, updated_at);

CREATE INDEX IF NOT EXISTS khub_commit_intent_repo_ref_idx
    ON khub_commit_intents (repository_id, ref, created_at DESC);

ALTER TABLE khub_commit_intents
    ADD COLUMN IF NOT EXISTS requested_by_user_id BIGINT;
ALTER TABLE khub_commit_intents
    ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
ALTER TABLE khub_commit_intents
    ADD COLUMN IF NOT EXISTS request_hash TEXT;
ALTER TABLE khub_commit_intents
    ADD COLUMN IF NOT EXISTS dispatch_started_at TIMESTAMPTZ;
ALTER TABLE khub_commit_intents
    ADD COLUMN IF NOT EXISTS remote_deadline_at TIMESTAMPTZ;
ALTER TABLE khub_commit_intents
    ADD COLUMN IF NOT EXISTS observe_not_before TIMESTAMPTZ;
ALTER TABLE khub_commit_intents
    ADD COLUMN IF NOT EXISTS observation_operation_id UUID;
ALTER TABLE khub_commit_intents
    ADD COLUMN IF NOT EXISTS prepared_deadline_at TIMESTAMPTZ;
ALTER TABLE khub_commit_intents
    ADD COLUMN IF NOT EXISTS observation_cursor TEXT;
ALTER TABLE khub_commit_intents
    ADD COLUMN IF NOT EXISTS observation_head TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS khub_commit_intent_request_uidx
    ON khub_commit_intents (requested_by_user_id, repository_id, ref, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS khub_commit_intent_observation_operation_uidx
    ON khub_commit_intents (observation_operation_id)
    WHERE observation_operation_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS khub_quota_reservations (
    id UUID PRIMARY KEY,
    intent_id UUID NOT NULL REFERENCES khub_commit_intents(id) ON DELETE CASCADE,
    scope_type TEXT NOT NULL,
    scope_id BIGINT NOT NULL,
    is_private BOOLEAN NOT NULL,
    reserved_bytes BIGINT NOT NULL,
    state TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    finished_at TIMESTAMPTZ,
    CONSTRAINT khub_quota_reservation_scope_ck CHECK (
        scope_type IN ('repository', 'namespace')
    ),
    CONSTRAINT khub_quota_reservation_bytes_ck CHECK (reserved_bytes > 0),
    CONSTRAINT khub_quota_reservation_state_ck CHECK (
        state IN ('reserved', 'consumed', 'released')
    ),
    CONSTRAINT khub_quota_reservation_unique_scope_uidx
        UNIQUE (intent_id, scope_type)
);

CREATE INDEX IF NOT EXISTS khub_quota_reservation_active_idx
    ON khub_quota_reservations (scope_type, scope_id, is_private, state);
CREATE INDEX IF NOT EXISTS khub_quota_reservation_intent_idx
    ON khub_quota_reservations (intent_id, state);

"""

PROCRASTINATE_SCHEMA_SQL_V390: str = """-- Procrastinate Schema

DO $$
BEGIN
    CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;
EXCEPTION
    WHEN OTHERS THEN
        -- On managed PostgreSQL services (e.g. Azure Database for PostgreSQL, Amazon RDS,
        -- Google Cloud SQL), CREATE EXTENSION may fail with various error codes:
        -- insufficient_privilege, feature_not_supported, or provider-specific errors.
        -- Before ignoring, verify the extension actually exists — if it does not exist
        -- and cannot be created, re-raise so the failure is explicit.
        IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'plpgsql') THEN
            RAISE NOTICE 'plpgsql extension already exists, skipping creation.';
        ELSE
            RAISE;
        END IF;
END;
$$;

-- Enums

CREATE TYPE procrastinate_job_status AS ENUM (
    'todo',  -- The job is queued
    'doing',  -- The job has been fetched by a worker
    'succeeded',  -- The job ended successfully
    'failed',  -- The job ended with an error
    'cancelled', -- The job was cancelled
    'aborting',  -- legacy, not used anymore since v3.0.0
    'aborted'  -- The job was aborted
);

CREATE TYPE procrastinate_job_event_type AS ENUM (
    'deferred',  -- Job created, in todo
    'started',  -- todo -> doing
    'deferred_for_retry',  -- doing -> todo
    'failed',  -- doing -> failed
    'succeeded',  -- doing -> succeeded
    'cancelled', -- todo -> cancelled
    'abort_requested', -- not a state transition, but set in a separate field
    'aborted', -- doing -> aborted (only allowed when abort_requested field is set)
    'scheduled', -- not a state transition, but recording when a task is scheduled for
    'retried' -- Manually retried failed job
);

-- Composite Types

CREATE TYPE procrastinate_job_to_defer_v1 AS (
    queue_name character varying,
    task_name character varying,
    priority integer,
    lock text,
    queueing_lock text,
    args jsonb,
    scheduled_at timestamp with time zone
);

-- Tables

CREATE TABLE procrastinate_workers(
    id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    last_heartbeat timestamp with time zone NOT NULL DEFAULT NOW()
);

CREATE TABLE procrastinate_jobs (
    id bigserial PRIMARY KEY,
    queue_name character varying(128) NOT NULL,
    task_name character varying(128) NOT NULL,
    priority integer DEFAULT 0 NOT NULL,
    lock text,
    queueing_lock text,
    args jsonb DEFAULT '{}' NOT NULL,
    status procrastinate_job_status DEFAULT 'todo'::procrastinate_job_status NOT NULL,
    scheduled_at timestamp with time zone NULL,
    attempts integer DEFAULT 0 NOT NULL,
    abort_requested boolean DEFAULT false NOT NULL,
    worker_id bigint REFERENCES procrastinate_workers(id) ON DELETE SET NULL,
    CONSTRAINT check_not_todo_abort_requested CHECK (NOT (status = 'todo' AND abort_requested = true))
);

CREATE TABLE procrastinate_periodic_defers (
    id bigserial PRIMARY KEY,
    task_name character varying(128) NOT NULL,
    defer_timestamp bigint,
    job_id bigint REFERENCES procrastinate_jobs(id) NULL,
    periodic_id character varying(128) NOT NULL DEFAULT '',
    CONSTRAINT procrastinate_periodic_defers_unique UNIQUE (task_name, periodic_id, defer_timestamp)
);

CREATE TABLE procrastinate_events (
    id bigserial PRIMARY KEY,
    job_id bigint NOT NULL REFERENCES procrastinate_jobs ON DELETE CASCADE,
    type procrastinate_job_event_type,
    at timestamp with time zone DEFAULT NOW() NULL
);

-- Constraints & Indices

-- this prevents from having several jobs with the same queueing lock in the "todo" state
CREATE UNIQUE INDEX procrastinate_jobs_queueing_lock_idx_v1 ON procrastinate_jobs (queueing_lock) WHERE status = 'todo';
-- this prevents from having several jobs with the same lock in the "doing" state
CREATE UNIQUE INDEX procrastinate_jobs_lock_idx_v1 ON procrastinate_jobs (lock) WHERE status = 'doing';

-- Index for select_stalled_jobs_by_heartbeat query
CREATE INDEX idx_procrastinate_jobs_worker_not_null ON procrastinate_jobs(worker_id) WHERE worker_id IS NOT NULL AND status = 'doing'::procrastinate_job_status;

CREATE INDEX procrastinate_jobs_queue_name_idx_v1 ON procrastinate_jobs(queue_name);
CREATE INDEX procrastinate_jobs_id_lock_idx_v1 ON procrastinate_jobs (id, lock) WHERE status = ANY (ARRAY['todo'::procrastinate_job_status, 'doing'::procrastinate_job_status]);
CREATE INDEX procrastinate_jobs_priority_idx_v1 ON procrastinate_jobs(priority desc, id asc) WHERE (status = 'todo'::procrastinate_job_status);

CREATE INDEX procrastinate_events_job_id_fkey_v1 ON procrastinate_events(job_id);

CREATE INDEX procrastinate_periodic_defers_job_id_fkey_v1 ON procrastinate_periodic_defers(job_id);

CREATE INDEX idx_procrastinate_workers_last_heartbeat ON procrastinate_workers(last_heartbeat);

-- Functions
CREATE FUNCTION procrastinate_defer_jobs_v1(
    jobs procrastinate_job_to_defer_v1[]
)
    RETURNS bigint[]
    LANGUAGE plpgsql
AS $$
DECLARE
    job_ids bigint[];
BEGIN
    WITH inserted_jobs AS (
        INSERT INTO procrastinate_jobs (queue_name, task_name, priority, lock, queueing_lock, args, scheduled_at)
        SELECT (job).queue_name,
               (job).task_name,
               (job).priority,
               (job).lock,
               (job).queueing_lock,
               (job).args,
               (job).scheduled_at
        FROM unnest(jobs) AS job
        RETURNING id
    )
    SELECT array_agg(id) FROM inserted_jobs INTO job_ids;

    RETURN job_ids;
END;
$$;

CREATE FUNCTION procrastinate_defer_periodic_job_v2(
    _queue_name character varying,
    _lock character varying,
    _queueing_lock character varying,
    _task_name character varying,
    _priority integer,
    _periodic_id character varying,
    _defer_timestamp bigint,
    _args jsonb
)
    RETURNS bigint
    LANGUAGE plpgsql
AS $$
DECLARE
	_job_id bigint;
	_defer_id bigint;
BEGIN
    INSERT
        INTO procrastinate_periodic_defers (task_name, periodic_id, defer_timestamp)
        VALUES (_task_name, _periodic_id, _defer_timestamp)
        ON CONFLICT DO NOTHING
        RETURNING id into _defer_id;

    IF _defer_id IS NULL THEN
        RETURN NULL;
    END IF;

    UPDATE procrastinate_periodic_defers
        SET job_id = (
            SELECT COALESCE((
                SELECT unnest(procrastinate_defer_jobs_v1(
                    ARRAY[
                        ROW(
                            _queue_name,
                            _task_name,
                            _priority,
                            _lock,
                            _queueing_lock,
                            _args,
                            NULL::timestamptz
                        )
                    ]::procrastinate_job_to_defer_v1[]
                ))
            ), NULL)
        )
        WHERE id = _defer_id
        RETURNING job_id INTO _job_id;

    DELETE
        FROM procrastinate_periodic_defers
        USING (
            SELECT id
            FROM procrastinate_periodic_defers
            WHERE procrastinate_periodic_defers.task_name = _task_name
            AND procrastinate_periodic_defers.periodic_id = _periodic_id
            AND procrastinate_periodic_defers.defer_timestamp < _defer_timestamp
            ORDER BY id
            FOR UPDATE
        ) to_delete
        WHERE procrastinate_periodic_defers.id = to_delete.id;

    RETURN _job_id;
END;
$$;

CREATE FUNCTION procrastinate_fetch_job_v2(
    target_queue_names character varying[],
    p_worker_id bigint
)
    RETURNS procrastinate_jobs
    LANGUAGE plpgsql
AS $$
DECLARE
	found_jobs procrastinate_jobs;
BEGIN
    WITH candidate AS (
        SELECT jobs.*
            FROM procrastinate_jobs AS jobs
            WHERE
                -- reject the job if its lock has earlier or higher priority jobs
                NOT EXISTS (
                    SELECT 1
                        FROM procrastinate_jobs AS other_jobs
                        WHERE
                            jobs.lock IS NOT NULL
                            AND other_jobs.lock = jobs.lock
                            AND (
                                -- job with same lock is already running
                                other_jobs.status = 'doing'
                                OR
                                -- job with same lock is waiting and has higher priority (or same priority but was queued first)
                                (
                                    other_jobs.status = 'todo'
                                    AND (
                                        other_jobs.priority > jobs.priority
                                        OR (
                                        other_jobs.priority = jobs.priority
                                        AND other_jobs.id < jobs.id
                                        )
                                    )
                                )
                            )
                )
                AND jobs.status = 'todo'
                AND (target_queue_names IS NULL OR jobs.queue_name = ANY( target_queue_names ))
                AND (jobs.scheduled_at IS NULL OR jobs.scheduled_at <= now())
            ORDER BY jobs.priority DESC, jobs.id ASC LIMIT 1
            FOR UPDATE OF jobs SKIP LOCKED
    )
    UPDATE procrastinate_jobs
        SET status = 'doing', worker_id = p_worker_id
        FROM candidate
        WHERE procrastinate_jobs.id = candidate.id
        RETURNING procrastinate_jobs.* INTO found_jobs;

 RETURN found_jobs;
END;
$$;

CREATE FUNCTION procrastinate_finish_job_v1(job_id bigint, end_status procrastinate_job_status, delete_job boolean)
    RETURNS void
    LANGUAGE plpgsql
AS $$
DECLARE
    _job_id bigint;
BEGIN
    IF end_status NOT IN ('succeeded', 'failed', 'aborted') THEN
        RAISE 'End status should be either "succeeded", "failed" or "aborted" (job id: %)', job_id;
    END IF;
    IF delete_job THEN
        DELETE FROM procrastinate_jobs
        WHERE id = job_id AND status IN ('todo', 'doing')
        RETURNING id INTO _job_id;
    ELSE
        UPDATE procrastinate_jobs
        SET status = end_status,
            abort_requested = false,
            attempts = CASE status
                WHEN 'doing' THEN attempts + 1 ELSE attempts
            END
        WHERE id = job_id AND status IN ('todo', 'doing')
        RETURNING id INTO _job_id;
    END IF;
    IF _job_id IS NULL THEN
        RAISE 'Job was not found or not in "doing" or "todo" status (job id: %)', job_id;
    END IF;
END;
$$;

CREATE FUNCTION procrastinate_cancel_job_v1(job_id bigint, abort boolean, delete_job boolean)
    RETURNS bigint
    LANGUAGE plpgsql
AS $$
DECLARE
    _job_id bigint;
BEGIN
    IF delete_job THEN
        DELETE FROM procrastinate_jobs
        WHERE id = job_id AND status = 'todo'
        RETURNING id INTO _job_id;
    END IF;
    IF _job_id IS NULL THEN
        IF abort THEN
            UPDATE procrastinate_jobs
            SET abort_requested = true,
                status = CASE status
                    WHEN 'todo' THEN 'cancelled'::procrastinate_job_status ELSE status
                END
            WHERE id = job_id AND status IN ('todo', 'doing')
            RETURNING id INTO _job_id;
        ELSE
            UPDATE procrastinate_jobs
            SET status = 'cancelled'::procrastinate_job_status
            WHERE id = job_id AND status = 'todo'
            RETURNING id INTO _job_id;
        END IF;
    END IF;
    RETURN _job_id;
END;
$$;

CREATE FUNCTION procrastinate_retry_job_v1(
    job_id bigint,
    retry_at timestamp with time zone,
    new_priority integer,
    new_queue_name character varying,
    new_lock character varying
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _job_id bigint;
    _abort_requested boolean;
BEGIN
    SELECT abort_requested FROM procrastinate_jobs
    WHERE id = job_id AND status = 'doing'
    FOR UPDATE
    INTO _abort_requested;
    IF _abort_requested THEN
        UPDATE procrastinate_jobs
        SET status = 'failed'::procrastinate_job_status
        WHERE id = job_id AND status = 'doing'
        RETURNING id INTO _job_id;
    ELSE
        UPDATE procrastinate_jobs
        SET status = 'todo'::procrastinate_job_status,
            attempts = attempts + 1,
            scheduled_at = retry_at,
            priority = COALESCE(new_priority, priority),
            queue_name = COALESCE(new_queue_name, queue_name),
            lock = COALESCE(new_lock, lock)
        WHERE id = job_id AND status = 'doing'
        RETURNING id INTO _job_id;
    END IF;

    IF _job_id IS NULL THEN
        RAISE 'Job was not found or not in "doing" status (job id: %)', job_id;
    END IF;
END;
$$;

CREATE FUNCTION procrastinate_retry_job_v2(
    job_id bigint,
    retry_at timestamp with time zone,
    new_priority integer,
    new_queue_name character varying,
    new_lock character varying
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    _job_id bigint;
    _abort_requested boolean;
    _current_status procrastinate_job_status;
BEGIN
    SELECT status, abort_requested FROM procrastinate_jobs
    WHERE id = job_id AND status IN ('doing', 'failed')
    FOR UPDATE
    INTO _current_status, _abort_requested;
    IF _current_status = 'doing' AND _abort_requested THEN
        UPDATE procrastinate_jobs
        SET status = 'failed'::procrastinate_job_status
        WHERE id = job_id AND status = 'doing'
        RETURNING id INTO _job_id;
    ELSE
        UPDATE procrastinate_jobs
        SET status = 'todo'::procrastinate_job_status,
            attempts = attempts + 1,
            scheduled_at = retry_at,
            priority = COALESCE(new_priority, priority),
            queue_name = COALESCE(new_queue_name, queue_name),
            lock = COALESCE(new_lock, lock)
        WHERE id = job_id AND status IN ('doing', 'failed')
        RETURNING id INTO _job_id;
    END IF;

    IF _job_id IS NULL THEN
        RAISE 'Job was not found or has an invalid status to retry (job id: %)', job_id;
    END IF;

END;
$$;

CREATE FUNCTION procrastinate_notify_queue_job_inserted_v1()
    RETURNS trigger
    LANGUAGE plpgsql
AS $$
DECLARE
    payload TEXT;
BEGIN
    SELECT json_build_object('type', 'job_inserted', 'job_id', NEW.id)::text INTO payload;
	PERFORM pg_notify('procrastinate_queue_v1#' || NEW.queue_name, payload);
	PERFORM pg_notify('procrastinate_any_queue_v1', payload);
	RETURN NEW;
END;
$$;

CREATE FUNCTION procrastinate_notify_queue_abort_job_v1()
RETURNS trigger
    LANGUAGE plpgsql
AS $$
DECLARE
    payload TEXT;
BEGIN
    SELECT json_build_object('type', 'abort_job_requested', 'job_id', NEW.id)::text INTO payload;
	PERFORM pg_notify('procrastinate_queue_v1#' || NEW.queue_name, payload);
	PERFORM pg_notify('procrastinate_any_queue_v1', payload);
	RETURN NEW;
END;
$$;

CREATE FUNCTION procrastinate_trigger_function_status_events_insert_v1()
    RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO procrastinate_events(job_id, type)
        VALUES (NEW.id, 'deferred'::procrastinate_job_event_type);
	RETURN NEW;
END;
$$;

CREATE FUNCTION procrastinate_trigger_function_status_events_update_v1()
    RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    WITH t AS (
        SELECT CASE
            WHEN OLD.status = 'todo'::procrastinate_job_status
                AND NEW.status = 'doing'::procrastinate_job_status
                THEN 'started'::procrastinate_job_event_type
            WHEN OLD.status = 'doing'::procrastinate_job_status
                AND NEW.status = 'todo'::procrastinate_job_status
                THEN 'deferred_for_retry'::procrastinate_job_event_type
            WHEN OLD.status = 'doing'::procrastinate_job_status
                AND NEW.status = 'failed'::procrastinate_job_status
                THEN 'failed'::procrastinate_job_event_type
            WHEN OLD.status = 'doing'::procrastinate_job_status
                AND NEW.status = 'succeeded'::procrastinate_job_status
                THEN 'succeeded'::procrastinate_job_event_type
            WHEN OLD.status = 'todo'::procrastinate_job_status
                AND (
                    NEW.status = 'cancelled'::procrastinate_job_status
                    OR NEW.status = 'failed'::procrastinate_job_status
                    OR NEW.status = 'succeeded'::procrastinate_job_status
                )
                THEN 'cancelled'::procrastinate_job_event_type
            WHEN OLD.status = 'doing'::procrastinate_job_status
                AND NEW.status = 'aborted'::procrastinate_job_status
                THEN 'aborted'::procrastinate_job_event_type
            WHEN OLD.status = 'failed'::procrastinate_job_status
                AND NEW.status = 'todo'::procrastinate_job_status
                THEN 'retried'::procrastinate_job_event_type
            ELSE NULL
        END as event_type
    )
    INSERT INTO procrastinate_events(job_id, type)
        SELECT NEW.id, t.event_type
        FROM t
        WHERE t.event_type IS NOT NULL;
	RETURN NEW;
END;
$$;

CREATE FUNCTION procrastinate_trigger_function_scheduled_events_v1()
    RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO procrastinate_events(job_id, type, at)
        VALUES (NEW.id, 'scheduled'::procrastinate_job_event_type, NEW.scheduled_at);

	RETURN NEW;
END;
$$;

CREATE FUNCTION procrastinate_trigger_abort_requested_events_procedure_v1()
    RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO procrastinate_events(job_id, type)
        VALUES (NEW.id, 'abort_requested'::procrastinate_job_event_type);
    RETURN NEW;
END;
$$;

CREATE FUNCTION procrastinate_unlink_periodic_defers_v1()
    RETURNS trigger
    LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE procrastinate_periodic_defers
    SET job_id = NULL
    WHERE job_id = OLD.id;
    RETURN OLD;
END;
$$;

CREATE FUNCTION procrastinate_register_worker_v1()
    RETURNS TABLE(worker_id bigint)
    LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    INSERT INTO procrastinate_workers DEFAULT VALUES
    RETURNING procrastinate_workers.id;
END;
$$;

CREATE FUNCTION procrastinate_unregister_worker_v1(worker_id bigint)
    RETURNS void
    LANGUAGE plpgsql
AS $$
BEGIN
    DELETE FROM procrastinate_workers
    WHERE id = worker_id;
END;
$$;

CREATE FUNCTION procrastinate_update_heartbeat_v1(worker_id bigint)
    RETURNS void
    LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE procrastinate_workers
    SET last_heartbeat = NOW()
    WHERE id = worker_id;
END;
$$;

CREATE FUNCTION procrastinate_prune_stalled_workers_v1(seconds_since_heartbeat float)
    RETURNS TABLE(worker_id bigint)
    LANGUAGE plpgsql
AS $$
BEGIN
    RETURN QUERY
    DELETE FROM procrastinate_workers
    WHERE last_heartbeat < NOW() - (seconds_since_heartbeat || 'SECOND')::INTERVAL
    RETURNING procrastinate_workers.id;
END;
$$;

-- Triggers

CREATE TRIGGER procrastinate_jobs_notify_queue_job_inserted_v1
    AFTER INSERT ON procrastinate_jobs
    FOR EACH ROW WHEN ((new.status = 'todo'::procrastinate_job_status))
    EXECUTE PROCEDURE procrastinate_notify_queue_job_inserted_v1();

CREATE TRIGGER procrastinate_jobs_notify_queue_job_aborted_v1
    AFTER UPDATE OF abort_requested ON procrastinate_jobs
    FOR EACH ROW WHEN ((old.abort_requested = false AND new.abort_requested = true AND new.status = 'doing'::procrastinate_job_status))
    EXECUTE PROCEDURE procrastinate_notify_queue_abort_job_v1();

CREATE TRIGGER procrastinate_trigger_status_events_update_v1
    AFTER UPDATE OF status ON procrastinate_jobs
    FOR EACH ROW
    EXECUTE PROCEDURE procrastinate_trigger_function_status_events_update_v1();

CREATE TRIGGER procrastinate_trigger_status_events_insert_v1
    AFTER INSERT ON procrastinate_jobs
    FOR EACH ROW WHEN ((new.status = 'todo'::procrastinate_job_status))
    EXECUTE PROCEDURE procrastinate_trigger_function_status_events_insert_v1();

CREATE TRIGGER procrastinate_trigger_scheduled_events_v1
    AFTER UPDATE OR INSERT ON procrastinate_jobs
    FOR EACH ROW WHEN ((new.scheduled_at IS NOT NULL AND new.status = 'todo'::procrastinate_job_status))
    EXECUTE PROCEDURE procrastinate_trigger_function_scheduled_events_v1();

CREATE TRIGGER procrastinate_trigger_abort_requested_events_v1
    AFTER UPDATE OF abort_requested ON procrastinate_jobs
    FOR EACH ROW WHEN ((new.abort_requested = true))
    EXECUTE PROCEDURE procrastinate_trigger_abort_requested_events_procedure_v1();

CREATE TRIGGER procrastinate_trigger_delete_jobs_v1
    BEFORE DELETE ON procrastinate_jobs
    FOR EACH ROW EXECUTE PROCEDURE procrastinate_unlink_periodic_defers_v1();
"""


def bootstrap_pre_017_application_schema(database: Any, backend: str) -> None:
    """Create the frozen application schema used before numbered migrations."""

    statements = (
        PRE_017_POSTGRES_APPLICATION_SCHEMA_SQL
        if backend == "postgres"
        else PRE_017_SQLITE_APPLICATION_SCHEMA_SQL
    )
    for statement in statements:
        database.execute_sql(statement)
