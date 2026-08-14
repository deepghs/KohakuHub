---
title: Branches & Tags API
description: Branch/tag management, merge, revert, reset operations
icon: i-carbon-branch
---

# Branches & Tags API

Manage branches, tags, and advanced Git operations (merge, revert, reset).

---

## Repository Operation Gate

The history-mutating operations below are independently controlled by server-side
capabilities: `revert`, `reset`, and `squash`. The public capability state is
available without authentication from `GET /api/site-config`:

```json
{
  "capabilities": {
    "repository_operations": {
      "revert": false,
      "reset": false,
      "squash": false
    }
  }
}
```

Clients must treat an operation as enabled only when its value is the boolean
`true`. If the request fails, the field is missing, or the value has any other
type, clients must fail closed and hide the corresponding action.

When an operation is disabled, its API gate runs before authentication and
repository lookup. The server returns `503 Service Unavailable` with this
stable response shape:

```json
{
  "detail": {
    "code": "operation_disabled",
    "operation": "reset",
    "error": "Repository reset is temporarily disabled",
    "message": "Repository Reset is temporarily disabled"
  }
}
```

The `operation` value is one of `revert`, `reset`, or `squash`. Once enabled,
the normal authentication, permission, and repository validation requirements
still apply.

## Branches

### Create Branch

**Pattern:** `POST /api/{repo_type}s/{namespace}/{name}/branch`

**Authentication:** Required (delete permission)

**Request Body:**
```json
{
  "branch": "feature-branch",
  "revision": "main"
}
```

**Fields:**
- `branch`: New branch name (required)
- `revision`: Source revision (branch/commit, default: "main")

**Response:**
```json
{
  "success": true,
  "message": "Branch 'feature-branch' created"
}
```

**Status Codes:**
- `200 OK` - Branch created
- `404 Not Found` - Repository or source revision not found
- `409 Conflict` - Branch already exists

---

### Delete Branch

**Pattern:** `DELETE /api/{repo_type}s/{namespace}/{name}/branch/{branch}`

**Authentication:** Required (delete permission)

**Protection:** Cannot delete "main" branch

**Response:**
```json
{
  "success": true,
  "message": "Branch 'feature-branch' deleted"
}
```

**Status Codes:**
- `200 OK` - Branch deleted
- `400 Bad Request` - Attempting to delete main branch
- `404 Not Found` - Repository or branch not found

---

### Merge Branches

**Pattern:** `POST /api/{repo_type}s/{namespace}/{name}/merge/{source_ref}/into/{destination_branch}`

**Authentication:** Required (write permission)

**Request Body:**
```json
{
  "message": "Merge feature into main",
  "metadata": {
    "author": "alice"
  },
  "strategy": "dest-wins",
  "force": false,
  "allow_empty": false,
  "squash_merge": false
}
```

**Fields (all optional):**
- `message`: Commit message
- `metadata`: Additional metadata
- `strategy`: Conflict resolution
  - `"dest-wins"`: Destination wins on conflict
  - `"source-wins"`: Source wins on conflict
  - `null`: Fail on conflict (default)
- `force`: Force merge even with conflicts
- `allow_empty`: Allow merge with no changes
- `squash_merge`: Squash all commits into one

**Response:**
```json
{
  "success": true,
  "message": "Successfully merged feature into main",
  "result": {
    "reference": "commit_hash",
    "summary": {
      "added": 5,
      "removed": 2,
      "changed": 3
    }
  }
}
```

**Status Codes:**
- `200 OK` - Merged successfully
- `409 Conflict` - Merge conflict (use strategy to resolve)
- `500 Internal Server Error` - Merge failed

**Auto-tracking:**
- LFS objects tracked automatically
- Commit recorded in database
- User attribution preserved

---

### Revert Commit

**Pattern:** `POST /api/{repo_type}s/{namespace}/{name}/branch/{branch}/revert`

**Authentication:** Required (write permission)

**Purpose:** Create new commit that undoes changes from a specific commit

**Request Body:**
```json
{
  "ref": "commit_id_to_revert",
  "parent_number": 1,
  "message": "Revert: broken feature",
  "metadata": {
    "reason": "broke production"
  },
  "force": false,
  "allow_empty": false
}
```

**Fields:**
- `ref`: Commit ID or ref to revert (required)
- `parent_number`: For merge commits (default: 1)
- `message`: Commit message (optional)
- `metadata`: Additional metadata (optional)
- `force`: Force revert even with conflicts
- `allow_empty`: Allow empty revert

**Response:**
```json
{
  "success": true,
  "message": "Successfully reverted commit abc123de on branch 'main'",
  "new_commit_id": "new_commit_hash"
}
```

**Important Notes:**
- Creates NEW commit (doesn't delete history)
- LFS files automatically handled (no recoverability check needed)
- If conflicts: returns 409 error

**Status Codes:**
- `200 OK` - Reverted successfully
- `503 Service Unavailable` - Revert operation is disabled by server policy
- `404 Not Found` - Commit not found
- `409 Conflict` - Revert caused conflicts

---

### Reset Branch

**Pattern:** `POST /api/{repo_type}s/{namespace}/{name}/branch/{branch}/reset`

**Authentication:** Required (write permission)

**Purpose:** Reset branch to a specific commit (like `git reset --hard`)

**Request Body:**
```json
{
  "ref": "commit_id_to_reset_to",
  "message": "Reset to stable version",
  "force": false
}
```

**Fields:**
- `ref`: Commit ID or ref to reset to (required)
- `message`: Commit message (optional)
- `force`: Skip LFS recoverability check

**Safety Checks:**
1. Main branch requires `force: true`
2. LFS file availability checked (unless `force: true`)
3. Creates new commit (preserves history)

**Response:**
```json
{
  "success": true,
  "message": "Successfully reset branch 'main' to commit abc123de (new commit created)",
  "commit_id": "new_commit_hash"
}
```

**LFS Recoverability Check:**
- Validates all LFS files from target commit still exist
- Checks all commits between target and HEAD
- If files garbage-collected: returns 400 error

**Error if LFS unavailable:**
```json
{
  "error": "Cannot reset to commit abc123: 5 LFS file(s) across 3 commit(s) have been garbage collected",
  "missing_files": ["model.safetensors", "data.bin"],
  "affected_commits": ["commit1", "commit2", "commit3"],
  "recoverable": false
}
```

**Use `force: true` to bypass check (may break LFS refs)**

**Status Codes:**
- `200 OK` - Reset successful
- `503 Service Unavailable` - Reset operation is disabled by server policy
- `400 Bad Request` - LFS files not recoverable or main branch without force
- `404 Not Found` - Commit not found

**Implementation Details:**
- Uses diff-based approach (not destructive)
- Restores files from target commit
- Syncs File table after reset
- Records reset commit in database

---

## Tags

### Create Tag

**Pattern:** `POST /api/{repo_type}s/{namespace}/{name}/tag`

**Authentication:** Required (delete permission)

**Request Body:**
```json
{
  "tag": "v1.0.0",
  "revision": "main",
  "message": "Release version 1.0"
}
```

**Fields:**
- `tag`: Tag name (required)
- `revision`: Source revision (default: "main")
- `message`: Tag message (optional)

**Response:**
```json
{
  "success": true,
  "message": "Tag 'v1.0.0' created"
}
```

**Status Codes:**
- `200 OK` - Tag created
- `404 Not Found` - Repository or revision not found
- `500 Internal Server Error` - Failed to create tag

---

### Delete Tag

**Pattern:** `DELETE /api/{repo_type}s/{namespace}/{name}/tag/{tag}`

**Authentication:** Required (delete permission)

**Response:**
```json
{
  "success": true,
  "message": "Tag 'v1.0.0' deleted"
}
```

**Status Codes:**
- `200 OK` - Tag deleted
- `404 Not Found` - Repository or tag not found
- `500 Internal Server Error` - Failed to delete tag

---

## Usage Examples

### Merge Feature Branch

```python
import requests

API_BASE = "http://localhost:28080/api"
TOKEN = "your_token"
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

# Merge feature into main
response = requests.post(
    f"{API_BASE}/models/username/repo/merge/feature-branch/into/main",
    json={
        "message": "Merge feature: Add new model",
        "strategy": "dest-wins"  # Main wins on conflicts
    },
    headers=HEADERS
)

result = response.json()
print(f"Merged: {result['message']}")
print(f"Commit: {result['result']['reference']}")
```

---

### Revert Bad Commit

```python
# Find the bad commit
commits_resp = requests.get(
    f"{API_BASE}/models/username/repo/commits/main?limit=10",
    headers=HEADERS
)
commits = commits_resp.json()["commits"]
bad_commit = commits[0]["id"]  # Latest commit

# Revert it
revert_resp = requests.post(
    f"{API_BASE}/models/username/repo/branch/main/revert",
    json={
        "ref": bad_commit,
        "message": "Revert: broken model weights"
    },
    headers=HEADERS
)

result = revert_resp.json()
print(f"Reverted: {result['new_commit_id']}")
```

---

### Reset to Previous Version

```python
# Reset to 3 commits ago
commits_resp = requests.get(
    f"{API_BASE}/models/username/repo/commits/main?limit=5",
    headers=HEADERS
)
commits = commits_resp.json()["commits"]
target_commit = commits[3]["id"]  # 3 commits back

# Reset (with LFS check)
reset_resp = requests.post(
    f"{API_BASE}/models/username/repo/branch/main/reset",
    json={
        "ref": target_commit,
        "message": "Reset to stable version",
        "force": False  # Check LFS availability
    },
    headers=HEADERS
)

if reset_resp.status_code == 400:
    # LFS files not available
    error = reset_resp.json()
    print(f"Cannot reset: {error['error']}")
    print(f"Missing files: {error['missing_files']}")

    # Force reset anyway (may break)
    reset_resp = requests.post(
        f"{API_BASE}/models/username/repo/branch/main/reset",
        json={"ref": target_commit, "force": True},
        headers=HEADERS
    )

result = reset_resp.json()
print(f"Reset to: {result['commit_id']}")
```

---

### Create Release Tag

```python
# Tag current main branch
tag_resp = requests.post(
    f"{API_BASE}/models/username/repo/tag",
    json={
        "tag": "v1.0.0",
        "revision": "main",
        "message": "Release version 1.0 - Production ready"
    },
    headers=HEADERS
)

result = tag_resp.json()
print(result["message"])

# Download specific version
file_url = f"{API_BASE}/models/username/repo/resolve/v1.0.0/model.safetensors"
```

---

## Git Command Equivalents

### Branches

```bash
# Create branch
POST /branch {"branch": "feature", "revision": "main"}
# git checkout -b feature main

# Delete branch
DELETE /branch/feature
# git branch -d feature

# Merge
POST /merge/feature/into/main {"strategy": "dest-wins"}
# git checkout main && git merge feature -X theirs
```

### Tags

```bash
# Create tag
POST /tag {"tag": "v1.0", "revision": "main"}
# git tag v1.0 main

# Delete tag
DELETE /tag/v1.0
# git tag -d v1.0
```

### Advanced Operations

```bash
# Revert
POST /branch/main/revert {"ref": "abc123"}
# git revert abc123

# Reset
POST /branch/main/reset {"ref": "abc123"}
# git reset --hard abc123 (but creates new commit)
```

---

## Best Practices

**Branching Strategy:**
- Use `main` for production
- Create feature branches for development
- Merge with `dest-wins` to protect main
- Tag releases for versioning

**LFS Considerations:**
- Reset checks LFS availability (prevent broken refs)
- Use `force: true` only if you understand the risk
- Keep `lfs_keep_versions` high enough for your workflow
- Monitor storage after resets/reverts

**Safety:**
- Always use `force: false` first (see errors)
- Test merges on feature branches first
- Tag before risky operations (easy rollback)
- Document reset/revert reasons in commit message

---

## Next Steps

- [Commits API](./commits.md) - View commit history
- [File Upload API](./file-upload.md) - Commit files
- [Repositories API](./repositories.md) - Repo management
