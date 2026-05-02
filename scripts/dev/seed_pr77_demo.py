"""Idempotent seed for the PR #77 strict-consistency demos.

Run after ``make seed-demo`` against a dev backend that has the
default fallback config (single source pointing at
``https://huggingface.co``). Creates a small set of repos whose
``(namespace, name)`` collides with real HuggingFace repos but whose
content is intentionally **structurally different**, so that a few
``huggingface_hub`` calls are enough to demonstrate every major
contract this PR locks down.

Usage:

    # Make sure the dev backend is up (make backend) and seeded
    # (make seed-demo) first. Then:
    PYTHONPATH=src python scripts/dev/seed_pr77_demo.py

The script is idempotent — re-running it picks up where it left off
without 409s. After running, follow the verification commands in the
README of each created repo (returned by ``HfApi.list_repo_files``
or visible in the SPA at ``/{namespace}/{name}``).

Each demo repo's ``README.md`` includes:
  - what the repo is for (which contract it demonstrates)
  - expected behavior under PR #77 (specific ``huggingface_hub``
    calls and the responses they should produce)
  - the counterfactual (what the buggy pre-fix behavior would have
    looked like for the same calls)

The repos are designed to be harmless if accessed in production:
they're tiny and clearly marked as demos.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import requests

KHUB = "http://127.0.0.1:48888"
ADMIN_TOKEN = "dev-admin-token-change-me"
SEED_USER = "mai_lin"  # super-admin of aurora-labs in seed-demo
SEED_PWD = "KohakuDev123!"


# ---------------------------------------------------------------------------
# Demo repo definitions. Each one collides on path with a real HF repo
# but holds completely different content here.
# ---------------------------------------------------------------------------


DEMO_REPOS: list[dict] = [
    {
        "org": "openai-community",
        "org_description": (
            "Local-only org demonstrating PR #77 namespace-priority. "
            "The same name exists on HuggingFace; this khub instance owns it locally."
        ),
        "repo_name": "gpt2",
        "repo_type": "model",
        "files": [
            ("README.md", """# PR #77 demo: openai-community/gpt2 (LOCAL)

This is a **local-only** repository at `openai-community/gpt2` on this
KohakuHub instance. The same path exists on huggingface.co (the famous
GPT-2 with weights, vocab files, tokenizer, etc.). This local repo is
**structurally different**: it contains only this README — no model
weights, no `config.json`, no tokenizer, no anything else.

## What this repo demonstrates

The dev backend has a single fallback source pointing at
`https://huggingface.co`. With strict consistency (PR #77) the rule is:
**the local namespace owns its name**. Even when the requested resource
exists on HF, a local repo at the same `(namespace, name)` is the only
place the response can come from.

## Expected behavior under PR #77

Run from a Python shell with `huggingface_hub` and a khub token:

```python
from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.errors import EntryNotFoundError, RevisionNotFoundError

api = HfApi(endpoint="http://127.0.0.1:48888", token="<your-khub-token>")

# (a) info / list_repo_files: LOCAL data wins
api.model_info("openai-community/gpt2")
# → ModelInfo with this local repo's sha (NOT HF's sha)

api.list_repo_files("openai-community/gpt2")
# → ['README.md']  (NOT HF's 26-file list)

# (b) Download README.md — gets LOCAL bytes
import tempfile
with tempfile.TemporaryDirectory() as tmp:
    p = hf_hub_download(
        "openai-community/gpt2", "README.md",
        endpoint="http://127.0.0.1:48888",
        token="<your-khub-token>",
        cache_dir=tmp,
    )
    body = open(p).read()
    assert "PR #77 demo" in body, "FAIL: client got HF's README, not local"
    print("OK — local README served")

# (c) Download config.json — must raise EntryNotFoundError
try:
    hf_hub_download(
        "openai-community/gpt2", "config.json",
        endpoint="http://127.0.0.1:48888",
        token="<your-khub-token>",
    )
    print("FAIL: client got HF's config.json (cross-source mixing)")
except EntryNotFoundError:
    print("OK — EntryNotFoundError; chain did NOT walk to HF")

# (d) Non-existent revision: must raise RevisionNotFoundError
try:
    hf_hub_download(
        "openai-community/gpt2", "README.md",
        revision="some-bogus-branch",
        endpoint="http://127.0.0.1:48888",
        token="<your-khub-token>",
    )
    print("FAIL: chain fell through to HF on missing revision")
except (RevisionNotFoundError, EntryNotFoundError):
    # hf_hub_download wraps revision miss into either class depending
    # on version; both indicate local short-circuit.
    print("OK — local handler short-circuited the chain")
```

## Wire-level signal

A direct HEAD also works:

```bash
$ curl -i http://127.0.0.1:48888/openai-community/gpt2/resolve/main/config.json
HTTP/1.1 404 Not Found
X-Error-Code: EntryNotFound
X-Error-Message: Entry 'config.json' not found in repository 'openai-community/gpt2' at revision 'main'
# Note: no X-Source-Count / X-Source / X-Source-URL headers — the
# fallback chain did not run. This is byte-equivalent to the same
# request with `?fallback=false` (verified in PR #77 comments).
```

## Pre-fix counterfactual

Before PR #77, requests (c)/(d) would have:
  1. Local handler returns 404 + X-Error-Code: EntryNotFound
  2. The decorator's "any 404 → run fallback" rule discards the local
     signal and probes the chain.
  3. HF has openai-community/gpt2/config.json → 307 + bytes.
  4. khub serves HF's bytes for what the client thinks is `openai-community/gpt2`.
  5. Client cache now has HF's GPT-2 config under this `repo_id` — but the
     local `(namespace, name)` is a *different* repo. Two distinct repos
     collapsed into one — exactly the cross-source mixing PR #77 fixes.
"""),
        ],
    },
    {
        "org": "bigscience",
        "org_description": (
            "Local-only org demonstrating PR #77 chain-level short-circuit. "
            "Same name as the HuggingFace org; local content is unrelated."
        ),
        "repo_name": "bloom",
        "repo_type": "model",
        "files": [
            ("README.md", """# PR #77 demo: bigscience/bloom (LOCAL)

This is a **local-only** repository at `bigscience/bloom` on this
KohakuHub instance. HF has the actual BLOOM 176B model at the same
path. This local repo is intentionally minimal — README + a single
small text file.

## What this repo demonstrates

Sister of `openai-community/gpt2`. Same contract, different namespace,
to show the rule isn't tied to one specific repo path: **any** local
repo at `(local-org-name, local-repo-name)` short-circuits the
fallback chain on its own EntryNotFound / RevisionNotFound, even when
HF has a same-named repo with the requested file.

## Expected behavior under PR #77

```python
from huggingface_hub import HfApi
api = HfApi(endpoint="http://127.0.0.1:48888", token="<your-khub-token>")

api.model_info("bigscience/bloom")
# → tiny local ModelInfo, NOT HF's 176B-parameter BLOOM

api.list_repo_files("bigscience/bloom")
# → ['README.md', 'demo-note.txt']

# Asking for a real BLOOM file: must short-circuit to local 404
import tempfile
from huggingface_hub import hf_hub_download
from huggingface_hub.errors import EntryNotFoundError

try:
    with tempfile.TemporaryDirectory() as tmp:
        hf_hub_download(
            "bigscience/bloom", "config.json",
            endpoint="http://127.0.0.1:48888",
            token="<your-khub-token>",
            cache_dir=tmp,
        )
    print("FAIL: client got HF's bloom config")
except EntryNotFoundError:
    print("OK — local short-circuit; HF's bloom never asked")
```

## Pre-fix counterfactual

Pre-#77 the request would have walked to HF and downloaded HF's
actual BLOOM `config.json` (~660 bytes for the 176B model header).
On systems that pre-cache file-by-file (e.g., `snapshot_download`)
this would mean a `bigscience/bloom` directory on the user's
machine slowly accumulating HF's BLOOM bytes one file at a time —
attributed to this khub's local repo. The cross-source mixing
defeats any meaningful "I'm pinning to my local mirror" guarantee.
"""),
            ("demo-note.txt", """This file proves the local repo has
non-trivial content. The PR #77 contract says: a request for any of
the *real* HF BLOOM files (config.json, tokenizer.json, model
shards, etc.) against this khub MUST raise EntryNotFoundError, NOT
silently download HF's bytes.
"""),
        ],
    },
    {
        "org": "meta-llama",
        "org_description": (
            "Local-only org demonstrating PR #77 against an HF-gated upstream. "
            "Same name as the (gated) HuggingFace org; local content is unrelated."
        ),
        "repo_name": "Llama-2-7b",
        "repo_type": "model",
        "files": [
            ("README.md", """# PR #77 demo: meta-llama/Llama-2-7b (LOCAL, with gated HF sibling)

This is a **local-only** repository at `meta-llama/Llama-2-7b`. The
same path on huggingface.co is a *gated* repository — anonymous
callers get `401 + X-Error-Code: GatedRepo`, even just to read the
file metadata.

This local repo is intentionally minimal — README only. The fallback
config on this khub points at HuggingFace anonymously, so a chain
probe for any file under `meta-llama/Llama-2-7b` would land on
HF and get gated.

## What this repo demonstrates

The strict-consistency contract holds even against a *gated*
upstream: if the local namespace owns the name, the local handler is
authoritative. The gated HF response never reaches the client — not
as content, not as a misleading `GatedRepoError`, not as anything.
The client sees only what the local repo has (or doesn't).

## Expected behavior under PR #77

```python
from huggingface_hub import HfApi, hf_hub_download
from huggingface_hub.errors import EntryNotFoundError, GatedRepoError

api = HfApi(endpoint="http://127.0.0.1:48888", token="<your-khub-token>")

# (a) Repo exists locally — info / tree succeed locally
api.model_info("meta-llama/Llama-2-7b")
# -> ModelInfo with local sha
api.list_repo_files("meta-llama/Llama-2-7b")
# -> ['README.md']

# (b) Download README — local bytes
import tempfile
with tempfile.TemporaryDirectory() as tmp:
    p = hf_hub_download(
        "meta-llama/Llama-2-7b", "README.md",
        endpoint="http://127.0.0.1:48888",
        token="<your-khub-token>",
        cache_dir=tmp,
    )
    body = open(p).read()
    assert "PR #77 demo" in body
    print("OK - local README served")

# (c) Request `config.json` (HF gates this; local doesn't have it)
#     -> EntryNotFoundError (NOT GatedRepoError, NOT a silent download)
try:
    hf_hub_download(
        "meta-llama/Llama-2-7b", "config.json",
        endpoint="http://127.0.0.1:48888",
        token="<your-khub-token>",
    )
    print("FAIL: chain walked to HF and got gated/served something")
except EntryNotFoundError:
    print("OK - local short-circuit; HF's gated 401 was never seen")
except GatedRepoError:
    print("FAIL: GatedRepoError surfaced - chain ran past local")
```

## Wire-level signal

```bash
$ curl -i http://127.0.0.1:48888/meta-llama/Llama-2-7b/resolve/main/config.json
HTTP/1.1 404 Not Found
x-error-code: EntryNotFound
x-error-message: Entry 'config.json' not found in repository 'meta-llama/Llama-2-7b' at revision 'main'
# Note: no X-Source-Count, no X-Error-Code: GatedRepo, no upstream
# 401 surfacing. The local namespace owns this name; HF's gating is
# entirely irrelevant.
```

## Pre-fix counterfactual

Pre-#75 the chain would have run:
  1. Local 404 (file not present locally)
  2. Decorator triggers fallback chain regardless of local X-Error-Code
  3. HF responds `401 + X-Error-Code: GatedRepo` (anonymous can't access gated)
  4. Aggregate (single source) -> `401 + GatedRepo` -> hf_hub raises `GatedRepoError`
  5. Client sees `GatedRepoError` for a file in a *local* repo that has no
     such file. The error is misleading — it suggests the user lacks
     access to *their own* repo. Cross-source semantics blur the meaning
     of every error class.

PR #77 ensures the local handler's `EntryNotFound` is what the client
sees, with the local repo identified in the message.

## When you DO want HF's gated content

Just don't have a local collision. Anonymous callers will get
`GatedRepoError` for a real-HF gated repo if there's no local repo at
the same path; supply a HF token via `Authorization: Bearer <hf-token>`
or configure the source's admin token to reach the gated content.
The pure-fallback gated case is documented in the
`narugo1992-pr77-demo/guide` README on this same khub.
"""),
        ],
    },
    {
        "org": "narugo1992-pr77-demo",
        "org_description": (
            "PR #77 demo org — fallback-chain pure-pass-through case. "
            "Repos referenced here intentionally don't exist locally."
        ),
        "repo_name": "guide",
        "repo_type": "model",
        "files": [
            ("README.md", """# PR #77 demo: pure fallback (no local collision)

This repo is a *guide* — no namespace collision with HF. It exists
locally only to host these instructions. The interesting cases are
the repos under HuggingFace orgs that this khub does NOT have
locally, and what `huggingface_hub` calls against them produce.

## Pure-fallback path: HF has it, local doesn't

```python
from huggingface_hub import HfApi, hf_hub_download
api = HfApi(endpoint="http://127.0.0.1:48888", token="<your-khub-token>")

# google-bert/bert-base-uncased exists on HF; not on this khub.
api.model_info("google-bert/bert-base-uncased")
# → succeeds via fallback; ModelInfo._source = 'HuggingFace'

import tempfile
with tempfile.TemporaryDirectory() as tmp:
    p = hf_hub_download(
        "google-bert/bert-base-uncased", "config.json",
        endpoint="http://127.0.0.1:48888",
        token="<your-khub-token>",
        cache_dir=tmp,
    )
    print(f"OK — {open(p).read()[:80]}...")  # HF's BERT config
```

## Genuinely missing path

```python
from huggingface_hub.errors import RepositoryNotFoundError

try:
    api.model_info("narugo1992-pr77-demo/this-doesnt-exist-anywhere")
    print("FAIL: returned data for non-existent repo")
except RepositoryNotFoundError:
    print("OK - RepositoryNotFoundError aggregated from chain")
```

The chain probed HF (only configured source), got HF's anti-enum 401
(or 404+RepoNotFound for authed callers), aggregated to
``RepoNotFound``, and the client raises ``RepositoryNotFoundError``.

## Gated upstream, no local collision

When local doesn't have the repo and HF gates it, the chain's gated
signal *does* reach the client (as it should — there's no local
namespace to override it). Anonymous fallback to a real-HF gated
repo:

```python
from huggingface_hub.errors import GatedRepoError

try:
    hf_hub_download(
        "meta-llama/Llama-2-70b",  # really exists on HF, gated
        "config.json",
        endpoint="http://127.0.0.1:48888",
        token="<your-khub-token>",
    )
    print("FAIL: anonymous fallback bypassed HF gating")
except GatedRepoError:
    print("OK - HF's gated response propagated through the aggregate")
```

For comparison, the contrasting case where the *local* khub has a
repo at a same path as a gated HF repo, see
``meta-llama/Llama-2-7b`` on this same khub: even with HF gated and
the file present-on-HF, the client receives the *local*
``EntryNotFound`` — never HF's gated signal.

## Wire-level inspection

For any of the above, raw curl can inspect the path-level signals:

```bash
# Pure fallback, success: X-Source headers attached by chain
curl -I http://127.0.0.1:48888/google-bert/bert-base-uncased/resolve/main/config.json
# → HTTP/1.1 307 Temporary Redirect
#   Location: https://huggingface.co/...
#   X-Source: HuggingFace
#   X-Source-URL: https://huggingface.co
#   X-Source-Status: 307

# Genuinely missing repo: aggregate response
curl -I http://127.0.0.1:48888/narugo1992-pr77-demo/this-doesnt-exist-anywhere/resolve/main/file
# → HTTP/1.1 404 Not Found
#   X-Error-Code: RepoNotFound
#   X-Source-Count: 1
```

## Cross-reference

  - Local-namespace-priority + EntryNotFound/RevisionNotFound short-circuit:
    see `openai-community/gpt2` and `bigscience/bloom` on this same khub.
  - All chain-level decision combinations are exhaustively unit-tested
    in `test/kohakuhub/api/fallback/test_chain_enumeration.py` (4368 cases).
  - Every contract here corresponds to a row of the matrix table in
    PR #77 / issue #75.
"""),
        ],
    },
]


# ---------------------------------------------------------------------------
# API helpers
# ---------------------------------------------------------------------------


def _login() -> tuple[requests.cookies.RequestsCookieJar, str]:
    cookies = requests.post(
        f"{KHUB}/api/auth/login",
        json={"username": SEED_USER, "password": SEED_PWD},
        timeout=10,
    ).cookies
    token = requests.post(
        f"{KHUB}/api/auth/tokens/create",
        json={"name": "pr77-demo-seeder"},
        cookies=cookies,
        timeout=10,
    ).json().get("token")
    if not token:
        raise RuntimeError(
            f"Could not log in as {SEED_USER!r}. Make sure `make seed-demo` "
            f"has been run against this backend."
        )
    return cookies, token


def _ensure_org(cookies, org: str, description: str) -> None:
    r = requests.post(
        f"{KHUB}/org/create",
        json={"name": org, "description": description},
        cookies=cookies,
        timeout=10,
    )
    if r.status_code in (200, 201):
        print(f"  [+] created org {org!r}")
    elif r.status_code == 400 and "already exists" in r.text:
        print(f"  [=] org {org!r} already exists")
    else:
        raise RuntimeError(f"create-org {org!r} failed: {r.status_code} {r.text}")


def _ensure_repo(cookies, org: str, name: str, repo_type: str) -> None:
    r = requests.post(
        f"{KHUB}/api/repos/create",
        json={"type": repo_type, "name": name, "organization": org, "private": False},
        cookies=cookies,
        timeout=15,
    )
    if r.status_code in (200, 201):
        print(f"  [+] created repo {org}/{name} ({repo_type})")
    elif r.status_code == 409:
        print(f"  [=] repo {org}/{name} already exists")
    elif r.status_code == 500 and "already exists" in r.text.lower():
        print(f"  [=] repo {org}/{name} already exists (500 from idempotency)")
    else:
        raise RuntimeError(f"create-repo {org}/{name} failed: {r.status_code} {r.text}")


def _existing_file_size(api, repo_id: str, path: str) -> int | None:
    """Return the existing file's size in bytes, or ``None`` if absent.
    Used to make the seed idempotent on content equality, not just on
    path presence — so a script-side README rewrite re-uploads cleanly
    on next run."""
    try:
        for it in api.list_repo_tree(repo_id, repo_type="model", recursive=True):
            if getattr(it, "path", None) == path:
                return int(getattr(it, "size", 0) or 0)
    except Exception:
        return None
    return None


def _upload_file(api, repo_id: str, path: str, content) -> None:
    if isinstance(content, str):
        content = content.encode("utf-8")
    api.upload_file(
        path_or_fileobj=content,
        path_in_repo=path,
        repo_id=repo_id,
        repo_type="model",
        commit_message=f"PR #77 demo seed: add {path}",
    )


def main() -> int:
    try:
        from huggingface_hub import HfApi
    except ImportError:
        print("ERROR: huggingface_hub is not installed in this env.", file=sys.stderr)
        return 1

    print(f"[seed-pr77-demo] target: {KHUB}")
    try:
        cookies, token = _login()
    except Exception as e:
        print(f"ERROR: login failed — {e}", file=sys.stderr)
        return 1

    api = HfApi(endpoint=KHUB, token=token)

    for spec in DEMO_REPOS:
        org = spec["org"]
        repo_name = spec["repo_name"]
        repo_type = spec["repo_type"]
        repo_id = f"{org}/{repo_name}"

        print(f"\n[{repo_id}] ({spec['repo_type']})")
        _ensure_org(cookies, org, spec["org_description"])
        _ensure_repo(cookies, org, repo_name, repo_type)

        for path, content in spec["files"]:
            content_bytes = content.encode("utf-8") if isinstance(content, str) else content
            existing_size = _existing_file_size(api, repo_id, path)
            if existing_size == len(content_bytes):
                print(f"  [=] {repo_id}:{path} present + size matches ({existing_size}B)")
                continue
            try:
                _upload_file(api, repo_id, path, content_bytes)
                tag = "uploaded" if existing_size is None else "rewrote"
                print(f"  [+] {tag} {repo_id}:{path} ({len(content_bytes)}B)")
            except Exception as e:
                # Print but keep going so partial failures don't block.
                print(f"  [!] upload {repo_id}:{path} failed: {type(e).__name__}: {e}")

    print()
    print("[seed-pr77-demo] done. Verification commands are in each repo's README.md.")
    print("[seed-pr77-demo] List of demo repos:")
    for spec in DEMO_REPOS:
        print(f"  - {spec['org']}/{spec['repo_name']}  ({spec['repo_type']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
