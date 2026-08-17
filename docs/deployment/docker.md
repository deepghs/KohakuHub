---
title: Docker Deployment
description: Complete Docker Compose setup guide for KohakuHub.
icon: i-carbon-container-services
---

# Docker Deployment

This guide provides a complete walkthrough for deploying KohakuHub using Docker Compose.

## Quick Deploy (Recommended)

This is the easiest and fastest way to get KohakuHub running.

### 1. Clone the Repository

```bash
git clone https://github.com/KohakuBlueleaf/KohakuHub.git
cd KohakuHub
```

### 2. Configure and Deploy

First, run the interactive script to generate your `docker-compose.yml` file. This will guide you through setting up the database, storage, and other services.

```bash
python scripts/generate_docker_compose.py
```

Then, run the deployment script. This will automatically build the frontend applications and start all the Docker services.

```bash
python scripts/deploy.py
```

That's it! The application is now running.

## Step-by-Step Setup

If you need more control over the setup process, you can follow these manual steps.

### 1. Clone the Repository

```bash
git clone https://github.com/KohakuBlueleaf/KohakuHub.git
cd KohakuHub
```

### 2. Generate `docker-compose.yml`

Copy the example file and manually edit it to fit your environment.

```bash
cp docker-compose.example.yml docker-compose.yml
```

### 3. Build the Frontend

Before starting the services, you need to build the frontend applications:

```bash
pnpm install
pnpm run build
```

### 4. Start the Services

To start all services in detached mode, run:

```bash
docker-compose up -d --build
```

## Security Configuration

It is **critical** to change the default secrets before deploying to production.

### Generate Secret Keys

You can generate secure random strings for your secrets using the following commands:

```bash
# Generate a 64-character random string for session and admin tokens
python scripts/generate_secret.py 64

# Or use openssl
openssl rand -base64 48
```

Update the following variables in your `docker-compose.yml` with the generated secrets:

- `KOHAKU_HUB_SESSION_SECRET`
- `KOHAKU_HUB_ADMIN_SECRET_TOKEN`
- `LAKEFS_AUTH_ENCRYPT_SECRET_KEY`

## Services

The Docker Compose setup includes the following services:

- **hub-ui**: Nginx server for the frontend application (port `28080`).
- **hub-api**: The main FastAPI backend (port `48888`).
- **khub-migrate**: One-shot PostgreSQL schema owner. It must finish before
  the API or worker starts.
- **khub-worker**: Durable PostgreSQL-backed background task service. It
  contains the control, sync, bulk, and cleanup queues in one process and
  exposes metrics on port `9108`.
- **postgres**: PostgreSQL database for metadata (port `5432`).
- **lakefs**: LakeFS for data versioning (port `28000`). Pinned to
  `treeverse/lakefs:latest`; **minimum supported LakeFS is v0.54.0**
  (2021-11-08) because the file-list `expand=true` path uses
  `logCommits`'s `objects=` / `prefixes=` / `limit=` filters introduced
  in that release.
- **minio**: MinIO for S3-compatible object storage (ports `29000` and `29001`).

### Worker Deployment Contract

The operation ledger and Procrastinate jobs are authoritative PostgreSQL
state. API enqueue and the first job insert commit in one transaction; the
worker does not receive executable code or task credentials over HTTP.

Schema release is migration-first. For a version change, drain or stop the
old API and worker, run `khub-migrate` to completion, then start the new
services. The current readiness checks intentionally reject incomplete or
incompatible durable schemas. The Compose `depends_on` ordering only covers
cold startup; it does not coordinate an already-running deployment.

The worker endpoints are:

- `GET /healthz`: process liveness.
- `GET /readyz`: both worker lanes have registered and passed schema checks.
- `GET /metrics`: queue depth/age, operation backlog, reconciliation backlog,
  retry/stall counters, event-loop lag, RSS, and database pool pressure.

Keep the worker metrics endpoint on a private network in production. Queue
depth and age are the first signals for distinguishing a slow upstream from
an unavailable worker or an exhausted database connection pool.

Terminal operation history is retained for 168 hours by default and pruned in
batches of 100 during reconciliation. Configure
`KOHAKU_HUB_OPERATION_RETENTION_HOURS` and
`KOHAKU_HUB_OPERATION_RETENTION_BATCH` for a different policy. The reaper only
removes fully terminal operations, steps, and terminal Procrastinate jobs. It
never removes an operation or step in `dispatch_started`, `uncertain`, or
`cleanup_pending`, or one with an unresolved commit intent.

## Managing the Application

### View Logs

To view the logs for all services, use:

```bash
docker-compose logs -f
```

To view the logs for a specific service, use:

```bash
docker-compose logs -f hub-api
```

### Stop the Services

To stop all running services, use:

```bash
docker-compose down
```

## Accessing the Application

- **Web UI**: `http://localhost:28080`
- **Admin Portal**: `http://localhost:28080/admin`
- **API Docs**: `http://localhost:48888/docs`
- **LakeFS UI**: `http://localhost:28000`
- **MinIO Console**: `http://localhost:29000`
