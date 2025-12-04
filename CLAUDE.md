# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This project implements data pipelines for the MODERATE project using Dagster as the workflow orchestration service. Pipelines run on a Kubernetes cluster using the Dagster Kubernetes integration. The system integrates with multiple services including Keycloak (identity), PostgreSQL (database), OpenMetadata (data catalog), S3-compatible object storage, RabbitMQ, and the MODERATE platform API.

## Core Architecture

### Dagster Code Organization

The main Dagster definitions are in `moderate/moderate/__init__.py:22-83`, which loads:

- **Assets**: Data artifacts defined in `moderate.assets`, `moderate.openmetadata.assets`, `moderate.trust`, and `moderate.datasets`
- **Resources**: Configurable services (Keycloak, Postgres, OpenMetadata, S3, Platform API, RabbitMQ) defined in `moderate/moderate/resources.py`
- **Jobs**: Runnable workflows including metadata ingestion, trust service propagation, and matrix profile jobs
- **Sensors**: Event-driven triggers monitoring Keycloak users, platform API asset objects, and RabbitMQ messages

### Resource Pattern

All external service integrations use Dagster's `ConfigurableResource` pattern defined in `resources.py`:

- Configuration is passed via environment variables using `EnvVar`
- Resources are injected into ops/assets via dependency injection
- Connection details and credentials are managed centrally

### Key Modules

- **openmetadata/**: Metadata and profiler workflow ingestion for Postgres and S3 datalake
- **trust/**: Integration with trust services for user DID generation and asset object proof creation
- **datasets/**: Data pipeline definitions for genome project and building stock datasets
- **matrix_profile/**: K8s job execution for matrix profile analysis workflows

## Development Commands

### Local Development with Dagster Dev

Start the local development server (requires dependency services):

```bash
task dagster-dev
```

This will:

1. Start the Compose stack with Postgres, Keycloak, and Elasticsearch
2. Create a Python virtualenv at `.venv`
3. Install the `moderate` package in editable mode with dev dependencies
4. Launch Dagster UI at http://localhost:3000

### Kubernetes-Based Local Development

Deploy a full local K8s environment with Minikube:

```bash
task start-dev-k8s
```

This orchestrates:

1. Starting Minikube cluster
2. Building and pushing Docker image to local registry
3. Starting dependency services via Docker Compose
4. Installing Dagster and OpenMetadata Helm charts
5. Waiting for all pods to be ready

Access UIs:

```bash
task forward-k8s-dagster-ui      # Dagster at http://<your-ip>:8181
task forward-k8s-open-metadata-ui # OpenMetadata at http://<your-ip>:8585
```

**Important**: Access UIs using your local IP address (not localhost) due to OpenMetadata OIDC integration with Keycloak.

### OpenMetadata Token Update

After deploying locally, update the OpenMetadata integration token:

1. Go to OpenMetadata UI → Settings → Bots → ingestion-bot
2. Revoke and create new token
3. Run: `OPEN_METADATA_TOKEN="<token>" task update-k8s-open-metadata-token`

### Code Changes and Restart

Force restart Dagster deployment with updated code:

```bash
task force-restart-k8s-dagster
```

### Testing

Run tests from the moderate package:

```bash
cd moderate && pytest moderate_tests
```

### Clean Development Environment

Remove all local K8s and Docker resources:

```bash
task clean
```

## Environment Configuration

Create a `.env` file (not committed) with required service credentials:

```bash
# Platform API
API_BASE_URL=http://localhost:9080
API_USERNAME=<username>
API_PASSWORD=<password>

# S3-compatible storage
S3_ACCESS_KEY_ID=<key>
S3_SECRET_ACCESS_KEY=<secret>
S3_REGION=europe-west1
S3_BUCKET_NAME=<bucket>
S3_ENDPOINT_URL=https://storage.googleapis.com

# RabbitMQ
RABBIT_URL=amqp://guest:guest@localhost:5672

# Matrix Profile Job
MATRIX_PROFILE_JOB_IMAGE=<image>
```

Dependencies on these services are optional; pipelines not requiring them will still run.

## Python Package Management

The Dagster code location is defined in `moderate/setup.py`:

- Dependencies: Dagster 1.8.7, postgres/k8s extensions, pandas, keycloak, openmetadata-ingestion, boto3, pika, pydantic
- Dev dependencies: dagster-webserver, pytest, black
- Install: `pip install -e "moderate[dev]"`

## Key Dagster Patterns

### Dynamic Ops for Parallel Execution

OpenMetadata ingestion uses dynamic mapping to process databases in parallel (see `moderate/moderate/openmetadata/assets.py:41-66`):

1. `find_postgres_databases` discovers all databases and outputs `DynamicOutput`
2. Downstream ops process each database independently
3. Max concurrency controlled via job config

### Sensor-Driven Workflows

The system uses sensors to react to external events:

- `keycloak_user_sensor`: Polls Keycloak for new users, triggers trust DID generation
- `platform_api_asset_object_sensor`: Monitors API for new asset objects, creates proofs
- `matrix_profile_messages_sensor`: Consumes RabbitMQ messages, launches K8s jobs

Sensors maintain state using PostgresState to track last processed items and avoid duplicates.

### Kubernetes Job Execution

Matrix profile jobs use `dagster_k8s.execute_k8s_job` to run containerized workloads:

- Job config includes image, tag, timeout, pull policy
- Environment variables and secrets passed to containers
- Job output written to S3, status updated via Platform API

## Taskfile Variables

Key variables in `Taskfile.yml`:

- `DAGSTER_CHART_VERSION`: 1.8.7 (must match setup.py Dagster version)
- `OPEN_METADATA_CHART_VERSION`: 1.5.4
- `MINIKUBE_MEM`/`MINIKUBE_CPU`: Resource allocation for local K8s
- `CODE_LOCATION_MODERATE_REMOTE_NAME`: Docker image name for code deployment

## OpenMetadata Admin User Setup

When using local K8s with Keycloak OIDC:

1. Create user in Keycloak UI (moderate realm)
2. Email must match: `<initialAdmins>@<principalDomain>` from `values-openmetadata.yaml.template`

## Development Workflow

### General Principles

- **User-Guided Planning**: For non-trivial or unclear features, begin with brief planning. Ask questions to remove ambiguity.
- **Simplicity First**: Keep implementation and logic straightforward. Only include third-party dependencies with clear justification.
- **Functional Structure**: Organize files/directories by feature to maximize maintainability and ease onboarding.
- **Focused Files**: Keep files concise; split when scope grows or clarity suffers.
- **Repository Hygiene**: Maintain clean repositories—remove dead code, unnecessary comments, and unused files.

### Code Style Guidelines

#### Python (Primary Language)

- **Formatting**: Follow PEP 8 with 4-space indentation, 79-character max lines, logical spacing.
- **Naming**: Functions and variables are `snake_case`. Classes are `PascalCase`.
- **Type Hints**: Use type hints (PEP 484) for all functions and class attributes.
- **Docstrings**: Every public function/class/module uses Google-style docstrings with clear sections for Args, Returns, Raises.
- **Imports**: Group and order as: standard library, third-party, local. Avoid wildcard imports.
- **Immutability**: Use immutable objects and `dataclasses` where appropriate.
- **Error Handling**: Always handle errors explicitly; avoid bare except clauses.

#### Docker

- **Version Pinning**: Images and dependencies must be pinned with explicit version tags (e.g., `postgres:14`)—never use `latest` or `main`.
- **Multi-stage Builds**: Use multi-stage Dockerfile builds to minimize final images. Optimize layer ordering for cache efficiency.
- **Docker Compose**: Use explicit service constraints (`depends_on`), named volumes, `.env` for configuration.

#### Bash/Scripting

- **Strict Mode**: Always start scripts with `#!/usr/bin/env bash`; set strict mode (`set -euo pipefail`).
- **Variable References**: Variables must be referenced with braces (`${var}`).
- **Conditionals**: Use `[[ ]]` for conditionals.
- **Flags**: Prefer long-form flags (e.g., `--verbose`) for self-documentation.

### Universal Best Practices

#### Don't

- Add dependencies unless native APIs are insufficient.
- Leave dependency versions unpinned—always specify at least major version.
- Make large, unfocused commits; prefer atomic changes.
- Commit secrets—use environment variables via `.env` (gitignored).
- Hardcode values; use named constants or configuration files.
- Over-optimize before profiling.
- Ignore errors; handle all failures visibly.
- Leave dead code or commented-out sections.

#### Do

- Clarify requirements with short planning, especially for ambiguous work.
- Confirm framework/library versions before writing code (check `setup.py` for Dagster version 1.8.7).
- Make atomic, descriptive commits with clear commit messages.
- Use environment variables for configuration and secrets.
- Run `black` for code formatting before committing.
- Document complex business logic or non-obvious design decisions.
