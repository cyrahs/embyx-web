# embyx-web

Standalone web management surface for `embyx` workflows. Phase 2 adds a durable Fill Actor backend, a FastAPI API,
and a packaged React management page while keeping filesystem mutation independent from HTTP and UI code.

## Implemented

- Framework-independent `FillActorService` with explicit scan/plan/apply boundaries.
- SQLite and in-memory repositories for plans, private candidates, results, durable jobs, leases, and move journals.
- Explicit SQLite migrations, WAL mode, write-readiness probes, cancellation-safe operations, and restart recovery.
- A bounded persisted scan queue with atomic claim/lease/heartbeat fencing across processes and durable, idempotent
  cancellation of queued or running scans.
- Linux atomic no-overwrite moves using `renameat2(RENAME_NOREPLACE)`, with a same-filesystem hard-link/quarantine
  fallback for NFS servers that reject rename flags, plus a cross-process advisory lock.
- Recovery for current atomic moves and legacy `prepared`, `linked`, and `source_removed` journal states.
- FastAPI endpoints with stable JSON error codes, request/actor/video limits, readiness checks, and Bearer auth for writes.
- React + TypeScript + Vite UI with polling, in-progress scan cancellation, grouped results, move confirmation,
  conflicts, partial failures, one-click bulk magnet copying, RSSHub cache readiness, FreshRSS hand-off, and
  stale-plan recovery.
- Per-actor RSSHub feed prewarming persisted alongside scan jobs, with lease-fenced updates and cache-HIT detection before
  a FreshRSS subscription action is exposed.
- Static frontend assets included in the Python wheel.
- A lazy, origin-checked adapter boundary for a narrow `embyx` compatibility API; tests never load legacy secrets.

## Filesystem safety and recovery

Public plans contain only opaque IDs and display-safe filenames. Absolute source, root, and destination paths remain in
private repository records. Applying a candidate follows this sequence:

1. Validate plan revision, expiry, candidate membership, configured root sentinels, device identity, and source
   fingerprint.
2. Persist a `prepared` journal entry and acquire the cross-process mutation lock.
3. Atomically rename source to destination with `RENAME_NOREPLACE`. If the mounted filesystem rejects that flag, create
   the destination with an atomic no-overwrite hard link and move the source through a private recovery quarantine.
4. Verify inode/fingerprint identity, remove only the verified source link, persist the result, and advance the journal
   through its compatibility states to `reconciled`.

If a process stops after the rename but before persistence, startup reconciliation compares both paths with the recorded
fingerprint. Ambiguous replacements and failed quarantine/rollback operations remain unreconciled for a later retry;
plans with such journals cannot be purged or explicitly deleted. Cancelling an apply request waits for native
filesystem and SQLite operations to finish before releasing locks or propagating cancellation.

The move source and move-in destination must be on the same filesystem. The host must be Linux. The filesystem must
support either `renameat2(RENAME_NOREPLACE)` or hard links; a target that supports neither is reported as
`move_unsupported` and is left unchanged.

## Backend configuration

```bash
uv sync --locked
```

The production bootstrap reads only explicit `EMBYX_WEB_*` variables:

| Variable | Purpose | Default |
| --- | --- | --- |
| `EMBYX_WEB_DATABASE_PATH` | SQLite state database | `state/embyx-web.sqlite3` |
| `EMBYX_WEB_MUTATION_LOCK_PATH` | Cross-process move lock | `<database>.move.lock` |
| `EMBYX_WEB_ACTOR_ROOT` | Primary actor library root | required |
| `EMBYX_WEB_ADDITIONAL_ROOTS` | Additional roots separated by the OS path separator | required |
| `EMBYX_WEB_MOVE_IN_ROOT` | Move-in destination root | required |
| `EMBYX_WEB_MOVE_IN_BY_BRAND` | Put moved files under `<move-in>/<resolved-brand>/` | `false` |
| `EMBYX_WEB_ROOT_SENTINEL` | Required regular marker file in every configured root | `.embyx-root` |
| `EMBYX_WEB_RUNTIME_ROOT` | Root containing the compatibility package | required |
| `EMBYX_WEB_RUNTIME_MODULE` | Compatibility module name | `src.embyx_runtime.fill_actor_api` |
| `EMBYX_WEB_API_TOKEN` | Bearer token for mutation endpoints | optional on loopback |
| `EMBYX_WEB_TLS_TERMINATED` | Assert that a non-loopback deployment is behind TLS | `false` |
| `EMBYX_WEB_HOST` / `EMBYX_WEB_PORT` | Bind address and port | `127.0.0.1:8000` |
| `EMBYX_WEB_MAX_REQUEST_BYTES` | Maximum mutation body | `65536` |
| `EMBYX_WEB_MAX_ACTORS` / `EMBYX_WEB_MAX_VIDEOS` | Per-plan limits | `20` / `2000` |
| `EMBYX_WEB_MAGNET_CONCURRENCY` | Process-wide lookup concurrency | `8` |
| `EMBYX_WEB_RSSHUB_URL` | RSSHub base URL reachable from embyx-web and used to prewarm actor feeds | disabled |
| `EMBYX_WEB_FRESHRSS_URL` | Browser-facing FreshRSS base URL used for the site and add-subscription actions | disabled |
| `EMBYX_WEB_FRESHRSS_RSSHUB_URL` | RSSHub base URL reachable from FreshRSS and embedded in `url_rss` | disabled |

Create the sentinel deliberately in the actual mounted filesystem of the actor root, every additional root, and the
move-in root. A missing marker makes readiness fail and prevents scanning/reconciliation, protecting against an empty
mount point being mistaken for the real library.

Binding to a non-loopback address is rejected unless both `EMBYX_WEB_API_TOKEN` and
`EMBYX_WEB_TLS_TERMINATED=true` are set. The flag is an operator assertion: this app does not terminate TLS itself, so
the listener must be reachable only through the configured TLS reverse proxy.

Run after configuring the environment:

```bash
uv run embyx-web
```

## Runtime compatibility boundary

Do not point the loader at the interactive legacy `fill_actor` entrypoint. The configured module must be a file-backed
module located under `EMBYX_WEB_RUNTIME_ROOT` and expose:

```python
async def list_actor_video_ids(actor_id: str) -> tuple[str, ...]: ...
def resolve_brand(video_id: str) -> str | None: ...
async def find_sukebei_magnet(video_id: str) -> str | None: ...
async def aclose() -> None: ...
```

The loader resolves and validates every package/module origin before execution. This prevents an unrelated same-named
module on `sys.path` from running.

The production container defaults to `src.embyx_runtime.fill_actor_api`, the required compatibility package layout in
the `embyx` runtime image. The selected base image must contain that package under `/app`; changing only `PYTHONPATH`
or mounting the media PVC cannot supply code that is absent from the image.

## API

- `POST /api/fill-actor/plans` validates actor IDs and atomically enqueues a persisted job.
- `GET /api/fill-actor/plans/{plan_id}` returns `{job, plan, feeds}`. The plan becomes visible once its scan result is
  persisted; the job can remain running briefly while per-actor RSSHub cache probes reach a terminal state.
- `POST /api/fill-actor/plans/{plan_id}/cancel` atomically cancels a queued or running scan. The first request and
  repeats after a successful cancellation return `200`; an unknown job returns `404 unknown_plan`, and a job that
  already reached another terminal state returns `409 plan_not_cancellable`. The persisted representation is
  `state=failed` with `error_code=job_cancelled`, which the UI presents as a neutral cancellation rather than a
  failure. This mutation requires Bearer auth but remains available when media roots are temporarily unavailable.
- `POST /api/fill-actor/plans/{plan_id}/apply` accepts only a published plan and applies opaque candidate IDs with
  revision checking.
- `GET /api/health` reports database write-readiness and root/sentinel readiness.

Responses never expose private paths or raw exception messages. The queue defaults to 2 workers and at most 32 active
queued/running jobs per process configuration.

Cancellation first persists the terminal job and pending feed states. When the request reaches the process that owns
the running job, it then stops the actor-catalog, RSSHub, magnet, and filesystem-scan work before returning success. If
another process handles the request, the owner observes the lost lease on its next heartbeat and performs the same
cleanup asynchronously. Filesystem enumeration uses cooperative cancellation and checks its stop token between roots
and directory entries. A single NFS system call already blocked inside the kernel cannot be interrupted safely; a
local-owner cancellation response waits for that call to return and for the scan worker to exit instead of claiming
the task stopped while background I/O is still active.

When RSSHub integration is enabled, each scan schedules the cluster-local `/javbus/star/{actor-id}` request before
actor catalog scanning begins. A background `HEAD` probe waits for a successful XML response carrying
`RSSHub-Cache-Status: HIT`; cache failures do not fail the library scan. Once ready, the UI builds FreshRSS's standard
prefilled add-feed URL from `EMBYX_WEB_FRESHRSS_URL`, using `EMBYX_WEB_FRESHRSS_RSSHUB_URL` for the feed URL that
FreshRSS itself resolves. It also exposes the configured FreshRSS site as a separate action. Deployment-specific URLs
must be injected through environment variables rather than committed as application defaults. The user remains inside
FreshRSS's normal authenticated and CSRF-protected confirmation flow, so embyx-web never stores FreshRSS credentials.

## Frontend and packaging

```bash
cd frontend
npm ci
npm run dev
```

The development server proxies `/api` to `127.0.0.1:8000`. When auth is enabled, the UI asks for the token after a
`401` and retains it only in browser session storage; no token is embedded in build output.

Build the frontend before building the Python artifacts:

```bash
cd frontend
npm run build
cd ..
uv build
```

Vite writes into `src/embyx_web/static`, which Hatch includes in the wheel and the backend serves at `/`.

## Container image

The Dockerfile builds the React bundle and Python wheel in separate stages, installs only locked runtime dependencies
and the wheel under `/opt/embyx-web`, then copies that tree into a digest-pinned compatible `ghcr.io/cyrahs/embyx`
runtime image. The final process uses
`/app/.venv/bin/python`, so the original `/app/src` package and the legacy virtual-environment dependencies remain
visible. `PYTHONPATH` places `/opt/embyx-web` first and retains `/app` for legacy imports.

Build the image with the verified default runtime base, or select another immutable compatible embyx image explicitly:

```bash
docker build -t embyx-web:local .
docker build \
  --build-arg EMBYX_RUNTIME_IMAGE=ghcr.io/cyrahs/embyx@sha256:<compatible-digest> \
  -t embyx-web:local .
```

Container defaults are:

- state database: `/var/lib/embyx-web/embyx-web.sqlite3`;
- compatibility-layer logs: `/var/lib/embyx-web/log`;
- runtime root: `/app`;
- runtime module: `src.embyx_runtime.fill_actor_api`;
- entrypoint: `/app/.venv/bin/python -m embyx_web`.

Mount a persistent volume at `/var/lib/embyx-web` and mount the `media-embyx` PVC at the same paths referenced by
`EMBYX_WEB_ACTOR_ROOT`, `EMBYX_WEB_ADDITIONAL_ROOTS`, and `EMBYX_WEB_MOVE_IN_ROOT`. The compatibility API does not need
the legacy `config.toml`; its optional magnet log directory is controlled only by `EMBYX_RUNTIME_LOG_DIR`. The image
keeps the safe loopback bind default; a Kubernetes Deployment normally sets `EMBYX_WEB_HOST=0.0.0.0` together with a
Bearer token and `EMBYX_WEB_TLS_TERMINATED=true` behind a TLS-terminating proxy.

For the migrated production library, the GitOps deployment mounts `media-embyx` at `/root/data/embyx`, scans the
category roots under `/root/data/embyx/remote`, and moves matches into
`/root/data/embyx/remote/actor/clt/<brand>/` with `EMBYX_WEB_MOVE_IN_BY_BRAND=true`. The old CloudDrive paths are not
mounted into this service. Each configured category root must contain its deliberately created `.embyx-root` marker.

The container smoke check verifies that the installed web package comes from `/opt/embyx-web`, legacy `src` and `tap`
remain discoverable from `/app`, the compatibility module passes the origin-checked loader, static assets ship in the
wheel, and the runtime defaults and entrypoint are intact. Publish a compatible `embyx` base image before building
`embyx-web`; the smoke check intentionally fails when the selected base predates `src.embyx_runtime.fill_actor_api`:

```bash
bash tests/container_smoke.sh embyx-web:local
```

## Verification

```bash
uv lock --check
uv sync --locked
uv run pytest
uv run ruff check .
uv run ruff format --check .
uv run python -m compileall -q src tests

cd frontend
npm run lint
npm test
npm run build
cd ..
uv build

docker build -t embyx-web:ci .
bash tests/container_smoke.sh embyx-web:ci
```

Pushes to the default branch run the same backend, frontend, and container contract checks before publishing
`ghcr.io/<owner>/embyx-web:latest` plus an immutable `sha-<commit>` tag for both `linux/amd64` and `linux/arm64`.
Production GitOps should resolve the published manifest and pin its digest rather than deploy the mutable tag.

Operators must keep the database, lock file, runtime package, and configured media roots writable only by trusted
users.
