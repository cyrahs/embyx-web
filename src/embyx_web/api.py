import asyncio
import logging
import math
import secrets
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated

from fastapi import Depends, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, ConfigDict, Field
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from embyx_web.fill_actor.errors import (
    ExpiredPlanError,
    FillActorError,
    InvalidActorIdError,
    JobQueueFullError,
    RevisionMismatchError,
    TooManyActorsError,
    TooManyVideosError,
    UnknownCandidateError,
    UnknownPlanError,
)
from embyx_web.fill_actor.feeds import build_freshrss_add_url
from embyx_web.fill_actor.jobs import FillActorJobManager
from embyx_web.fill_actor.models import ApplyResult, FillActorPlan
from embyx_web.fill_actor.persistence import (
    CancelJobOutcome,
    FillActorRepository,
    JobFeedRecord,
    JobProgress,
    JobRecord,
    JobStage,
    JobState,
)
from embyx_web.fill_actor.service import FillActorService

HTTP_UNAUTHORIZED = 401
LOGGER = logging.getLogger(__name__)


class CreatePlanRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    actor_ids: list[str] = Field(min_length=1)


class ApplyPlanRequest(BaseModel):
    model_config = ConfigDict(extra='forbid')

    revision: str = Field(min_length=1, max_length=256)
    candidate_ids: list[str] = Field(default_factory=list, max_length=5_000)


class JobProgressView(BaseModel):
    stage: str
    completed: int
    total: int | None
    unit: str
    current: str | None
    stage_started_at: datetime
    updated_at: datetime
    percent: float | None
    eta_seconds: int | None
    elapsed_seconds: int
    last_progress_seconds: int

    @classmethod
    def from_record(cls, progress: JobProgress, *, state: JobState, now: datetime) -> 'JobProgressView':
        elapsed_seconds = max(0, math.floor((now - progress.stage_started_at).total_seconds()))
        last_progress_seconds = max(0, math.floor((now - progress.updated_at).total_seconds()))
        if progress.total is None:
            percent = None
        elif progress.total == 0:
            percent = 100.0
        else:
            percent = round(min(progress.completed / progress.total * 100, 100.0), 2)

        if progress.stage is JobStage.DONE or state not in {JobState.QUEUED, JobState.RUNNING}:
            eta_seconds = 0
        elif progress.total is None or progress.completed == 0:
            eta_seconds = None
        elif progress.completed >= progress.total:
            eta_seconds = 0
        elif elapsed_seconds == 0:
            eta_seconds = None
        else:
            eta_seconds = math.ceil(elapsed_seconds / progress.completed * (progress.total - progress.completed))
        return cls(
            stage=progress.stage.value,
            completed=progress.completed,
            total=progress.total,
            unit=progress.unit.value,
            current=progress.current,
            stage_started_at=progress.stage_started_at,
            updated_at=progress.updated_at,
            percent=percent,
            eta_seconds=eta_seconds,
            elapsed_seconds=elapsed_seconds,
            last_progress_seconds=last_progress_seconds,
        )


class JobView(BaseModel):
    job_id: str
    plan_id: str | None
    operation: str
    state: str
    created_at: datetime
    updated_at: datetime
    error_code: str | None
    progress: JobProgressView

    @classmethod
    def from_record(cls, record: JobRecord) -> 'JobView':
        if record.progress is None:  # pragma: no cover - JobRecord normalizes this invariant
            msg = 'job progress is required'
            raise ValueError(msg)
        now = datetime.now(UTC)
        return cls(
            job_id=record.job_id,
            plan_id=record.plan_id,
            operation=record.operation.value,
            state=record.state.value,
            created_at=record.created_at,
            updated_at=record.updated_at,
            error_code=record.error_code,
            progress=JobProgressView.from_record(record.progress, state=record.state, now=now),
        )


class ActorFeedView(BaseModel):
    actor_id: str
    state: str
    attempts: int
    updated_at: datetime
    error_code: str | None
    freshrss_add_url: str | None
    freshrss_url: str | None

    @classmethod
    def from_record(
        cls,
        record: JobFeedRecord,
        *,
        freshrss_url: str | None = None,
        freshrss_rsshub_url: str | None = None,
    ) -> 'ActorFeedView':
        return cls(
            actor_id=record.actor_id,
            state=record.state.value,
            attempts=record.attempts,
            updated_at=record.updated_at,
            error_code=record.error_code.value if record.error_code is not None else None,
            freshrss_add_url=build_freshrss_add_url(
                record.actor_id,
                freshrss_url=freshrss_url,
                freshrss_rsshub_url=freshrss_rsshub_url,
            ),
            freshrss_url=freshrss_url,
        )


class PlanEnvelope(BaseModel):
    job: JobView
    plan: FillActorPlan | None
    feeds: tuple[ActorFeedView, ...]


class ApiError(Exception):
    def __init__(self, status_code: int, code: str) -> None:
        self.status_code = status_code
        self.code = code
        super().__init__(code)


class RequestSizeLimitMiddleware:
    def __init__(self, app: ASGIApp, *, max_bytes: int) -> None:
        self._app = app
        self._max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope['type'] != 'http' or scope.get('method') not in {'POST', 'PUT', 'PATCH'}:
            await self._app(scope, receive, send)
            return

        messages: list[Message] = []
        received = 0
        while True:
            message = await receive()
            messages.append(message)
            if message['type'] == 'http.request':
                received += len(message.get('body', b''))
                if received > self._max_bytes:
                    response = JSONResponse({'error': {'code': 'request_too_large'}}, status_code=413)
                    await response(scope, receive, send)
                    return
                if not message.get('more_body', False):
                    break
            elif message['type'] == 'http.disconnect':
                break

        iterator = iter(messages)

        async def replay() -> Message:
            try:
                return next(iterator)
            except StopIteration:
                return {'type': 'http.request', 'body': b'', 'more_body': False}

        await self._app(scope, replay, send)


def create_app(  # noqa: C901, PLR0913, PLR0915
    *,
    service: FillActorService,
    repository: FillActorRepository,
    jobs: FillActorJobManager,
    api_token: str | None = None,
    max_request_bytes: int = 65_536,
    runtime_close: Callable[[], Awaitable[None]] | None = None,
    frontend_dist: Path | None = None,
    freshrss_url: str | None = None,
    freshrss_rsshub_url: str | None = None,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        if not await repository.health_check():
            msg = 'fill-actor repository is unavailable'
            raise RuntimeError(msg)
        if await service.roots_ready():
            await service.reconcile_moves()
        await jobs.start()

        async def maintain() -> None:
            while True:
                try:
                    if await service.roots_ready():
                        await service.reconcile_moves()
                    await repository.purge_expired_plans(datetime.now(UTC))
                except asyncio.CancelledError:
                    raise
                except Exception:
                    LOGGER.exception('fill-actor maintenance iteration failed')
                await asyncio.sleep(5)

        maintenance = asyncio.create_task(maintain(), name='fill-actor-maintenance')
        try:
            yield
        finally:
            maintenance.cancel()
            with suppress(asyncio.CancelledError):
                await maintenance
            await jobs.aclose()
            await service.aclose()
            if runtime_close is not None:
                await runtime_close()

    app = FastAPI(title='embyx-web', version='0.2.0', lifespan=lifespan)
    app.add_middleware(RequestSizeLimitMiddleware, max_bytes=max_request_bytes)
    bearer = HTTPBearer(auto_error=False)

    @app.middleware('http')
    async def add_api_cache_control(
        request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        response = await call_next(request)
        if request.url.path.startswith('/api/'):
            response.headers['Cache-Control'] = 'no-store'
        return response

    async def require_mutation_auth(
        credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)],
    ) -> None:
        if api_token is None:
            return
        if (
            credentials is None
            or credentials.scheme.casefold() != 'bearer'
            or not secrets.compare_digest(credentials.credentials, api_token)
        ):
            raise ApiError(401, 'unauthorized')

    async def require_ready() -> None:
        if not await repository.health_check() or not await service.roots_ready():
            raise ApiError(503, 'not_ready')

    async def require_repository_ready() -> None:
        if not await repository.health_check():
            raise ApiError(503, 'not_ready')

    @app.exception_handler(ApiError)
    async def handle_api_error(_request: Request, exc: ApiError) -> JSONResponse:
        headers = {'WWW-Authenticate': 'Bearer'} if exc.status_code == HTTP_UNAUTHORIZED else None
        return JSONResponse({'error': {'code': exc.code}}, status_code=exc.status_code, headers=headers)

    @app.exception_handler(FillActorError)
    async def handle_fill_actor_error(_request: Request, exc: FillActorError) -> JSONResponse:
        return JSONResponse(
            {'error': {'code': exc.code}},
            status_code=_service_error_status(exc),
        )

    @app.exception_handler(RequestValidationError)
    async def handle_validation_error(_request: Request, _exc: RequestValidationError) -> JSONResponse:
        return JSONResponse({'error': {'code': 'invalid_request'}}, status_code=422)

    @app.post(
        '/api/fill-actor/plans',
        status_code=202,
        dependencies=[Depends(require_mutation_auth), Depends(require_ready)],
    )
    async def create_plan(request: CreatePlanRequest) -> PlanEnvelope:
        job = await jobs.start_plan(request.actor_ids)
        feeds = await jobs.get_feeds(job.job_id)
        return PlanEnvelope(
            job=JobView.from_record(job),
            plan=None,
            feeds=tuple(
                ActorFeedView.from_record(
                    feed,
                    freshrss_url=freshrss_url,
                    freshrss_rsshub_url=freshrss_rsshub_url,
                )
                for feed in feeds
            ),
        )

    @app.get('/api/fill-actor/plans/{plan_id}')
    async def get_plan(plan_id: str) -> PlanEnvelope:
        job = await jobs.get_job(plan_id)
        if job is None:
            raise UnknownPlanError(plan_id)
        plan = await jobs.get_plan(plan_id)
        if plan is None and job.state in {JobState.COMPLETED, JobState.PARTIAL_FAILED}:
            raise UnknownPlanError(plan_id)
        if plan is None and job.plan_id is None and job.error_code is None:
            raise UnknownPlanError(plan_id)
        feeds = await jobs.get_feeds(plan_id)
        return PlanEnvelope(
            job=JobView.from_record(job),
            plan=plan,
            feeds=tuple(
                ActorFeedView.from_record(
                    feed,
                    freshrss_url=freshrss_url,
                    freshrss_rsshub_url=freshrss_rsshub_url,
                )
                for feed in feeds
            ),
        )

    @app.post(
        '/api/fill-actor/plans/{plan_id}/cancel',
        dependencies=[Depends(require_mutation_auth), Depends(require_repository_ready)],
    )
    async def cancel_plan(plan_id: str) -> PlanEnvelope:
        result = await jobs.cancel_plan(plan_id)
        if result.outcome is CancelJobOutcome.NOT_FOUND or result.job is None:
            raise UnknownPlanError(plan_id)
        if result.outcome is CancelJobOutcome.ALREADY_TERMINAL:
            raise ApiError(409, 'plan_not_cancellable')
        feeds = await jobs.get_feeds(plan_id)
        return PlanEnvelope(
            job=JobView.from_record(result.job),
            plan=None,
            feeds=tuple(
                ActorFeedView.from_record(
                    feed,
                    freshrss_url=freshrss_url,
                    freshrss_rsshub_url=freshrss_rsshub_url,
                )
                for feed in feeds
            ),
        )

    @app.post(
        '/api/fill-actor/plans/{plan_id}/apply',
        dependencies=[Depends(require_mutation_auth), Depends(require_ready)],
    )
    async def apply_plan(plan_id: str, request: ApplyPlanRequest) -> ApplyResult:
        job = await jobs.get_job(plan_id)
        if job is None:
            raise UnknownPlanError(plan_id)
        if job.state not in {JobState.COMPLETED, JobState.PARTIAL_FAILED}:
            raise ApiError(409, 'plan_not_ready')
        return await service.apply(
            plan_id=plan_id,
            revision=request.revision,
            candidate_ids=request.candidate_ids,
        )

    @app.get('/api/health')
    async def health() -> JSONResponse:
        database_ready = await repository.health_check()
        roots_ready = await service.roots_ready()
        ready = database_ready and roots_ready
        return JSONResponse(
            {
                'status': 'ok' if ready else 'not_ready',
                'database': database_ready,
                'roots': roots_ready,
            },
            status_code=200 if ready else 503,
        )

    if frontend_dist is not None and frontend_dist.is_dir():
        app.mount('/', StaticFiles(directory=frontend_dist, html=True), name='frontend')
    return app


def _service_error_status(exc: FillActorError) -> int:
    mappings: Sequence[tuple[type[FillActorError], int]] = (
        (InvalidActorIdError, 422),
        (TooManyActorsError, 422),
        (TooManyVideosError, 422),
        (UnknownPlanError, 404),
        (ExpiredPlanError, 410),
        (RevisionMismatchError, 409),
        (UnknownCandidateError, 422),
        (JobQueueFullError, 429),
    )
    return next((status for error_type, status in mappings if isinstance(exc, error_type)), 500)
