import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from embyx_web.fill_actor.cloud_moves import CloudFileMetadata
from embyx_web.fill_actor.models import FillActorPlan, MoveResult

SHA256_HEX_LENGTH = 64


class JobState(StrEnum):
    QUEUED = 'queued'
    RUNNING = 'running'
    COMPLETED = 'completed'
    PARTIAL_FAILED = 'partial_failed'
    FAILED = 'failed'


JOB_CANCELLED_ERROR_CODE = 'job_cancelled'


class CancelJobOutcome(StrEnum):
    CANCELLED = 'cancelled'
    ALREADY_CANCELLED = 'already_cancelled'
    ALREADY_TERMINAL = 'already_terminal'
    NOT_FOUND = 'not_found'


class JobOperation(StrEnum):
    CREATE_PLAN = 'create_plan'
    APPLY = 'apply'


class JobStage(StrEnum):
    QUEUED = 'queued'
    ACTOR_CATALOG = 'actor_catalog'
    LIBRARY_SCAN = 'library_scan'
    MAGNET_LOOKUP = 'magnet_lookup'
    PERSISTING = 'persisting'
    DONE = 'done'
    UNKNOWN = 'unknown'


class JobProgressUnit(StrEnum):
    ACTORS = 'actors'
    PAGES = 'pages'
    VIDEOS = 'videos'
    MAGNETS = 'magnets'
    STEPS = 'steps'
    ITEMS = 'items'


class JobFeedState(StrEnum):
    QUEUED = 'queued'
    WARMING = 'warming'
    READY = 'ready'
    FAILED = 'failed'


class JobFeedErrorCode(StrEnum):
    TIMEOUT = 'rsshub_timeout'
    NETWORK = 'rsshub_network_error'
    HTTP = 'rsshub_http_error'
    INVALID_FEED = 'rsshub_invalid_feed'
    NOT_READY = 'rsshub_not_ready'
    CANCELLED = 'rsshub_cancelled'


class MoveJournalState(StrEnum):
    PREPARED = 'prepared'
    LINKED = 'linked'
    SOURCE_REMOVED = 'source_removed'
    RECONCILED = 'reconciled'


class CandidateKind(StrEnum):
    LOCAL_FILE = 'local_file'
    CLOUD_STRM = 'cloud_strm'


class CloudMoveOperationState(StrEnum):
    PREPARED = 'prepared'
    SUBMITTING = 'submitting'
    VERIFYING = 'verifying'
    UNKNOWN = 'unknown'
    SUCCEEDED = 'succeeded'
    CONFLICT = 'conflict'
    FAILED = 'failed'

    @property
    def terminal(self) -> bool:
        return self in {
            CloudMoveOperationState.SUCCEEDED,
            CloudMoveOperationState.CONFLICT,
            CloudMoveOperationState.FAILED,
        }


@dataclass(frozen=True)
class FileFingerprint:
    device: int
    inode: int
    size: int
    mtime_ns: int
    ctime_ns: int


@dataclass(frozen=True)
class CandidateRecord:
    candidate_id: str
    video_id: str
    source: Path
    source_root: Path
    destination: Path
    fingerprint: FileFingerprint
    kind: CandidateKind = CandidateKind.LOCAL_FILE
    mapping_sha256: str | None = None
    cloud_source_path: str | None = None
    cloud_destination_dir: str | None = None
    cloud_file: CloudFileMetadata | None = None

    def __post_init__(self) -> None:
        cloud_values = (
            self.mapping_sha256,
            self.cloud_source_path,
            self.cloud_destination_dir,
            self.cloud_file,
        )
        if self.kind is CandidateKind.LOCAL_FILE and any(value is not None for value in cloud_values):
            msg = 'local candidates must not contain CloudDrive metadata'
            raise ValueError(msg)
        if self.kind is CandidateKind.CLOUD_STRM:
            if any(value is None for value in cloud_values):
                msg = 'CloudDrive candidates require mapping and remote metadata'
                raise ValueError(msg)
            if self.cloud_file is not None and self.cloud_file.path != self.cloud_source_path:
                msg = 'CloudDrive candidate metadata path must match its source path'
                raise ValueError(msg)
            if self.mapping_sha256 is None or len(self.mapping_sha256) != SHA256_HEX_LENGTH:
                msg = 'CloudDrive candidate mapping digest is invalid'
                raise ValueError(msg)


@dataclass(frozen=True)
class PlanRecord:
    public: FillActorPlan
    candidates: tuple[CandidateRecord, ...]

    def __post_init__(self) -> None:
        candidate_ids = [candidate.candidate_id for candidate in self.candidates]
        if len(candidate_ids) != len(set(candidate_ids)):
            msg = 'candidate ids must be unique within a plan'
            raise ValueError(msg)
        public_candidates = {
            candidate.candidate_id: candidate.video_id
            for video in self.public.videos
            for candidate in video.move_candidates
        }
        private_candidates = {candidate.candidate_id: candidate.video_id for candidate in self.candidates}
        if private_candidates != public_candidates:
            msg = 'private candidate records must match public plan candidates'
            raise ValueError(msg)

    def candidate(self, candidate_id: str) -> CandidateRecord | None:
        return next((candidate for candidate in self.candidates if candidate.candidate_id == candidate_id), None)


@dataclass(frozen=True)
class JobProgress:
    stage: JobStage
    completed: int
    total: int | None
    unit: JobProgressUnit
    current: str | None
    stage_started_at: datetime
    updated_at: datetime

    def __post_init__(self) -> None:
        if self.completed < 0:
            msg = 'job progress completed must not be negative'
            raise ValueError(msg)
        if self.total is not None and self.total < 0:
            msg = 'job progress total must not be negative'
            raise ValueError(msg)
        if self.total is not None and self.completed > self.total:
            msg = 'job progress completed must not exceed total'
            raise ValueError(msg)


@dataclass(frozen=True)
class JobProgressEvent:
    stage: JobStage
    completed: int
    total: int | None
    unit: JobProgressUnit
    current: str | None = None

    def __post_init__(self) -> None:
        if self.completed < 0:
            msg = 'job progress completed must not be negative'
            raise ValueError(msg)
        if self.total is not None and self.total < 0:
            msg = 'job progress total must not be negative'
            raise ValueError(msg)
        if self.total is not None and self.completed > self.total:
            msg = 'job progress completed must not exceed total'
            raise ValueError(msg)


@dataclass(frozen=True)
class JobRecord:
    job_id: str
    operation: JobOperation
    state: JobState
    created_at: datetime
    updated_at: datetime
    plan_id: str | None = None
    error_code: str | None = None
    owner_id: str | None = None
    lease_expires_at: datetime | None = None
    actor_ids: tuple[str, ...] = ()
    progress: JobProgress | None = None

    def __post_init__(self) -> None:
        if self.progress is None:
            object.__setattr__(
                self,
                'progress',
                JobProgress(
                    stage=JobStage.QUEUED,
                    completed=0,
                    total=len(self.actor_ids) if self.actor_ids else None,
                    unit=JobProgressUnit.ACTORS if self.actor_ids else JobProgressUnit.ITEMS,
                    current=None,
                    stage_started_at=self.created_at,
                    updated_at=self.updated_at,
                ),
            )


@dataclass(frozen=True)
class CancelJobResult:
    outcome: CancelJobOutcome
    job: JobRecord | None
    previous_state: JobState | None = None
    previous_owner_id: str | None = None


@dataclass(frozen=True)
class JobFeedRecord:
    job_id: str
    actor_id: str
    state: JobFeedState
    attempts: int
    updated_at: datetime
    error_code: JobFeedErrorCode | None = None
    freshrss_add_url: str | None = None

    def __post_init__(self) -> None:
        if not self.job_id or not self.actor_id:
            msg = 'job feed identifiers must not be empty'
            raise ValueError(msg)
        if self.attempts < 0:
            msg = 'job feed attempts must not be negative'
            raise ValueError(msg)
        if self.state is JobFeedState.FAILED and not self.error_code:
            msg = 'failed job feeds require an error code'
            raise ValueError(msg)
        if self.state is not JobFeedState.FAILED and self.error_code is not None:
            msg = 'only failed job feeds may have an error code'
            raise ValueError(msg)


@dataclass(frozen=True)
class MoveJournalRecord:
    plan_id: str
    candidate_id: str
    state: MoveJournalState
    updated_at: datetime


@dataclass(frozen=True)
class CloudMoveOperationRecord:
    plan_id: str
    candidate_id: str
    attempt_id: str
    source_path: str
    destination_dir: str
    state: CloudMoveOperationState
    updated_at: datetime
    error_code: str | None = None

    def __post_init__(self) -> None:
        if not self.attempt_id or not self.source_path or not self.destination_dir:
            msg = 'CloudDrive operation identifiers and paths must not be empty'
            raise ValueError(msg)
        error_states = {
            CloudMoveOperationState.UNKNOWN,
            CloudMoveOperationState.CONFLICT,
            CloudMoveOperationState.FAILED,
        }
        if self.state in error_states and self.error_code is None:
            msg = 'non-successful CloudDrive operations require an error code'
            raise ValueError(msg)
        if self.state not in error_states and self.error_code is not None:
            msg = 'successful or in-progress CloudDrive operations must not carry an error code'
            raise ValueError(msg)


class InvalidMoveJournalTransitionError(ValueError):
    def __init__(self, current: MoveJournalState | None, requested: MoveJournalState) -> None:
        current_value = current.value if current is not None else 'none'
        super().__init__(f'invalid move journal transition: {current_value} -> {requested.value}')


class InvalidCloudMoveTransitionError(ValueError):
    def __init__(self, current: CloudMoveOperationState | None, requested: CloudMoveOperationState) -> None:
        current_value = current.value if current is not None else 'none'
        super().__init__(f'invalid CloudDrive move transition: {current_value} -> {requested.value}')


class FillActorRepository(Protocol):
    async def save_plan(self, record: PlanRecord) -> None: ...

    async def get_plan(self, plan_id: str) -> PlanRecord | None: ...

    async def get_candidate(self, plan_id: str, candidate_id: str) -> CandidateRecord | None: ...

    async def delete_plan(self, plan_id: str) -> bool: ...

    async def purge_expired_plans(self, now: datetime) -> int: ...

    async def save_move_result(self, plan_id: str, result: MoveResult) -> None: ...

    async def get_move_result(self, plan_id: str, candidate_id: str) -> MoveResult | None: ...

    async def list_move_results(self, plan_id: str) -> tuple[MoveResult, ...]: ...

    async def save_job(self, record: JobRecord) -> None: ...

    async def enqueue_job(
        self,
        record: JobRecord,
        *,
        max_active: int,
        feeds: Sequence[JobFeedRecord] = (),
    ) -> bool: ...

    async def claim_next_job(
        self,
        *,
        owner_id: str,
        now: datetime,
        lease_expires_at: datetime,
    ) -> JobRecord | None: ...

    async def renew_owned_job_lease(
        self,
        *,
        job_id: str,
        owner_id: str,
        now: datetime,
        lease_expires_at: datetime,
    ) -> bool: ...

    async def update_owned_job_progress(
        self,
        *,
        job_id: str,
        owner_id: str,
        progress: JobProgress,
        now: datetime,
    ) -> bool: ...

    async def finish_owned_job(  # noqa: PLR0913
        self,
        *,
        job_id: str,
        owner_id: str,
        state: JobState,
        error_code: str | None,
        now: datetime,
        progress: JobProgress,
    ) -> bool: ...

    async def cancel_job(self, *, job_id: str, now: datetime) -> CancelJobResult: ...

    async def fail_expired_jobs(self, *, now: datetime, error_code: str) -> int: ...

    async def get_job(self, job_id: str) -> JobRecord | None: ...

    async def list_jobs(self, states: Sequence[JobState] | None = None) -> tuple[JobRecord, ...]: ...

    async def list_job_feeds(self, job_id: str) -> tuple[JobFeedRecord, ...]: ...

    async def update_owned_job_feed(  # noqa: PLR0913
        self,
        *,
        job_id: str,
        actor_id: str,
        owner_id: str,
        state: JobFeedState,
        attempts: int,
        error_code: JobFeedErrorCode | None,
        now: datetime,
    ) -> bool: ...

    async def save_move_journal(self, record: MoveJournalRecord) -> None: ...

    async def get_move_journal(self, plan_id: str, candidate_id: str) -> MoveJournalRecord | None: ...

    async def list_unreconciled_moves(self) -> tuple[MoveJournalRecord, ...]: ...

    async def save_cloud_move_operation(self, record: CloudMoveOperationRecord) -> None: ...

    async def get_cloud_move_operation(
        self,
        plan_id: str,
        candidate_id: str,
    ) -> CloudMoveOperationRecord | None: ...

    async def list_unresolved_cloud_moves(self) -> tuple[CloudMoveOperationRecord, ...]: ...

    async def finalize_cloud_move(self, operation: CloudMoveOperationRecord, result: MoveResult) -> None: ...

    async def health_check(self) -> bool: ...


class MemoryFillActorRepository:
    def __init__(self) -> None:
        self._plans: dict[str, PlanRecord] = {}
        self._move_results: dict[tuple[str, str], MoveResult] = {}
        self._jobs: dict[str, JobRecord] = {}
        self._job_feeds: dict[tuple[str, str], JobFeedRecord] = {}
        self._move_journal: dict[tuple[str, str], MoveJournalRecord] = {}
        self._cloud_move_operations: dict[tuple[str, str], CloudMoveOperationRecord] = {}
        self._lock = asyncio.Lock()

    async def save_plan(self, record: PlanRecord) -> None:
        async with self._lock:
            plan_id = record.public.plan_id
            self._plans[plan_id] = record
            candidate_ids = {candidate.candidate_id for candidate in record.candidates}
            self._move_results = {
                key: result
                for key, result in self._move_results.items()
                if key[0] != plan_id or key[1] in candidate_ids
            }
            self._move_journal = {
                key: journal
                for key, journal in self._move_journal.items()
                if key[0] != plan_id or key[1] in candidate_ids
            }
            self._cloud_move_operations = {
                key: operation
                for key, operation in self._cloud_move_operations.items()
                if key[0] != plan_id or key[1] in candidate_ids
            }

    async def get_plan(self, plan_id: str) -> PlanRecord | None:
        async with self._lock:
            return self._plans.get(plan_id)

    async def get_candidate(self, plan_id: str, candidate_id: str) -> CandidateRecord | None:
        async with self._lock:
            plan = self._plans.get(plan_id)
            return plan.candidate(candidate_id) if plan is not None else None

    async def delete_plan(self, plan_id: str) -> bool:
        async with self._lock:
            if any(
                key[0] == plan_id and journal.state is not MoveJournalState.RECONCILED
                for key, journal in self._move_journal.items()
            ) or any(
                key[0] == plan_id and not operation.state.terminal
                for key, operation in self._cloud_move_operations.items()
            ):
                return False
            removed = self._plans.pop(plan_id, None) is not None
            if removed:
                self._delete_plan_dependents(plan_id)
            return removed

    async def purge_expired_plans(self, now: datetime) -> int:
        async with self._lock:
            normalized_now = normalize_datetime(now)
            expired = [
                plan_id
                for plan_id, record in self._plans.items()
                if normalize_datetime(record.public.expires_at) <= normalized_now
                and not any(
                    key[0] == plan_id and journal.state is not MoveJournalState.RECONCILED
                    for key, journal in self._move_journal.items()
                )
                and not any(
                    key[0] == plan_id and not operation.state.terminal
                    for key, operation in self._cloud_move_operations.items()
                )
            ]
            for plan_id in expired:
                self._plans.pop(plan_id)
                self._delete_plan_dependents(plan_id)
            return len(expired)

    async def save_move_result(self, plan_id: str, result: MoveResult) -> None:
        async with self._lock:
            self._require_matching_candidate(plan_id, result.candidate_id, result.video_id)
            self._move_results[(plan_id, result.candidate_id)] = result

    async def get_move_result(self, plan_id: str, candidate_id: str) -> MoveResult | None:
        async with self._lock:
            return self._move_results.get((plan_id, candidate_id))

    async def list_move_results(self, plan_id: str) -> tuple[MoveResult, ...]:
        async with self._lock:
            return tuple(self._move_results[key] for key in sorted(self._move_results) if key[0] == plan_id)

    async def save_job(self, record: JobRecord) -> None:
        async with self._lock:
            self._jobs[record.job_id] = record

    async def enqueue_job(
        self,
        record: JobRecord,
        *,
        max_active: int,
        feeds: Sequence[JobFeedRecord] = (),
    ) -> bool:
        async with self._lock:
            active = sum(job.state in {JobState.QUEUED, JobState.RUNNING} for job in self._jobs.values())
            if active >= max_active or record.job_id in self._jobs:
                return False
            feed_keys = [(feed.job_id, feed.actor_id) for feed in feeds]
            if any(feed.job_id != record.job_id for feed in feeds) or len(feed_keys) != len(set(feed_keys)):
                msg = 'job feeds must be unique and belong to the enqueued job'
                raise ValueError(msg)
            self._jobs[record.job_id] = record
            self._job_feeds.update({(feed.job_id, feed.actor_id): feed for feed in feeds})
            return True

    async def claim_next_job(
        self,
        *,
        owner_id: str,
        now: datetime,
        lease_expires_at: datetime,
    ) -> JobRecord | None:
        async with self._lock:
            queued = sorted(
                (job for job in self._jobs.values() if job.state is JobState.QUEUED),
                key=lambda job: (job.created_at, job.job_id),
            )
            if not queued:
                return None
            current = queued[0]
            claimed = _replace_job(
                current,
                state=JobState.RUNNING,
                updated_at=now,
                owner_id=owner_id,
                lease_expires_at=lease_expires_at,
            )
            self._jobs[current.job_id] = claimed
            return claimed

    async def renew_owned_job_lease(
        self,
        *,
        job_id: str,
        owner_id: str,
        now: datetime,
        lease_expires_at: datetime,
    ) -> bool:
        async with self._lock:
            current = self._jobs.get(job_id)
            if (
                current is None
                or current.owner_id != owner_id
                or current.state is not JobState.RUNNING
                or current.lease_expires_at is None
                or normalize_datetime(current.lease_expires_at) <= normalize_datetime(now)
            ):
                return False
            self._jobs[job_id] = _replace_job(
                current,
                state=JobState.RUNNING,
                updated_at=now,
                owner_id=owner_id,
                lease_expires_at=lease_expires_at,
                progress=current.progress,
            )
            return True

    async def update_owned_job_progress(
        self,
        *,
        job_id: str,
        owner_id: str,
        progress: JobProgress,
        now: datetime,
    ) -> bool:
        async with self._lock:
            current = self._jobs.get(job_id)
            if (
                current is None
                or current.owner_id != owner_id
                or current.state is not JobState.RUNNING
                or current.lease_expires_at is None
                or normalize_datetime(current.lease_expires_at) <= normalize_datetime(now)
            ):
                return False
            self._jobs[job_id] = _replace_job(
                current,
                state=JobState.RUNNING,
                updated_at=current.updated_at,
                owner_id=owner_id,
                lease_expires_at=current.lease_expires_at,
                progress=progress,
            )
            return True

    async def finish_owned_job(  # noqa: PLR0913
        self,
        *,
        job_id: str,
        owner_id: str,
        state: JobState,
        error_code: str | None,
        now: datetime,
        progress: JobProgress,
    ) -> bool:
        async with self._lock:
            current = self._jobs.get(job_id)
            if (
                current is None
                or current.owner_id != owner_id
                or current.state is not JobState.RUNNING
                or current.lease_expires_at is None
                or normalize_datetime(current.lease_expires_at) <= normalize_datetime(now)
            ):
                return False
            self._jobs[job_id] = _replace_job(
                current,
                state=state,
                updated_at=now,
                error_code=error_code,
                owner_id=None,
                lease_expires_at=None,
                progress=progress,
            )
            return True

    async def cancel_job(self, *, job_id: str, now: datetime) -> CancelJobResult:
        async with self._lock:
            current = self._jobs.get(job_id)
            if current is None:
                return CancelJobResult(CancelJobOutcome.NOT_FOUND, None)
            if current.state is JobState.FAILED and current.error_code == JOB_CANCELLED_ERROR_CODE:
                return CancelJobResult(
                    CancelJobOutcome.ALREADY_CANCELLED,
                    current,
                    previous_state=current.state,
                    previous_owner_id=current.owner_id,
                )
            if current.state not in {JobState.QUEUED, JobState.RUNNING}:
                return CancelJobResult(
                    CancelJobOutcome.ALREADY_TERMINAL,
                    current,
                    previous_state=current.state,
                    previous_owner_id=current.owner_id,
                )

            cancelled = _replace_job(
                current,
                state=JobState.FAILED,
                updated_at=now,
                error_code=JOB_CANCELLED_ERROR_CODE,
                owner_id=None,
                lease_expires_at=None,
                progress=_terminal_progress(current.progress, now),
            )
            self._jobs[job_id] = cancelled
            for key, feed in tuple(self._job_feeds.items()):
                if key[0] == job_id and feed.state in {JobFeedState.QUEUED, JobFeedState.WARMING}:
                    self._job_feeds[key] = JobFeedRecord(
                        job_id=feed.job_id,
                        actor_id=feed.actor_id,
                        state=JobFeedState.FAILED,
                        attempts=feed.attempts,
                        updated_at=now,
                        error_code=JobFeedErrorCode.CANCELLED,
                        freshrss_add_url=feed.freshrss_add_url,
                    )
            return CancelJobResult(
                CancelJobOutcome.CANCELLED,
                cancelled,
                previous_state=current.state,
                previous_owner_id=current.owner_id,
            )

    async def fail_expired_jobs(self, *, now: datetime, error_code: str) -> int:
        async with self._lock:
            normalized_now = normalize_datetime(now)
            expired = [
                job
                for job in self._jobs.values()
                if job.state is JobState.RUNNING
                and (job.lease_expires_at is None or normalize_datetime(job.lease_expires_at) <= normalized_now)
            ]
            for job in expired:
                self._jobs[job.job_id] = _replace_job(
                    job,
                    state=JobState.FAILED,
                    updated_at=now,
                    error_code=error_code,
                    owner_id=None,
                    lease_expires_at=None,
                    progress=_terminal_progress(job.progress, now),
                )
                for key, feed in tuple(self._job_feeds.items()):
                    if key[0] == job.job_id and feed.state in {JobFeedState.QUEUED, JobFeedState.WARMING}:
                        self._job_feeds[key] = JobFeedRecord(
                            job_id=feed.job_id,
                            actor_id=feed.actor_id,
                            state=JobFeedState.FAILED,
                            attempts=feed.attempts,
                            updated_at=now,
                            error_code=JobFeedErrorCode.CANCELLED,
                            freshrss_add_url=feed.freshrss_add_url,
                        )
            return len(expired)

    async def get_job(self, job_id: str) -> JobRecord | None:
        async with self._lock:
            return self._jobs.get(job_id)

    async def list_jobs(self, states: Sequence[JobState] | None = None) -> tuple[JobRecord, ...]:
        async with self._lock:
            selected_states = set(states) if states is not None else None
            records = [
                record for record in self._jobs.values() if selected_states is None or record.state in selected_states
            ]
            return tuple(sorted(records, key=lambda record: (record.created_at, record.job_id)))

    async def list_job_feeds(self, job_id: str) -> tuple[JobFeedRecord, ...]:
        async with self._lock:
            return tuple(self._job_feeds[key] for key in sorted(self._job_feeds) if key[0] == job_id)

    async def update_owned_job_feed(  # noqa: PLR0913
        self,
        *,
        job_id: str,
        actor_id: str,
        owner_id: str,
        state: JobFeedState,
        attempts: int,
        error_code: JobFeedErrorCode | None,
        now: datetime,
    ) -> bool:
        replacement = JobFeedRecord(
            job_id=job_id,
            actor_id=actor_id,
            state=state,
            attempts=attempts,
            updated_at=now,
            error_code=error_code,
        )
        async with self._lock:
            job = self._jobs.get(job_id)
            current = self._job_feeds.get((job_id, actor_id))
            if (
                job is None
                or current is None
                or job.owner_id != owner_id
                or job.state is not JobState.RUNNING
                or job.lease_expires_at is None
                or normalize_datetime(job.lease_expires_at) <= normalize_datetime(now)
                or current.state in {JobFeedState.READY, JobFeedState.FAILED}
                or attempts < current.attempts
            ):
                return False
            self._job_feeds[(job_id, actor_id)] = JobFeedRecord(
                job_id=replacement.job_id,
                actor_id=replacement.actor_id,
                state=replacement.state,
                attempts=replacement.attempts,
                updated_at=replacement.updated_at,
                error_code=replacement.error_code,
                freshrss_add_url=current.freshrss_add_url,
            )
            return True

    async def save_move_journal(self, record: MoveJournalRecord) -> None:
        async with self._lock:
            self._require_candidate(record.plan_id, record.candidate_id)
            key = (record.plan_id, record.candidate_id)
            current = self._move_journal.get(key)
            validate_journal_transition(current.state if current is not None else None, record.state)
            self._move_journal[key] = record

    async def get_move_journal(self, plan_id: str, candidate_id: str) -> MoveJournalRecord | None:
        async with self._lock:
            return self._move_journal.get((plan_id, candidate_id))

    async def list_unreconciled_moves(self) -> tuple[MoveJournalRecord, ...]:
        async with self._lock:
            records = [
                record for record in self._move_journal.values() if record.state is not MoveJournalState.RECONCILED
            ]
            return tuple(sorted(records, key=lambda record: (record.updated_at, record.plan_id, record.candidate_id)))

    async def save_cloud_move_operation(self, record: CloudMoveOperationRecord) -> None:
        if record.state.terminal:
            msg = 'terminal CloudDrive operations must be finalized with their result'
            raise ValueError(msg)
        async with self._lock:
            self._save_cloud_move_operation_locked(record)

    async def get_cloud_move_operation(
        self,
        plan_id: str,
        candidate_id: str,
    ) -> CloudMoveOperationRecord | None:
        async with self._lock:
            return self._cloud_move_operations.get((plan_id, candidate_id))

    async def list_unresolved_cloud_moves(self) -> tuple[CloudMoveOperationRecord, ...]:
        async with self._lock:
            records = [operation for operation in self._cloud_move_operations.values() if not operation.state.terminal]
            return tuple(sorted(records, key=lambda record: (record.updated_at, record.plan_id, record.candidate_id)))

    async def finalize_cloud_move(self, operation: CloudMoveOperationRecord, result: MoveResult) -> None:
        async with self._lock:
            if not operation.state.terminal:
                msg = 'finalized CloudDrive operation must be terminal'
                raise ValueError(msg)
            self._require_matching_candidate(operation.plan_id, result.candidate_id, result.video_id)
            if result.candidate_id != operation.candidate_id:
                msg = 'CloudDrive result must match its operation'
                raise ValueError(msg)
            self._save_cloud_move_operation_locked(operation)
            self._move_results[(operation.plan_id, operation.candidate_id)] = result

    async def health_check(self) -> bool:
        return True

    def _delete_plan_dependents(self, plan_id: str) -> None:
        self._move_results = {key: result for key, result in self._move_results.items() if key[0] != plan_id}
        self._move_journal = {key: record for key, record in self._move_journal.items() if key[0] != plan_id}
        self._cloud_move_operations = {
            key: record for key, record in self._cloud_move_operations.items() if key[0] != plan_id
        }
        self._jobs = {
            job_id: record if record.plan_id != plan_id else _without_plan(record)
            for job_id, record in self._jobs.items()
        }

    def _require_candidate(self, plan_id: str, candidate_id: str) -> CandidateRecord:
        plan = self._plans.get(plan_id)
        candidate = plan.candidate(candidate_id) if plan is not None else None
        if candidate is None:
            raise KeyError((plan_id, candidate_id))
        return candidate

    def _save_cloud_move_operation_locked(self, record: CloudMoveOperationRecord) -> None:
        candidate = self._require_candidate(record.plan_id, record.candidate_id)
        if candidate.kind is not CandidateKind.CLOUD_STRM:
            msg = 'CloudDrive operations require a CloudDrive candidate'
            raise ValueError(msg)
        if (
            candidate.cloud_source_path != record.source_path
            or candidate.cloud_destination_dir != record.destination_dir
        ):
            msg = 'CloudDrive operation paths must match its candidate'
            raise ValueError(msg)
        key = (record.plan_id, record.candidate_id)
        current = self._cloud_move_operations.get(key)
        validate_cloud_move_transition(current.state if current is not None else None, record.state)
        if current is not None and current.attempt_id != record.attempt_id:
            msg = 'CloudDrive operation attempt id cannot change'
            raise ValueError(msg)
        if not record.state.terminal:
            duplicate = next(
                (
                    operation
                    for operation_key, operation in self._cloud_move_operations.items()
                    if operation_key != key
                    and operation.source_path == record.source_path
                    and not operation.state.terminal
                ),
                None,
            )
            if duplicate is not None:
                msg = 'CloudDrive source already has an unresolved operation'
                raise ValueError(msg)
        self._cloud_move_operations[key] = record

    def _require_matching_candidate(self, plan_id: str, candidate_id: str, video_id: str) -> None:
        candidate = self._require_candidate(plan_id, candidate_id)
        if candidate.video_id != video_id:
            msg = 'move result video id does not match candidate'
            raise ValueError(msg)


_ALLOWED_JOURNAL_TRANSITIONS: dict[MoveJournalState | None, Sequence[MoveJournalState]] = {
    None: (MoveJournalState.PREPARED,),
    MoveJournalState.PREPARED: (
        MoveJournalState.PREPARED,
        MoveJournalState.LINKED,
        MoveJournalState.RECONCILED,
    ),
    MoveJournalState.LINKED: (
        MoveJournalState.LINKED,
        MoveJournalState.SOURCE_REMOVED,
        MoveJournalState.RECONCILED,
    ),
    MoveJournalState.SOURCE_REMOVED: (
        MoveJournalState.SOURCE_REMOVED,
        MoveJournalState.RECONCILED,
    ),
    MoveJournalState.RECONCILED: (MoveJournalState.RECONCILED,),
}


def validate_journal_transition(current: MoveJournalState | None, requested: MoveJournalState) -> None:
    if requested not in _ALLOWED_JOURNAL_TRANSITIONS[current]:
        raise InvalidMoveJournalTransitionError(current, requested)


_ALLOWED_CLOUD_MOVE_TRANSITIONS: dict[
    CloudMoveOperationState | None,
    Sequence[CloudMoveOperationState],
] = {
    None: (CloudMoveOperationState.PREPARED,),
    CloudMoveOperationState.PREPARED: (
        CloudMoveOperationState.PREPARED,
        CloudMoveOperationState.SUBMITTING,
        CloudMoveOperationState.CONFLICT,
        CloudMoveOperationState.FAILED,
    ),
    CloudMoveOperationState.SUBMITTING: (
        CloudMoveOperationState.SUBMITTING,
        CloudMoveOperationState.VERIFYING,
        CloudMoveOperationState.UNKNOWN,
    ),
    CloudMoveOperationState.VERIFYING: (
        CloudMoveOperationState.VERIFYING,
        CloudMoveOperationState.UNKNOWN,
        CloudMoveOperationState.SUCCEEDED,
        CloudMoveOperationState.CONFLICT,
        CloudMoveOperationState.FAILED,
    ),
    CloudMoveOperationState.UNKNOWN: (
        CloudMoveOperationState.UNKNOWN,
        CloudMoveOperationState.SUCCEEDED,
        CloudMoveOperationState.CONFLICT,
        CloudMoveOperationState.FAILED,
    ),
    CloudMoveOperationState.SUCCEEDED: (CloudMoveOperationState.SUCCEEDED,),
    CloudMoveOperationState.CONFLICT: (CloudMoveOperationState.CONFLICT,),
    CloudMoveOperationState.FAILED: (CloudMoveOperationState.FAILED,),
}


def validate_cloud_move_transition(
    current: CloudMoveOperationState | None,
    requested: CloudMoveOperationState,
) -> None:
    if requested not in _ALLOWED_CLOUD_MOVE_TRANSITIONS[current]:
        raise InvalidCloudMoveTransitionError(current, requested)


def normalize_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _without_plan(record: JobRecord) -> JobRecord:
    return JobRecord(
        job_id=record.job_id,
        operation=record.operation,
        state=record.state,
        created_at=record.created_at,
        updated_at=record.updated_at,
        error_code=record.error_code,
        owner_id=record.owner_id,
        lease_expires_at=record.lease_expires_at,
        actor_ids=record.actor_ids,
        progress=record.progress,
    )


def _replace_job(  # noqa: PLR0913
    record: JobRecord,
    *,
    state: JobState,
    updated_at: datetime,
    error_code: str | None = None,
    owner_id: str | None,
    lease_expires_at: datetime | None,
    progress: JobProgress | None = None,
) -> JobRecord:
    return JobRecord(
        job_id=record.job_id,
        operation=record.operation,
        state=state,
        created_at=record.created_at,
        updated_at=updated_at,
        plan_id=record.plan_id,
        error_code=error_code,
        owner_id=owner_id,
        lease_expires_at=lease_expires_at,
        actor_ids=record.actor_ids,
        progress=progress if progress is not None else record.progress,
    )


def _terminal_progress(progress: JobProgress | None, now: datetime) -> JobProgress:
    if progress is None:
        return JobProgress(
            stage=JobStage.DONE,
            completed=0,
            total=None,
            unit=JobProgressUnit.ITEMS,
            current=None,
            stage_started_at=now,
            updated_at=now,
        )
    return JobProgress(
        stage=JobStage.DONE,
        completed=progress.completed,
        total=progress.total,
        unit=progress.unit,
        current=progress.current,
        stage_started_at=now,
        updated_at=now,
    )
