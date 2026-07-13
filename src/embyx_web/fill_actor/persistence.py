import asyncio
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Protocol

from embyx_web.fill_actor.models import FillActorPlan, MoveResult


class JobState(StrEnum):
    QUEUED = 'queued'
    RUNNING = 'running'
    COMPLETED = 'completed'
    PARTIAL_FAILED = 'partial_failed'
    FAILED = 'failed'


class JobOperation(StrEnum):
    CREATE_PLAN = 'create_plan'
    APPLY = 'apply'


class MoveJournalState(StrEnum):
    PREPARED = 'prepared'
    LINKED = 'linked'
    SOURCE_REMOVED = 'source_removed'
    RECONCILED = 'reconciled'


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


@dataclass(frozen=True)
class MoveJournalRecord:
    plan_id: str
    candidate_id: str
    state: MoveJournalState
    updated_at: datetime


class InvalidMoveJournalTransitionError(ValueError):
    def __init__(self, current: MoveJournalState | None, requested: MoveJournalState) -> None:
        current_value = current.value if current is not None else 'none'
        super().__init__(f'invalid move journal transition: {current_value} -> {requested.value}')


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

    async def enqueue_job(self, record: JobRecord, *, max_active: int) -> bool: ...

    async def claim_next_job(
        self,
        *,
        owner_id: str,
        now: datetime,
        lease_expires_at: datetime,
    ) -> JobRecord | None: ...

    async def update_owned_job(
        self,
        record: JobRecord,
        *,
        owner_id: str,
        expected_states: Sequence[JobState],
    ) -> bool: ...

    async def fail_expired_jobs(self, *, now: datetime, error_code: str) -> int: ...

    async def get_job(self, job_id: str) -> JobRecord | None: ...

    async def list_jobs(self, states: Sequence[JobState] | None = None) -> tuple[JobRecord, ...]: ...

    async def save_move_journal(self, record: MoveJournalRecord) -> None: ...

    async def get_move_journal(self, plan_id: str, candidate_id: str) -> MoveJournalRecord | None: ...

    async def list_unreconciled_moves(self) -> tuple[MoveJournalRecord, ...]: ...

    async def health_check(self) -> bool: ...


class MemoryFillActorRepository:
    def __init__(self) -> None:
        self._plans: dict[str, PlanRecord] = {}
        self._move_results: dict[tuple[str, str], MoveResult] = {}
        self._jobs: dict[str, JobRecord] = {}
        self._move_journal: dict[tuple[str, str], MoveJournalRecord] = {}
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

    async def enqueue_job(self, record: JobRecord, *, max_active: int) -> bool:
        async with self._lock:
            active = sum(job.state in {JobState.QUEUED, JobState.RUNNING} for job in self._jobs.values())
            if active >= max_active or record.job_id in self._jobs:
                return False
            self._jobs[record.job_id] = record
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

    async def update_owned_job(
        self,
        record: JobRecord,
        *,
        owner_id: str,
        expected_states: Sequence[JobState],
    ) -> bool:
        async with self._lock:
            current = self._jobs.get(record.job_id)
            if current is None or current.owner_id != owner_id or current.state not in expected_states:
                return False
            self._jobs[record.job_id] = record
            return True

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

    async def health_check(self) -> bool:
        return True

    def _delete_plan_dependents(self, plan_id: str) -> None:
        self._move_results = {key: result for key, result in self._move_results.items() if key[0] != plan_id}
        self._move_journal = {key: record for key, record in self._move_journal.items() if key[0] != plan_id}
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
    )


def _replace_job(  # noqa: PLR0913
    record: JobRecord,
    *,
    state: JobState,
    updated_at: datetime,
    error_code: str | None = None,
    owner_id: str | None,
    lease_expires_at: datetime | None,
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
    )
