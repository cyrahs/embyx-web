import asyncio
import sqlite3
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from embyx_web.fill_actor.models import (
    ActorPlan,
    FillActorPlan,
    MoveCandidate,
    MoveResult,
    MoveState,
    VideoPlan,
    VideoState,
)
from embyx_web.fill_actor.persistence import (
    JOB_CANCELLED_ERROR_CODE,
    CancelJobOutcome,
    CandidateRecord,
    FileFingerprint,
    InvalidMoveJournalTransitionError,
    JobFeedErrorCode,
    JobFeedRecord,
    JobFeedState,
    JobOperation,
    JobProgress,
    JobProgressUnit,
    JobRecord,
    JobStage,
    JobState,
    MemoryFillActorRepository,
    MoveJournalRecord,
    MoveJournalState,
    PlanRecord,
)
from embyx_web.fill_actor.sqlite_repository import (
    CURRENT_SCHEMA_VERSION,
    SQLiteFillActorRepository,
    UnsupportedSchemaVersionError,
)


def make_plan_record(tmp_path: Path, *, plan_id: str = 'plan-1') -> PlanRecord:
    created_at = datetime(2026, 7, 13, 10, 0, tzinfo=UTC)
    source_root = tmp_path / 'additional'
    destination_root = tmp_path / 'move-in'
    source_root.mkdir(exist_ok=True)
    destination_root.mkdir(exist_ok=True)
    source = source_root / 'ABC-001.mp4'
    source.write_bytes(b'video')
    destination = destination_root / source.name
    stat = source.stat()
    candidate = MoveCandidate(
        candidate_id='candidate-1',
        video_id='ABC-001',
        file_name=source.name,
        source_label='additional-1',
        destination_conflict=False,
    )
    public = FillActorPlan(
        plan_id=plan_id,
        revision='revision-1',
        created_at=created_at,
        expires_at=created_at + timedelta(hours=1),
        actors=(ActorPlan(actor_id='actor-1', scraped_count=1, video_ids=('ABC-001',)),),
        videos=(
            VideoPlan(
                video_id='ABC-001',
                actor_ids=('actor-1',),
                state=VideoState.ADDITIONAL_FOUND,
                move_candidates=(candidate,),
            ),
        ),
    )
    return PlanRecord(
        public=public,
        candidates=(
            CandidateRecord(
                candidate_id=candidate.candidate_id,
                video_id=candidate.video_id,
                source=source,
                source_root=source_root,
                destination=destination,
                fingerprint=FileFingerprint(
                    device=stat.st_dev,
                    inode=stat.st_ino,
                    size=stat.st_size,
                    mtime_ns=stat.st_mtime_ns,
                    ctime_ns=stat.st_ctime_ns,
                ),
            ),
        ),
    )


def make_repository(kind: str, tmp_path: Path):
    if kind == 'memory':
        return MemoryFillActorRepository()
    return SQLiteFillActorRepository(tmp_path / 'fill-actor.sqlite3')


def test_plan_record_rejects_private_candidate_mismatch(tmp_path: Path) -> None:
    plan = make_plan_record(tmp_path)
    with pytest.raises(ValueError, match='private candidate records must match public plan candidates'):
        PlanRecord(public=plan.public, candidates=())


@pytest.mark.asyncio
@pytest.mark.parametrize('repository_kind', ['memory', 'sqlite'])
async def test_repository_contract_round_trips_persistent_records(
    tmp_path: Path,
    repository_kind: str,
) -> None:
    repository = make_repository(repository_kind, tmp_path)
    plan = make_plan_record(tmp_path)
    now = plan.public.created_at

    queued_job = JobRecord(
        job_id='job-1',
        operation=JobOperation.CREATE_PLAN,
        state=JobState.QUEUED,
        created_at=now,
        updated_at=now,
        plan_id=plan.public.plan_id,
    )
    await repository.save_job(queued_job)
    assert await repository.get_job(queued_job.job_id) == queued_job
    assert await repository.list_jobs((JobState.QUEUED,)) == (queued_job,)

    await repository.save_plan(plan)

    loaded_plan = await repository.get_plan(plan.public.plan_id)
    assert loaded_plan == plan
    assert await repository.get_candidate(plan.public.plan_id, 'candidate-1') == plan.candidates[0]
    assert str(plan.candidates[0].source) not in loaded_plan.public.model_dump_json()
    assert await repository.get_plan('unknown') is None

    result = MoveResult(
        candidate_id='candidate-1',
        video_id='ABC-001',
        file_name='ABC-001.mp4',
        state=MoveState.MOVED,
    )
    await repository.save_move_result(plan.public.plan_id, result)
    assert await repository.get_move_result(plan.public.plan_id, result.candidate_id) == result
    assert await repository.list_move_results(plan.public.plan_id) == (result,)

    job = JobRecord(
        job_id='job-1',
        operation=JobOperation.APPLY,
        state=JobState.RUNNING,
        created_at=now,
        updated_at=now,
        plan_id=plan.public.plan_id,
    )
    await repository.save_job(job)
    assert await repository.get_job(job.job_id) == job
    assert await repository.list_jobs((JobState.QUEUED,)) == ()
    assert await repository.list_jobs((JobState.RUNNING,)) == (job,)
    assert await repository.list_jobs() == (job,)
    assert await repository.health_check()

    journal = MoveJournalRecord(
        plan_id=plan.public.plan_id,
        candidate_id='candidate-1',
        state=MoveJournalState.PREPARED,
        updated_at=now,
    )
    await repository.save_move_journal(journal)
    assert await repository.get_move_journal(plan.public.plan_id, 'candidate-1') == journal
    assert await repository.list_unreconciled_moves() == (journal,)


@pytest.mark.asyncio
@pytest.mark.parametrize('repository_kind', ['memory', 'sqlite'])
async def test_repository_contract_enforces_move_journal_transitions(
    tmp_path: Path,
    repository_kind: str,
) -> None:
    repository = make_repository(repository_kind, tmp_path)
    plan = make_plan_record(tmp_path)
    await repository.save_plan(plan)
    now = plan.public.created_at

    for offset, state in enumerate(
        (
            MoveJournalState.PREPARED,
            MoveJournalState.LINKED,
            MoveJournalState.SOURCE_REMOVED,
            MoveJournalState.RECONCILED,
        )
    ):
        record = MoveJournalRecord(
            plan_id=plan.public.plan_id,
            candidate_id='candidate-1',
            state=state,
            updated_at=now + timedelta(seconds=offset),
        )
        await repository.save_move_journal(record)
        assert await repository.get_move_journal(plan.public.plan_id, 'candidate-1') == record

    assert await repository.list_unreconciled_moves() == ()
    with pytest.raises(InvalidMoveJournalTransitionError):
        await repository.save_move_journal(
            MoveJournalRecord(
                plan_id=plan.public.plan_id,
                candidate_id='candidate-1',
                state=MoveJournalState.LINKED,
                updated_at=now + timedelta(seconds=5),
            )
        )


@pytest.mark.asyncio
@pytest.mark.parametrize('repository_kind', ['memory', 'sqlite'])
async def test_repository_contract_purges_expired_plan_dependents(
    tmp_path: Path,
    repository_kind: str,
) -> None:
    repository = make_repository(repository_kind, tmp_path)
    plan = make_plan_record(tmp_path)
    await repository.save_plan(plan)
    await repository.save_move_result(
        plan.public.plan_id,
        MoveResult(
            candidate_id='candidate-1',
            video_id='ABC-001',
            file_name='ABC-001.mp4',
            state=MoveState.MOVED,
        ),
    )
    await repository.save_move_journal(
        MoveJournalRecord(
            plan_id=plan.public.plan_id,
            candidate_id='candidate-1',
            state=MoveJournalState.PREPARED,
            updated_at=plan.public.created_at,
        )
    )
    job = JobRecord(
        job_id='job-1',
        operation=JobOperation.APPLY,
        state=JobState.COMPLETED,
        created_at=plan.public.created_at,
        updated_at=plan.public.created_at,
        plan_id=plan.public.plan_id,
    )
    await repository.save_job(job)

    assert await repository.purge_expired_plans(plan.public.expires_at) == 0
    assert await repository.get_plan(plan.public.plan_id) is not None
    await repository.save_move_journal(
        MoveJournalRecord(
            plan_id=plan.public.plan_id,
            candidate_id='candidate-1',
            state=MoveJournalState.RECONCILED,
            updated_at=plan.public.expires_at,
        )
    )
    assert await repository.purge_expired_plans(plan.public.expires_at) == 1
    assert await repository.get_plan(plan.public.plan_id) is None
    assert await repository.get_move_result(plan.public.plan_id, 'candidate-1') is None
    assert await repository.get_move_journal(plan.public.plan_id, 'candidate-1') is None
    assert await repository.get_job(job.job_id) == JobRecord(
        job_id=job.job_id,
        operation=job.operation,
        state=job.state,
        created_at=job.created_at,
        updated_at=job.updated_at,
        error_code=job.error_code,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize('repository_kind', ['memory', 'sqlite'])
async def test_repository_refuses_to_delete_plan_with_unreconciled_move(
    tmp_path: Path,
    repository_kind: str,
) -> None:
    repository = make_repository(repository_kind, tmp_path)
    plan = make_plan_record(tmp_path)
    await repository.save_plan(plan)
    await repository.save_move_journal(
        MoveJournalRecord(
            plan_id=plan.public.plan_id,
            candidate_id='candidate-1',
            state=MoveJournalState.PREPARED,
            updated_at=plan.public.created_at,
        )
    )

    assert not await repository.delete_plan(plan.public.plan_id)
    assert await repository.get_plan(plan.public.plan_id) == plan
    assert len(await repository.list_unreconciled_moves()) == 1


@pytest.mark.asyncio
async def test_sqlite_repository_survives_reopen(tmp_path: Path) -> None:
    database_path = tmp_path / 'fill-actor.sqlite3'
    plan = make_plan_record(tmp_path)
    now = plan.public.created_at
    repository = SQLiteFillActorRepository(database_path)
    result = MoveResult(
        candidate_id='candidate-1',
        video_id='ABC-001',
        file_name='ABC-001.mp4',
        state=MoveState.MOVED,
    )
    job = JobRecord(
        job_id='job-1',
        operation=JobOperation.APPLY,
        state=JobState.PARTIAL_FAILED,
        created_at=now,
        updated_at=now + timedelta(seconds=1),
        plan_id=plan.public.plan_id,
        error_code='one_or_more_moves_failed',
    )
    journal = MoveJournalRecord(
        plan_id=plan.public.plan_id,
        candidate_id='candidate-1',
        state=MoveJournalState.PREPARED,
        updated_at=now,
    )
    await repository.save_plan(plan)
    await repository.save_move_result(plan.public.plan_id, result)
    await repository.save_job(job)
    await repository.save_move_journal(journal)

    reopened = SQLiteFillActorRepository(database_path)

    assert await reopened.get_plan(plan.public.plan_id) == plan
    assert await reopened.get_move_result(plan.public.plan_id, 'candidate-1') == result
    assert await reopened.get_job(job.job_id) == job
    assert await reopened.list_unreconciled_moves() == (journal,)


def test_sqlite_repository_applies_explicit_schema_migration(tmp_path: Path) -> None:
    database_path = tmp_path / 'fill-actor.sqlite3'

    SQLiteFillActorRepository(database_path)

    with sqlite3.connect(database_path) as connection:
        version = connection.execute('SELECT MAX(version) FROM schema_migrations').fetchone()[0]
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        jobs_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'jobs'"
        ).fetchone()[0]
    assert version == CURRENT_SCHEMA_VERSION
    assert tables == {
        'schema_migrations',
        'plans',
        'candidates',
        'move_results',
        'jobs',
        'job_feeds',
        'move_journal',
        'health_probe',
    }
    assert "'pages'" in jobs_sql


def test_sqlite_repository_migrates_v2_jobs_with_compatible_progress(tmp_path: Path) -> None:
    database_path = tmp_path / 'fill-actor-v2.sqlite3'
    created_at = datetime(2026, 7, 13, 10, 0, tzinfo=UTC).isoformat()
    with sqlite3.connect(database_path) as connection:
        connection.execute('CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)')
        connection.executemany(
            'INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)',
            ((1, created_at), (2, created_at)),
        )
        connection.execute(
            """
            CREATE TABLE jobs (
                job_id TEXT PRIMARY KEY, operation TEXT NOT NULL, state TEXT NOT NULL,
                created_at TEXT NOT NULL, updated_at TEXT NOT NULL, plan_id TEXT, error_code TEXT,
                owner_id TEXT, lease_expires_at TEXT, actor_ids_json TEXT NOT NULL DEFAULT '[]'
            )
            """
        )
        connection.executemany(
            """
            INSERT INTO jobs (
                job_id, operation, state, created_at, updated_at, plan_id, error_code,
                owner_id, lease_expires_at, actor_ids_json
            ) VALUES (?, 'create_plan', ?, ?, ?, ?, NULL, ?, ?, ?)
            """,
            (
                ('queued', 'queued', created_at, created_at, 'queued', None, None, '["a", "b"]'),
                ('running', 'running', created_at, created_at, 'running', 'owner', created_at, '["a"]'),
                ('completed', 'completed', created_at, created_at, 'completed', None, None, '["a"]'),
            ),
        )

    repository = SQLiteFillActorRepository(database_path)
    queued = asyncio.run(repository.get_job('queued'))
    running = asyncio.run(repository.get_job('running'))
    completed = asyncio.run(repository.get_job('completed'))

    assert queued is not None
    assert queued.progress is not None
    assert queued.progress.stage is JobStage.QUEUED
    assert queued.progress.total == 2
    assert queued.progress.unit is JobProgressUnit.ACTORS
    assert running is not None
    assert running.progress is not None
    assert running.progress.stage is JobStage.UNKNOWN
    assert completed is not None
    assert completed.progress is not None
    assert completed.progress.stage is JobStage.DONE


@pytest.mark.asyncio
@pytest.mark.parametrize('repository_kind', ['memory', 'sqlite'])
async def test_lease_and_progress_updates_do_not_overwrite_each_other(
    tmp_path: Path,
    repository_kind: str,
) -> None:
    repository = make_repository(repository_kind, tmp_path)
    now = datetime(2026, 7, 13, 10, 0, tzinfo=UTC)
    queued = JobRecord(
        job_id='progress-job',
        plan_id='progress-job',
        operation=JobOperation.CREATE_PLAN,
        state=JobState.QUEUED,
        created_at=now,
        updated_at=now,
        actor_ids=('actor',),
    )
    assert await repository.enqueue_job(queued, max_active=1)
    claimed = await repository.claim_next_job(
        owner_id='owner',
        now=now,
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert claimed is not None
    progress = JobProgress(
        stage=JobStage.LIBRARY_SCAN,
        completed=4,
        total=10,
        unit=JobProgressUnit.VIDEOS,
        current='ABC-004',
        stage_started_at=now + timedelta(seconds=1),
        updated_at=now + timedelta(seconds=5),
    )

    progress_saved, lease_saved = await asyncio.gather(
        repository.update_owned_job_progress(
            job_id=claimed.job_id,
            owner_id='owner',
            progress=progress,
            now=now + timedelta(seconds=6),
        ),
        repository.renew_owned_job_lease(
            job_id=claimed.job_id,
            owner_id='owner',
            now=now + timedelta(seconds=6),
            lease_expires_at=now + timedelta(seconds=36),
        ),
    )

    assert progress_saved
    assert lease_saved
    current = await repository.get_job(claimed.job_id)
    assert current is not None
    assert current.progress == progress
    assert current.updated_at == now + timedelta(seconds=6)
    assert current.lease_expires_at == now + timedelta(seconds=36)
    assert not await repository.update_owned_job_progress(
        job_id=claimed.job_id,
        owner_id='other-owner',
        progress=progress,
        now=now + timedelta(seconds=7),
    )
    assert not await repository.update_owned_job_progress(
        job_id=claimed.job_id,
        owner_id='owner',
        progress=progress,
        now=now + timedelta(seconds=37),
    )
    after_lease = JobProgress(
        stage=JobStage.PERSISTING,
        completed=1,
        total=1,
        unit=JobProgressUnit.STEPS,
        current='saved too late',
        stage_started_at=now + timedelta(seconds=30),
        updated_at=now + timedelta(seconds=37),
    )
    assert not await repository.update_owned_job_progress(
        job_id=claimed.job_id,
        owner_id='owner',
        progress=after_lease,
        now=now + timedelta(seconds=37),
    )
    assert not await repository.finish_owned_job(
        job_id=claimed.job_id,
        owner_id='owner',
        state=JobState.COMPLETED,
        error_code=None,
        now=now + timedelta(seconds=37),
        progress=after_lease,
    )
    still_running = await repository.get_job(claimed.job_id)
    assert still_running is not None
    assert still_running.state is JobState.RUNNING


@pytest.mark.asyncio
@pytest.mark.parametrize('repository_kind', ['memory', 'sqlite'])
async def test_job_feed_updates_require_current_owner_and_unexpired_lease(
    tmp_path: Path,
    repository_kind: str,
) -> None:
    repository = make_repository(repository_kind, tmp_path)
    now = datetime(2026, 7, 13, 10, 0, tzinfo=UTC)
    queued = JobRecord(
        job_id='feed-job',
        plan_id='feed-job',
        operation=JobOperation.CREATE_PLAN,
        state=JobState.QUEUED,
        created_at=now,
        updated_at=now,
        actor_ids=('actor',),
    )
    feed = JobFeedRecord(
        job_id=queued.job_id,
        actor_id='actor',
        state=JobFeedState.QUEUED,
        attempts=0,
        updated_at=now,
        freshrss_add_url='https://rss.example/i/?c=feed&a=add',
    )
    assert await repository.enqueue_job(queued, max_active=1, feeds=(feed,))
    claimed = await repository.claim_next_job(
        owner_id='current-owner',
        now=now,
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert claimed is not None

    assert await repository.update_owned_job_feed(
        job_id=queued.job_id,
        actor_id='actor',
        owner_id='current-owner',
        state=JobFeedState.WARMING,
        attempts=1,
        error_code=None,
        now=now + timedelta(seconds=1),
    )
    assert not await repository.update_owned_job_feed(
        job_id=queued.job_id,
        actor_id='actor',
        owner_id='stale-owner',
        state=JobFeedState.READY,
        attempts=2,
        error_code=None,
        now=now + timedelta(seconds=2),
    )
    assert not await repository.update_owned_job_feed(
        job_id=queued.job_id,
        actor_id='actor',
        owner_id='current-owner',
        state=JobFeedState.READY,
        attempts=2,
        error_code=None,
        now=now + timedelta(seconds=31),
    )

    assert await repository.fail_expired_jobs(now=now + timedelta(seconds=31), error_code='job_interrupted') == 1
    saved = (await repository.list_job_feeds(queued.job_id))[0]
    assert saved.state is JobFeedState.FAILED
    assert saved.attempts == 1
    assert saved.error_code is JobFeedErrorCode.CANCELLED
    assert saved.freshrss_add_url == feed.freshrss_add_url


@pytest.mark.asyncio
@pytest.mark.parametrize('repository_kind', ['memory', 'sqlite'])
async def test_cancel_job_atomically_terminalizes_running_job_and_pending_feeds(
    tmp_path: Path,
    repository_kind: str,
) -> None:
    repository = make_repository(repository_kind, tmp_path)
    now = datetime(2026, 7, 13, 10, 0, tzinfo=UTC)
    queued = JobRecord(
        job_id='cancel-job',
        plan_id='cancel-job',
        operation=JobOperation.CREATE_PLAN,
        state=JobState.QUEUED,
        created_at=now,
        updated_at=now,
        actor_ids=('pending', 'ready'),
    )
    feeds = (
        JobFeedRecord(
            job_id=queued.job_id,
            actor_id='pending',
            state=JobFeedState.QUEUED,
            attempts=0,
            updated_at=now,
        ),
        JobFeedRecord(
            job_id=queued.job_id,
            actor_id='ready',
            state=JobFeedState.QUEUED,
            attempts=0,
            updated_at=now,
        ),
    )
    assert await repository.enqueue_job(queued, max_active=1, feeds=feeds)
    claimed = await repository.claim_next_job(
        owner_id='owner',
        now=now,
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert claimed is not None
    assert await repository.update_owned_job_feed(
        job_id=queued.job_id,
        actor_id='pending',
        owner_id='owner',
        state=JobFeedState.WARMING,
        attempts=1,
        error_code=None,
        now=now + timedelta(seconds=1),
    )
    assert await repository.update_owned_job_feed(
        job_id=queued.job_id,
        actor_id='ready',
        owner_id='owner',
        state=JobFeedState.READY,
        attempts=1,
        error_code=None,
        now=now + timedelta(seconds=1),
    )

    cancelled_at = now + timedelta(seconds=2)
    result = await repository.cancel_job(job_id=queued.job_id, now=cancelled_at)

    assert result.outcome is CancelJobOutcome.CANCELLED
    assert result.previous_state is JobState.RUNNING
    assert result.previous_owner_id == 'owner'
    assert result.job is not None
    assert result.job.state is JobState.FAILED
    assert result.job.error_code == JOB_CANCELLED_ERROR_CODE
    assert result.job.owner_id is None
    assert result.job.lease_expires_at is None
    assert result.job.progress is not None
    assert result.job.progress.stage is JobStage.DONE
    saved_feeds = {feed.actor_id: feed for feed in await repository.list_job_feeds(queued.job_id)}
    assert saved_feeds['pending'].state is JobFeedState.FAILED
    assert saved_feeds['pending'].error_code is JobFeedErrorCode.CANCELLED
    assert saved_feeds['ready'].state is JobFeedState.READY
    assert saved_feeds['ready'].error_code is None
    assert not await repository.renew_owned_job_lease(
        job_id=queued.job_id,
        owner_id='owner',
        now=cancelled_at,
        lease_expires_at=cancelled_at + timedelta(seconds=30),
    )
    assert not await repository.finish_owned_job(
        job_id=queued.job_id,
        owner_id='owner',
        state=JobState.COMPLETED,
        error_code=None,
        now=cancelled_at,
        progress=result.job.progress,
    )
    repeated = await repository.cancel_job(job_id=queued.job_id, now=cancelled_at + timedelta(seconds=1))
    assert repeated.outcome is CancelJobOutcome.ALREADY_CANCELLED
    assert repeated.job == result.job


@pytest.mark.asyncio
@pytest.mark.parametrize('repository_kind', ['memory', 'sqlite'])
async def test_cancel_queued_job_prevents_claim_and_releases_capacity(tmp_path: Path, repository_kind: str) -> None:
    repository = make_repository(repository_kind, tmp_path)
    now = datetime(2026, 7, 13, 10, 0, tzinfo=UTC)
    queued = JobRecord(
        job_id='queued-cancel',
        plan_id='queued-cancel',
        operation=JobOperation.CREATE_PLAN,
        state=JobState.QUEUED,
        created_at=now,
        updated_at=now,
    )
    assert await repository.enqueue_job(queued, max_active=1)

    result = await repository.cancel_job(job_id=queued.job_id, now=now + timedelta(seconds=1))

    assert result.outcome is CancelJobOutcome.CANCELLED
    assert result.previous_state is JobState.QUEUED
    assert (
        await repository.claim_next_job(
            owner_id='owner',
            now=now + timedelta(seconds=2),
            lease_expires_at=now + timedelta(seconds=32),
        )
        is None
    )
    replacement = JobRecord(
        job_id='replacement',
        operation=JobOperation.CREATE_PLAN,
        state=JobState.QUEUED,
        created_at=now + timedelta(seconds=2),
        updated_at=now + timedelta(seconds=2),
    )
    assert await repository.enqueue_job(replacement, max_active=1)


@pytest.mark.asyncio
@pytest.mark.parametrize('repository_kind', ['memory', 'sqlite'])
async def test_cancel_and_finish_have_one_atomic_winner(tmp_path: Path, repository_kind: str) -> None:
    repository = make_repository(repository_kind, tmp_path)
    now = datetime(2026, 7, 13, 10, 0, tzinfo=UTC)
    queued = JobRecord(
        job_id='finish-race',
        plan_id='finish-race',
        operation=JobOperation.CREATE_PLAN,
        state=JobState.QUEUED,
        created_at=now,
        updated_at=now,
    )
    assert await repository.enqueue_job(queued, max_active=1)
    claimed = await repository.claim_next_job(
        owner_id='owner',
        now=now,
        lease_expires_at=now + timedelta(seconds=30),
    )
    assert claimed is not None
    assert claimed.progress is not None

    cancel_result, finished = await asyncio.gather(
        repository.cancel_job(job_id=queued.job_id, now=now + timedelta(seconds=1)),
        repository.finish_owned_job(
            job_id=queued.job_id,
            owner_id='owner',
            state=JobState.COMPLETED,
            error_code=None,
            now=now + timedelta(seconds=1),
            progress=claimed.progress,
        ),
    )
    current = await repository.get_job(queued.job_id)
    assert current is not None
    if cancel_result.outcome is CancelJobOutcome.CANCELLED:
        assert not finished
        assert current.state is JobState.FAILED
        assert current.error_code == JOB_CANCELLED_ERROR_CODE
    else:
        assert cancel_result.outcome is CancelJobOutcome.ALREADY_TERMINAL
        assert finished
        assert current.state is JobState.COMPLETED


def test_sqlite_repository_rejects_unknown_future_schema(tmp_path: Path) -> None:
    database_path = tmp_path / 'fill-actor.sqlite3'
    with sqlite3.connect(database_path) as connection:
        connection.execute('CREATE TABLE schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)')
        connection.execute(
            'INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)',
            (CURRENT_SCHEMA_VERSION + 1, datetime.now(tz=UTC).isoformat()),
        )

    with pytest.raises(UnsupportedSchemaVersionError):
        SQLiteFillActorRepository(database_path)


def test_job_states_are_stable_public_values() -> None:
    assert tuple(JobState) == (
        JobState.QUEUED,
        JobState.RUNNING,
        JobState.COMPLETED,
        JobState.PARTIAL_FAILED,
        JobState.FAILED,
    )


@pytest.mark.asyncio
async def test_sqlite_write_finishes_before_cancellation_propagates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = SQLiteFillActorRepository(tmp_path / 'fill-actor.sqlite3')
    now = datetime.now(UTC)
    job = JobRecord(
        job_id='job-1',
        operation=JobOperation.CREATE_PLAN,
        state=JobState.QUEUED,
        created_at=now,
        updated_at=now,
    )
    started = threading.Event()
    release = threading.Event()
    original_save = repository._save_job  # noqa: SLF001

    def delayed_save(record: JobRecord) -> None:
        started.set()
        assert release.wait(timeout=2)
        original_save(record)

    monkeypatch.setattr(repository, '_save_job', delayed_save)
    task = asyncio.create_task(repository.save_job(job))
    assert await asyncio.to_thread(started.wait, 2)
    task.cancel()
    await asyncio.sleep(0.02)
    assert not task.done()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert await repository.get_job(job.job_id) == job
