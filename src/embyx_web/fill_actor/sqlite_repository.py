import asyncio
import json
import sqlite3
from collections.abc import Callable, Sequence
from contextlib import closing
from datetime import datetime
from pathlib import Path

from embyx_web.fill_actor.models import FillActorPlan, MoveResult
from embyx_web.fill_actor.persistence import (
    JOB_CANCELLED_ERROR_CODE,
    CancelJobOutcome,
    CancelJobResult,
    CandidateRecord,
    FileFingerprint,
    JobFeedErrorCode,
    JobFeedRecord,
    JobFeedState,
    JobOperation,
    JobProgress,
    JobProgressUnit,
    JobRecord,
    JobStage,
    JobState,
    MoveJournalRecord,
    MoveJournalState,
    PlanRecord,
    normalize_datetime,
    validate_journal_transition,
)

CURRENT_SCHEMA_VERSION = 4


async def _run_sync[ResultT](function: Callable[..., ResultT], *args: object) -> ResultT:
    """Finish a SQLite operation before propagating caller cancellation."""
    task = asyncio.create_task(asyncio.to_thread(function, *args))
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    result = task.result()
    if cancelled:
        raise asyncio.CancelledError
    return result


class UnsupportedSchemaVersionError(RuntimeError):
    def __init__(self, version: int) -> None:
        super().__init__(f'unsupported fill-actor database schema version: {version}')


_MIGRATIONS: dict[int, tuple[str, ...]] = {
    1: (
        """
        CREATE TABLE plans (
            plan_id TEXT PRIMARY KEY,
            revision TEXT NOT NULL,
            public_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        )
        """,
        'CREATE INDEX plans_expires_at_idx ON plans (expires_at)',
        """
        CREATE TABLE candidates (
            plan_id TEXT NOT NULL,
            candidate_id TEXT NOT NULL,
            video_id TEXT NOT NULL,
            source_path TEXT NOT NULL,
            source_root TEXT NOT NULL,
            destination_path TEXT NOT NULL,
            fingerprint_device INTEGER NOT NULL,
            fingerprint_inode INTEGER NOT NULL,
            fingerprint_size INTEGER NOT NULL,
            fingerprint_mtime_ns INTEGER NOT NULL,
            fingerprint_ctime_ns INTEGER NOT NULL,
            PRIMARY KEY (plan_id, candidate_id),
            FOREIGN KEY (plan_id) REFERENCES plans (plan_id) ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE move_results (
            plan_id TEXT NOT NULL,
            candidate_id TEXT NOT NULL,
            result_json TEXT NOT NULL,
            PRIMARY KEY (plan_id, candidate_id),
            FOREIGN KEY (plan_id, candidate_id)
                REFERENCES candidates (plan_id, candidate_id) ON DELETE CASCADE
        )
        """,
        """
        CREATE TABLE jobs (
            job_id TEXT PRIMARY KEY,
            operation TEXT NOT NULL CHECK (operation IN ('create_plan', 'apply')),
            state TEXT NOT NULL CHECK (state IN ('queued', 'running', 'completed', 'partial_failed', 'failed')),
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            plan_id TEXT,
            error_code TEXT
        )
        """,
        'CREATE INDEX jobs_plan_id_idx ON jobs (plan_id)',
        """
        CREATE TABLE move_journal (
            plan_id TEXT NOT NULL,
            candidate_id TEXT NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('prepared', 'linked', 'source_removed', 'reconciled')),
            updated_at TEXT NOT NULL,
            PRIMARY KEY (plan_id, candidate_id),
            FOREIGN KEY (plan_id, candidate_id)
                REFERENCES candidates (plan_id, candidate_id) ON DELETE CASCADE
        )
        """,
        'CREATE INDEX move_journal_state_idx ON move_journal (state, updated_at)',
    ),
    2: (
        'ALTER TABLE jobs ADD COLUMN owner_id TEXT',
        'ALTER TABLE jobs ADD COLUMN lease_expires_at TEXT',
        "ALTER TABLE jobs ADD COLUMN actor_ids_json TEXT NOT NULL DEFAULT '[]'",
        'CREATE INDEX jobs_lease_idx ON jobs (state, lease_expires_at)',
        'CREATE TABLE health_probe (id INTEGER PRIMARY KEY CHECK (id = 1), checked_at TEXT NOT NULL)',
    ),
    3: (
        """
        ALTER TABLE jobs ADD COLUMN progress_stage TEXT NOT NULL DEFAULT 'queued'
        CHECK (progress_stage IN (
            'queued', 'actor_catalog', 'library_scan', 'magnet_lookup', 'persisting', 'done', 'unknown'
        ))
        """,
        'ALTER TABLE jobs ADD COLUMN progress_completed INTEGER NOT NULL DEFAULT 0 CHECK (progress_completed >= 0)',
        'ALTER TABLE jobs ADD COLUMN progress_total INTEGER CHECK (progress_total IS NULL OR progress_total >= 0)',
        """
        ALTER TABLE jobs ADD COLUMN progress_unit TEXT NOT NULL DEFAULT 'items'
        CHECK (progress_unit IN ('actors', 'pages', 'videos', 'magnets', 'steps', 'items'))
        """,
        'ALTER TABLE jobs ADD COLUMN progress_current TEXT',
        "ALTER TABLE jobs ADD COLUMN progress_stage_started_at TEXT NOT NULL DEFAULT '1970-01-01T00:00:00+00:00'",
        "ALTER TABLE jobs ADD COLUMN progress_updated_at TEXT NOT NULL DEFAULT '1970-01-01T00:00:00+00:00'",
        """
        UPDATE jobs SET
            progress_stage = CASE
                WHEN state = 'queued' THEN 'queued'
                WHEN state = 'running' THEN 'unknown'
                ELSE 'done'
            END,
            progress_total = CASE
                WHEN state = 'queued' THEN json_array_length(actor_ids_json)
                ELSE NULL
            END,
            progress_unit = CASE WHEN state = 'queued' THEN 'actors' ELSE 'items' END,
            progress_stage_started_at = created_at,
            progress_updated_at = updated_at
        """,
    ),
    4: (
        """
        CREATE TABLE job_feeds (
            job_id TEXT NOT NULL,
            actor_id TEXT NOT NULL,
            state TEXT NOT NULL CHECK (state IN ('queued', 'warming', 'ready', 'failed')),
            attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
            updated_at TEXT NOT NULL,
            error_code TEXT CHECK (error_code IS NULL OR error_code IN (
                'rsshub_timeout', 'rsshub_network_error', 'rsshub_http_error',
                'rsshub_invalid_feed', 'rsshub_not_ready', 'rsshub_cancelled'
            )),
            freshrss_add_url TEXT,
            CHECK (
                (state = 'failed' AND error_code IS NOT NULL)
                OR (state != 'failed' AND error_code IS NULL)
            ),
            PRIMARY KEY (job_id, actor_id),
            FOREIGN KEY (job_id) REFERENCES jobs (job_id) ON DELETE CASCADE
        )
        """,
        'CREATE INDEX job_feeds_state_idx ON job_feeds (state, updated_at)',
    ),
}


class SQLiteFillActorRepository:
    def __init__(self, database_path: Path) -> None:
        self._database_path = database_path
        self._database_path.parent.mkdir(parents=True, exist_ok=True)
        self._migrate()

    async def save_plan(self, record: PlanRecord) -> None:
        await _run_sync(self._save_plan, record)

    async def get_plan(self, plan_id: str) -> PlanRecord | None:
        return await _run_sync(self._get_plan, plan_id)

    async def get_candidate(self, plan_id: str, candidate_id: str) -> CandidateRecord | None:
        return await _run_sync(self._get_candidate, plan_id, candidate_id)

    async def delete_plan(self, plan_id: str) -> bool:
        return await _run_sync(self._delete_plan, plan_id)

    async def purge_expired_plans(self, now: datetime) -> int:
        return await _run_sync(self._purge_expired_plans, now)

    async def save_move_result(self, plan_id: str, result: MoveResult) -> None:
        await _run_sync(self._save_move_result, plan_id, result)

    async def get_move_result(self, plan_id: str, candidate_id: str) -> MoveResult | None:
        return await _run_sync(self._get_move_result, plan_id, candidate_id)

    async def list_move_results(self, plan_id: str) -> tuple[MoveResult, ...]:
        return await _run_sync(self._list_move_results, plan_id)

    async def save_job(self, record: JobRecord) -> None:
        await _run_sync(self._save_job, record)

    async def enqueue_job(
        self,
        record: JobRecord,
        *,
        max_active: int,
        feeds: Sequence[JobFeedRecord] = (),
    ) -> bool:
        return await _run_sync(self._enqueue_job, record, max_active, feeds)

    async def claim_next_job(
        self,
        *,
        owner_id: str,
        now: datetime,
        lease_expires_at: datetime,
    ) -> JobRecord | None:
        return await _run_sync(self._claim_next_job, owner_id, now, lease_expires_at)

    async def renew_owned_job_lease(
        self,
        *,
        job_id: str,
        owner_id: str,
        now: datetime,
        lease_expires_at: datetime,
    ) -> bool:
        return await _run_sync(self._renew_owned_job_lease, job_id, owner_id, now, lease_expires_at)

    async def update_owned_job_progress(
        self,
        *,
        job_id: str,
        owner_id: str,
        progress: JobProgress,
        now: datetime,
    ) -> bool:
        return await _run_sync(self._update_owned_job_progress, job_id, owner_id, progress, now)

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
        return await _run_sync(self._finish_owned_job, job_id, owner_id, state, error_code, now, progress)

    async def cancel_job(self, *, job_id: str, now: datetime) -> CancelJobResult:
        return await _run_sync(self._cancel_job, job_id, now)

    async def fail_expired_jobs(self, *, now: datetime, error_code: str) -> int:
        return await _run_sync(self._fail_expired_jobs, now, error_code)

    async def get_job(self, job_id: str) -> JobRecord | None:
        return await _run_sync(self._get_job, job_id)

    async def list_jobs(self, states: Sequence[JobState] | None = None) -> tuple[JobRecord, ...]:
        return await _run_sync(self._list_jobs, states)

    async def list_job_feeds(self, job_id: str) -> tuple[JobFeedRecord, ...]:
        return await _run_sync(self._list_job_feeds, job_id)

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
        return await _run_sync(
            self._update_owned_job_feed,
            job_id,
            actor_id,
            owner_id,
            state,
            attempts,
            error_code,
            now,
        )

    async def save_move_journal(self, record: MoveJournalRecord) -> None:
        await _run_sync(self._save_move_journal, record)

    async def get_move_journal(self, plan_id: str, candidate_id: str) -> MoveJournalRecord | None:
        return await _run_sync(self._get_move_journal, plan_id, candidate_id)

    async def list_unreconciled_moves(self) -> tuple[MoveJournalRecord, ...]:
        return await _run_sync(self._list_unreconciled_moves)

    async def health_check(self) -> bool:
        return await _run_sync(self._health_check)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self._database_path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute('PRAGMA foreign_keys = ON')
        connection.execute('PRAGMA busy_timeout = 10000')
        return connection

    def _migrate(self) -> None:
        with closing(self._connect()) as connection, connection:
            connection.execute('PRAGMA journal_mode = WAL')
            connection.execute('PRAGMA synchronous = NORMAL')
            connection.execute('BEGIN EXCLUSIVE')
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
                """
            )
            rows = connection.execute('SELECT version FROM schema_migrations ORDER BY version').fetchall()
            applied = [int(row['version']) for row in rows]
            if any(version > CURRENT_SCHEMA_VERSION for version in applied):
                raise UnsupportedSchemaVersionError(max(applied))
            expected_prefix = list(range(1, len(applied) + 1))
            if applied != expected_prefix:
                version = applied[-1] if applied else 0
                raise UnsupportedSchemaVersionError(version)
            for version in range(len(applied) + 1, CURRENT_SCHEMA_VERSION + 1):
                for statement in _MIGRATIONS[version]:
                    connection.execute(statement)
                connection.execute(
                    'INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)',
                    (version, _datetime_to_text(datetime.now().astimezone())),
                )

    def _save_plan(self, record: PlanRecord) -> None:
        public = record.public
        with closing(self._connect()) as connection, connection:
            connection.execute('BEGIN IMMEDIATE')
            connection.execute(
                """
                INSERT INTO plans (plan_id, revision, public_json, created_at, expires_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (plan_id) DO UPDATE SET
                    revision = excluded.revision,
                    public_json = excluded.public_json,
                    created_at = excluded.created_at,
                    expires_at = excluded.expires_at
                """,
                (
                    public.plan_id,
                    public.revision,
                    public.model_dump_json(),
                    _datetime_to_text(public.created_at),
                    _datetime_to_text(public.expires_at),
                ),
            )
            saved_candidate_ids = {candidate.candidate_id for candidate in record.candidates}
            existing_rows = connection.execute(
                'SELECT candidate_id FROM candidates WHERE plan_id = ?',
                (public.plan_id,),
            ).fetchall()
            for row in existing_rows:
                if row['candidate_id'] not in saved_candidate_ids:
                    connection.execute(
                        'DELETE FROM candidates WHERE plan_id = ? AND candidate_id = ?',
                        (public.plan_id, row['candidate_id']),
                    )
            for candidate in record.candidates:
                fingerprint = candidate.fingerprint
                connection.execute(
                    """
                    INSERT INTO candidates (
                        plan_id, candidate_id, video_id, source_path, source_root, destination_path,
                        fingerprint_device, fingerprint_inode, fingerprint_size,
                        fingerprint_mtime_ns, fingerprint_ctime_ns
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (plan_id, candidate_id) DO UPDATE SET
                        video_id = excluded.video_id,
                        source_path = excluded.source_path,
                        source_root = excluded.source_root,
                        destination_path = excluded.destination_path,
                        fingerprint_device = excluded.fingerprint_device,
                        fingerprint_inode = excluded.fingerprint_inode,
                        fingerprint_size = excluded.fingerprint_size,
                        fingerprint_mtime_ns = excluded.fingerprint_mtime_ns,
                        fingerprint_ctime_ns = excluded.fingerprint_ctime_ns
                    """,
                    (
                        public.plan_id,
                        candidate.candidate_id,
                        candidate.video_id,
                        str(candidate.source),
                        str(candidate.source_root),
                        str(candidate.destination),
                        fingerprint.device,
                        fingerprint.inode,
                        fingerprint.size,
                        fingerprint.mtime_ns,
                        fingerprint.ctime_ns,
                    ),
                )

    def _get_plan(self, plan_id: str) -> PlanRecord | None:
        with closing(self._connect()) as connection, connection:
            connection.execute('BEGIN')
            row = connection.execute('SELECT public_json FROM plans WHERE plan_id = ?', (plan_id,)).fetchone()
            if row is None:
                return None
            candidate_rows = connection.execute(
                'SELECT * FROM candidates WHERE plan_id = ? ORDER BY candidate_id',
                (plan_id,),
            ).fetchall()
        return PlanRecord(
            public=FillActorPlan.model_validate_json(row['public_json']),
            candidates=tuple(_candidate_from_row(candidate_row) for candidate_row in candidate_rows),
        )

    def _get_candidate(self, plan_id: str, candidate_id: str) -> CandidateRecord | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                'SELECT * FROM candidates WHERE plan_id = ? AND candidate_id = ?',
                (plan_id, candidate_id),
            ).fetchone()
        return _candidate_from_row(row) if row is not None else None

    def _delete_plan(self, plan_id: str) -> bool:
        with closing(self._connect()) as connection, connection:
            connection.execute('BEGIN IMMEDIATE')
            unreconciled = connection.execute(
                "SELECT 1 FROM move_journal WHERE plan_id = ? AND state != 'reconciled' LIMIT 1",
                (plan_id,),
            ).fetchone()
            if unreconciled is not None:
                return False
            connection.execute('UPDATE jobs SET plan_id = NULL WHERE plan_id = ?', (plan_id,))
            cursor = connection.execute('DELETE FROM plans WHERE plan_id = ?', (plan_id,))
            return cursor.rowcount > 0

    def _purge_expired_plans(self, now: datetime) -> int:
        with closing(self._connect()) as connection, connection:
            connection.execute('BEGIN IMMEDIATE')
            expired_rows = connection.execute(
                """
                SELECT plan_id FROM plans
                WHERE expires_at <= ?
                  AND NOT EXISTS (
                      SELECT 1 FROM move_journal
                      WHERE move_journal.plan_id = plans.plan_id
                        AND move_journal.state != 'reconciled'
                  )
                """,
                (_datetime_to_text(now),),
            ).fetchall()
            for row in expired_rows:
                connection.execute('UPDATE jobs SET plan_id = NULL WHERE plan_id = ?', (row['plan_id'],))
            if not expired_rows:
                return 0
            connection.executemany(
                'DELETE FROM plans WHERE plan_id = ?',
                ((row['plan_id'],) for row in expired_rows),
            )
            return len(expired_rows)

    def _save_move_result(self, plan_id: str, result: MoveResult) -> None:
        with closing(self._connect()) as connection, connection:
            row = connection.execute(
                'SELECT video_id FROM candidates WHERE plan_id = ? AND candidate_id = ?',
                (plan_id, result.candidate_id),
            ).fetchone()
            if row is None:
                raise KeyError((plan_id, result.candidate_id))
            if row['video_id'] != result.video_id:
                msg = 'move result video id does not match candidate'
                raise ValueError(msg)
            connection.execute(
                """
                INSERT INTO move_results (plan_id, candidate_id, result_json)
                VALUES (?, ?, ?)
                ON CONFLICT (plan_id, candidate_id) DO UPDATE SET result_json = excluded.result_json
                """,
                (plan_id, result.candidate_id, result.model_dump_json()),
            )

    def _get_move_result(self, plan_id: str, candidate_id: str) -> MoveResult | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                'SELECT result_json FROM move_results WHERE plan_id = ? AND candidate_id = ?',
                (plan_id, candidate_id),
            ).fetchone()
        return MoveResult.model_validate_json(row['result_json']) if row is not None else None

    def _list_move_results(self, plan_id: str) -> tuple[MoveResult, ...]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                'SELECT result_json FROM move_results WHERE plan_id = ? ORDER BY candidate_id',
                (plan_id,),
            ).fetchall()
        return tuple(MoveResult.model_validate_json(row['result_json']) for row in rows)

    def _save_job(self, record: JobRecord) -> None:
        with closing(self._connect()) as connection, connection:
            self._execute_save_job(connection, record)

    def _enqueue_job(
        self,
        record: JobRecord,
        max_active: int,
        feeds: Sequence[JobFeedRecord],
    ) -> bool:
        with closing(self._connect()) as connection, connection:
            connection.execute('BEGIN IMMEDIATE')
            active = connection.execute("SELECT COUNT(*) FROM jobs WHERE state IN ('queued', 'running')").fetchone()[0]
            exists = connection.execute('SELECT 1 FROM jobs WHERE job_id = ?', (record.job_id,)).fetchone()
            if active >= max_active or exists is not None:
                return False
            feed_keys = [(feed.job_id, feed.actor_id) for feed in feeds]
            if any(feed.job_id != record.job_id for feed in feeds) or len(feed_keys) != len(set(feed_keys)):
                msg = 'job feeds must be unique and belong to the enqueued job'
                raise ValueError(msg)
            self._execute_save_job(connection, record)
            connection.executemany(
                """
                INSERT INTO job_feeds (
                    job_id, actor_id, state, attempts, updated_at, error_code, freshrss_add_url
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    (
                        feed.job_id,
                        feed.actor_id,
                        feed.state.value,
                        feed.attempts,
                        _datetime_to_text(feed.updated_at),
                        feed.error_code,
                        feed.freshrss_add_url,
                    )
                    for feed in feeds
                ),
            )
            return True

    def _claim_next_job(
        self,
        owner_id: str,
        now: datetime,
        lease_expires_at: datetime,
    ) -> JobRecord | None:
        with closing(self._connect()) as connection, connection:
            connection.execute('BEGIN IMMEDIATE')
            row = connection.execute(
                "SELECT * FROM jobs WHERE state = 'queued' ORDER BY created_at, job_id LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            connection.execute(
                """
                UPDATE jobs
                SET state = 'running', updated_at = ?, owner_id = ?, lease_expires_at = ?
                WHERE job_id = ? AND state = 'queued'
                """,
                (
                    _datetime_to_text(now),
                    owner_id,
                    _datetime_to_text(lease_expires_at),
                    row['job_id'],
                ),
            )
            return JobRecord(
                job_id=row['job_id'],
                operation=JobOperation(row['operation']),
                state=JobState.RUNNING,
                created_at=datetime.fromisoformat(row['created_at']),
                updated_at=now,
                plan_id=row['plan_id'],
                error_code=row['error_code'],
                owner_id=owner_id,
                lease_expires_at=lease_expires_at,
                actor_ids=tuple(json.loads(row['actor_ids_json'])),
                progress=_progress_from_row(row),
            )

    def _renew_owned_job_lease(
        self,
        job_id: str,
        owner_id: str,
        now: datetime,
        lease_expires_at: datetime,
    ) -> bool:
        with closing(self._connect()) as connection, connection:
            cursor = connection.execute(
                """
                UPDATE jobs SET updated_at = ?, lease_expires_at = ?
                WHERE job_id = ? AND owner_id = ? AND state = 'running'
                  AND lease_expires_at IS NOT NULL AND lease_expires_at > ?
                """,
                (
                    _datetime_to_text(now),
                    _datetime_to_text(lease_expires_at),
                    job_id,
                    owner_id,
                    _datetime_to_text(now),
                ),
            )
            return cursor.rowcount == 1

    def _update_owned_job_progress(
        self,
        job_id: str,
        owner_id: str,
        progress: JobProgress,
        now: datetime,
    ) -> bool:
        with closing(self._connect()) as connection, connection:
            cursor = connection.execute(
                """
                UPDATE jobs SET
                    progress_stage = ?, progress_completed = ?, progress_total = ?, progress_unit = ?,
                    progress_current = ?, progress_stage_started_at = ?, progress_updated_at = ?
                WHERE job_id = ? AND owner_id = ? AND state = 'running'
                  AND lease_expires_at IS NOT NULL AND lease_expires_at > ?
                """,
                (*_progress_values(progress), job_id, owner_id, _datetime_to_text(now)),
            )
            return cursor.rowcount == 1

    def _finish_owned_job(  # noqa: PLR0913
        self,
        job_id: str,
        owner_id: str,
        state: JobState,
        error_code: str | None,
        now: datetime,
        progress: JobProgress,
    ) -> bool:
        with closing(self._connect()) as connection, connection:
            cursor = connection.execute(
                """
                UPDATE jobs SET
                    state = ?, updated_at = ?, error_code = ?, owner_id = NULL, lease_expires_at = NULL,
                    progress_stage = ?, progress_completed = ?, progress_total = ?, progress_unit = ?,
                    progress_current = ?, progress_stage_started_at = ?, progress_updated_at = ?
                WHERE job_id = ? AND owner_id = ? AND state = 'running'
                  AND lease_expires_at IS NOT NULL AND lease_expires_at > ?
                """,
                (
                    state.value,
                    _datetime_to_text(now),
                    error_code,
                    *_progress_values(progress),
                    job_id,
                    owner_id,
                    _datetime_to_text(now),
                ),
            )
            return cursor.rowcount == 1

    def _cancel_job(self, job_id: str, now: datetime) -> CancelJobResult:
        with closing(self._connect()) as connection, connection:
            connection.execute('BEGIN IMMEDIATE')
            row = connection.execute('SELECT * FROM jobs WHERE job_id = ?', (job_id,)).fetchone()
            if row is None:
                return CancelJobResult(CancelJobOutcome.NOT_FOUND, None)

            current = _job_from_row(row)
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

            progress = _terminal_progress(current, now)
            cursor = connection.execute(
                """
                UPDATE jobs SET
                    state = 'failed', updated_at = ?, error_code = ?, owner_id = NULL, lease_expires_at = NULL,
                    progress_stage = ?, progress_completed = ?, progress_total = ?, progress_unit = ?,
                    progress_current = ?, progress_stage_started_at = ?, progress_updated_at = ?
                WHERE job_id = ? AND state IN ('queued', 'running')
                """,
                (
                    _datetime_to_text(now),
                    JOB_CANCELLED_ERROR_CODE,
                    *_progress_values(progress),
                    job_id,
                ),
            )
            if cursor.rowcount != 1:  # pragma: no cover - BEGIN IMMEDIATE serializes competing writers
                msg = 'cancelled job transition was not atomic'
                raise RuntimeError(msg)
            connection.execute(
                """
                UPDATE job_feeds
                SET state = 'failed', updated_at = ?, error_code = ?
                WHERE job_id = ? AND state IN ('queued', 'warming')
                """,
                (_datetime_to_text(now), JobFeedErrorCode.CANCELLED.value, job_id),
            )
            cancelled_row = connection.execute('SELECT * FROM jobs WHERE job_id = ?', (job_id,)).fetchone()
            if cancelled_row is None:  # pragma: no cover - same transaction retains the row
                msg = 'cancelled job disappeared'
                raise RuntimeError(msg)
            return CancelJobResult(
                CancelJobOutcome.CANCELLED,
                _job_from_row(cancelled_row),
                previous_state=current.state,
                previous_owner_id=current.owner_id,
            )

    def _fail_expired_jobs(self, now: datetime, error_code: str) -> int:
        with closing(self._connect()) as connection, connection:
            connection.execute('BEGIN IMMEDIATE')
            expired_rows = connection.execute(
                """
                SELECT job_id FROM jobs
                WHERE state = 'running' AND (lease_expires_at IS NULL OR lease_expires_at <= ?)
                """,
                (_datetime_to_text(now),),
            ).fetchall()
            if not expired_rows:
                return 0
            connection.executemany(
                """
                UPDATE jobs
                SET state = 'failed', updated_at = ?, error_code = ?, owner_id = NULL, lease_expires_at = NULL,
                    progress_stage = 'done', progress_stage_started_at = ?, progress_updated_at = ?
                WHERE job_id = ? AND state = 'running'
                """,
                (
                    (
                        _datetime_to_text(now),
                        error_code,
                        _datetime_to_text(now),
                        _datetime_to_text(now),
                        row['job_id'],
                    )
                    for row in expired_rows
                ),
            )
            connection.executemany(
                """
                UPDATE job_feeds
                SET state = 'failed', updated_at = ?, error_code = ?
                WHERE job_id = ? AND state IN ('queued', 'warming')
                """,
                (
                    (
                        _datetime_to_text(now),
                        JobFeedErrorCode.CANCELLED.value,
                        row['job_id'],
                    )
                    for row in expired_rows
                ),
            )
            return len(expired_rows)

    @staticmethod
    def _execute_save_job(connection: sqlite3.Connection, record: JobRecord) -> None:
        connection.execute(
            """
                INSERT INTO jobs (
                    job_id, operation, state, created_at, updated_at, plan_id, error_code,
                    owner_id, lease_expires_at, actor_ids_json,
                    progress_stage, progress_completed, progress_total, progress_unit, progress_current,
                    progress_stage_started_at, progress_updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (job_id) DO UPDATE SET
                    operation = excluded.operation,
                    state = excluded.state,
                    created_at = excluded.created_at,
                    updated_at = excluded.updated_at,
                    plan_id = excluded.plan_id,
                    error_code = excluded.error_code,
                    owner_id = excluded.owner_id,
                    lease_expires_at = excluded.lease_expires_at,
                    actor_ids_json = excluded.actor_ids_json,
                    progress_stage = excluded.progress_stage,
                    progress_completed = excluded.progress_completed,
                    progress_total = excluded.progress_total,
                    progress_unit = excluded.progress_unit,
                    progress_current = excluded.progress_current,
                    progress_stage_started_at = excluded.progress_stage_started_at,
                    progress_updated_at = excluded.progress_updated_at
                """,
            (
                record.job_id,
                record.operation.value,
                record.state.value,
                _datetime_to_text(record.created_at),
                _datetime_to_text(record.updated_at),
                record.plan_id,
                record.error_code,
                record.owner_id,
                _datetime_to_text(record.lease_expires_at) if record.lease_expires_at is not None else None,
                json.dumps(record.actor_ids),
                *_progress_values(_require_progress(record)),
            ),
        )

    def _get_job(self, job_id: str) -> JobRecord | None:
        with closing(self._connect()) as connection:
            row = connection.execute('SELECT * FROM jobs WHERE job_id = ?', (job_id,)).fetchone()
        if row is None:
            return None
        return _job_from_row(row)

    def _list_jobs(self, states: Sequence[JobState] | None) -> tuple[JobRecord, ...]:
        with closing(self._connect()) as connection:
            rows = connection.execute('SELECT * FROM jobs ORDER BY created_at, job_id').fetchall()
        selected_states = set(states) if states is not None else None
        return tuple(
            _job_from_row(row) for row in rows if selected_states is None or JobState(row['state']) in selected_states
        )

    def _list_job_feeds(self, job_id: str) -> tuple[JobFeedRecord, ...]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                'SELECT * FROM job_feeds WHERE job_id = ? ORDER BY actor_id',
                (job_id,),
            ).fetchall()
        return tuple(_job_feed_from_row(row) for row in rows)

    def _update_owned_job_feed(  # noqa: PLR0913
        self,
        job_id: str,
        actor_id: str,
        owner_id: str,
        state: JobFeedState,
        attempts: int,
        error_code: JobFeedErrorCode | None,
        now: datetime,
    ) -> bool:
        record = JobFeedRecord(
            job_id=job_id,
            actor_id=actor_id,
            state=state,
            attempts=attempts,
            updated_at=now,
            error_code=error_code,
        )
        with closing(self._connect()) as connection, connection:
            cursor = connection.execute(
                """
                UPDATE job_feeds SET
                    state = ?, attempts = ?, updated_at = ?, error_code = ?
                WHERE job_id = ? AND actor_id = ?
                  AND state IN ('queued', 'warming')
                  AND attempts <= ?
                  AND EXISTS (
                      SELECT 1 FROM jobs
                      WHERE jobs.job_id = job_feeds.job_id
                        AND jobs.owner_id = ?
                        AND jobs.state = 'running'
                        AND jobs.lease_expires_at IS NOT NULL
                        AND jobs.lease_expires_at > ?
                  )
                """,
                (
                    record.state.value,
                    record.attempts,
                    _datetime_to_text(record.updated_at),
                    record.error_code,
                    record.job_id,
                    record.actor_id,
                    record.attempts,
                    owner_id,
                    _datetime_to_text(now),
                ),
            )
            return cursor.rowcount == 1

    def _save_move_journal(self, record: MoveJournalRecord) -> None:
        with closing(self._connect()) as connection, connection:
            connection.execute('BEGIN IMMEDIATE')
            candidate = connection.execute(
                'SELECT 1 FROM candidates WHERE plan_id = ? AND candidate_id = ?',
                (record.plan_id, record.candidate_id),
            ).fetchone()
            if candidate is None:
                raise KeyError((record.plan_id, record.candidate_id))
            current_row = connection.execute(
                'SELECT state FROM move_journal WHERE plan_id = ? AND candidate_id = ?',
                (record.plan_id, record.candidate_id),
            ).fetchone()
            current = MoveJournalState(current_row['state']) if current_row is not None else None
            validate_journal_transition(current, record.state)
            connection.execute(
                """
                INSERT INTO move_journal (plan_id, candidate_id, state, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (plan_id, candidate_id) DO UPDATE SET
                    state = excluded.state,
                    updated_at = excluded.updated_at
                """,
                (
                    record.plan_id,
                    record.candidate_id,
                    record.state.value,
                    _datetime_to_text(record.updated_at),
                ),
            )

    def _get_move_journal(self, plan_id: str, candidate_id: str) -> MoveJournalRecord | None:
        with closing(self._connect()) as connection:
            row = connection.execute(
                'SELECT * FROM move_journal WHERE plan_id = ? AND candidate_id = ?',
                (plan_id, candidate_id),
            ).fetchone()
        return _journal_from_row(row) if row is not None else None

    def _list_unreconciled_moves(self) -> tuple[MoveJournalRecord, ...]:
        with closing(self._connect()) as connection:
            rows = connection.execute(
                """
                SELECT * FROM move_journal
                WHERE state != 'reconciled'
                ORDER BY updated_at, plan_id, candidate_id
                """
            ).fetchall()
        return tuple(_journal_from_row(row) for row in rows)

    def _health_check(self) -> bool:
        try:
            with closing(self._connect()) as connection:
                connection.execute('BEGIN IMMEDIATE')
                row = connection.execute('SELECT MAX(version) FROM schema_migrations').fetchone()
                connection.execute(
                    """
                    INSERT INTO health_probe (id, checked_at) VALUES (1, ?)
                    ON CONFLICT (id) DO UPDATE SET checked_at = excluded.checked_at
                    """,
                    (_datetime_to_text(datetime.now().astimezone()),),
                )
                connection.rollback()
        except sqlite3.Error:
            return False
        return row is not None and row[0] == CURRENT_SCHEMA_VERSION


def _candidate_from_row(row: sqlite3.Row) -> CandidateRecord:
    return CandidateRecord(
        candidate_id=row['candidate_id'],
        video_id=row['video_id'],
        source=Path(row['source_path']),
        source_root=Path(row['source_root']),
        destination=Path(row['destination_path']),
        fingerprint=FileFingerprint(
            device=row['fingerprint_device'],
            inode=row['fingerprint_inode'],
            size=row['fingerprint_size'],
            mtime_ns=row['fingerprint_mtime_ns'],
            ctime_ns=row['fingerprint_ctime_ns'],
        ),
    )


def _journal_from_row(row: sqlite3.Row) -> MoveJournalRecord:
    return MoveJournalRecord(
        plan_id=row['plan_id'],
        candidate_id=row['candidate_id'],
        state=MoveJournalState(row['state']),
        updated_at=datetime.fromisoformat(row['updated_at']),
    )


def _job_from_row(row: sqlite3.Row) -> JobRecord:
    return JobRecord(
        job_id=row['job_id'],
        operation=JobOperation(row['operation']),
        state=JobState(row['state']),
        created_at=datetime.fromisoformat(row['created_at']),
        updated_at=datetime.fromisoformat(row['updated_at']),
        plan_id=row['plan_id'],
        error_code=row['error_code'],
        owner_id=row['owner_id'],
        lease_expires_at=datetime.fromisoformat(row['lease_expires_at']) if row['lease_expires_at'] else None,
        actor_ids=tuple(json.loads(row['actor_ids_json'])),
        progress=_progress_from_row(row),
    )


def _job_feed_from_row(row: sqlite3.Row) -> JobFeedRecord:
    return JobFeedRecord(
        job_id=row['job_id'],
        actor_id=row['actor_id'],
        state=JobFeedState(row['state']),
        attempts=row['attempts'],
        updated_at=datetime.fromisoformat(row['updated_at']),
        error_code=JobFeedErrorCode(row['error_code']) if row['error_code'] else None,
        freshrss_add_url=row['freshrss_add_url'],
    )


def _progress_from_row(row: sqlite3.Row) -> JobProgress:
    return JobProgress(
        stage=JobStage(row['progress_stage']),
        completed=row['progress_completed'],
        total=row['progress_total'],
        unit=JobProgressUnit(row['progress_unit']),
        current=row['progress_current'],
        stage_started_at=datetime.fromisoformat(row['progress_stage_started_at']),
        updated_at=datetime.fromisoformat(row['progress_updated_at']),
    )


def _terminal_progress(record: JobRecord, now: datetime) -> JobProgress:
    progress = _require_progress(record)
    return JobProgress(
        stage=JobStage.DONE,
        completed=progress.completed,
        total=progress.total,
        unit=progress.unit,
        current=progress.current,
        stage_started_at=now,
        updated_at=now,
    )


def _progress_values(progress: JobProgress) -> tuple[object, ...]:
    return (
        progress.stage.value,
        progress.completed,
        progress.total,
        progress.unit.value,
        progress.current,
        _datetime_to_text(progress.stage_started_at),
        _datetime_to_text(progress.updated_at),
    )


def _require_progress(record: JobRecord) -> JobProgress:
    if record.progress is None:  # pragma: no cover - JobRecord normalizes this invariant
        msg = 'job record progress is required'
        raise ValueError(msg)
    return record.progress


def _datetime_to_text(value: datetime) -> str:
    return normalize_datetime(value).isoformat(timespec='microseconds')
