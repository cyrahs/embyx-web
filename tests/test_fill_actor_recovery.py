import asyncio
import errno
import os
import threading
from datetime import UTC, datetime
from pathlib import Path

import pytest

import embyx_web.fill_actor.service as fill_actor_service_module
from embyx_web.fill_actor.models import MoveState
from embyx_web.fill_actor.persistence import MoveJournalRecord, MoveJournalState
from embyx_web.fill_actor.service import FillActorPaths, FillActorService
from embyx_web.fill_actor.sqlite_repository import SQLiteFillActorRepository
from embyx_web.locking import AsyncFileLock


class ActorCatalog:
    async def list_video_ids(self, _actor_id: str) -> list[str]:
        return ['ABC-001']


class MagnetProvider:
    async def find_magnet(self, _video_id: str) -> None:
        return None


class BrandResolver:
    def resolve_brand(self, _video_id: str) -> str:
        return 'ABC'


def make_service(
    tmp_path: Path,
    repository: SQLiteFillActorRepository,
    *,
    root_sentinel: str | None = None,
    apply_enabled: bool = True,
) -> tuple[FillActorService, FillActorPaths]:
    paths = FillActorPaths.from_iterable(
        actor_brand_path=tmp_path / 'actor',
        additional_brand_paths=(tmp_path / 'additional',),
        move_in_path=tmp_path / 'move-in',
    )
    for path in (paths.actor_brand_path, *paths.additional_brand_paths, paths.move_in_path):
        path.mkdir(exist_ok=True)
        if root_sentinel is not None:
            (path / root_sentinel).write_text('ready', encoding='utf-8')
    service = FillActorService(
        paths=paths,
        actor_catalog=ActorCatalog(),
        magnet_provider=MagnetProvider(),
        brand_resolver=BrandResolver(),
        repository=repository,
        mutation_lock=AsyncFileLock(tmp_path / 'move.lock'),
        root_sentinel=root_sentinel,
        apply_enabled=apply_enabled,
    )
    return service, paths


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('journal_state', 'filesystem_state'),
    [
        (MoveJournalState.PREPARED, 'source_only'),
        (MoveJournalState.PREPARED, 'both_linked'),
        (MoveJournalState.LINKED, 'both_linked'),
        (MoveJournalState.SOURCE_REMOVED, 'destination_only'),
    ],
)
async def test_startup_reconciles_each_move_journal_state(
    tmp_path: Path,
    journal_state: MoveJournalState,
    filesystem_state: str,
) -> None:
    repository = SQLiteFillActorRepository(tmp_path / 'state' / 'app.sqlite3')
    service, paths = make_service(tmp_path, repository)
    brand_path = paths.additional_brand_paths[0] / 'ABC'
    brand_path.mkdir()
    source = brand_path / 'ABC-001.mp4'
    source.write_bytes(b'video')
    plan = await service.create_plan(['actor'])
    candidate = plan.videos[0].move_candidates[0]
    record = await repository.get_candidate(plan.plan_id, candidate.candidate_id)
    assert record is not None
    states = [MoveJournalState.PREPARED, MoveJournalState.LINKED, MoveJournalState.SOURCE_REMOVED]
    for state in states[: states.index(journal_state) + 1]:
        await repository.save_move_journal(
            MoveJournalRecord(
                plan_id=plan.plan_id,
                candidate_id=candidate.candidate_id,
                state=state,
                updated_at=datetime.now(UTC),
            )
        )
    if filesystem_state in {'both_linked', 'destination_only'}:
        os.link(record.source, record.destination)
    if filesystem_state == 'destination_only':
        record.source.unlink()

    restarted_repository = SQLiteFillActorRepository(tmp_path / 'state' / 'app.sqlite3')
    restarted, _ = make_service(tmp_path, restarted_repository)
    results = await restarted.reconcile_moves()

    assert results[0].state is MoveState.MOVED
    assert not record.source.exists()
    assert record.destination.read_bytes() == b'video'
    saved = await restarted_repository.get_move_result(plan.plan_id, candidate.candidate_id)
    assert saved == results[0]
    journal = await restarted_repository.get_move_journal(plan.plan_id, candidate.candidate_id)
    assert journal is not None
    assert journal.state is MoveJournalState.RECONCILED


@pytest.mark.asyncio
async def test_disabled_apply_leaves_unreconciled_move_untouched(tmp_path: Path) -> None:
    database = tmp_path / 'state' / 'app.sqlite3'
    repository = SQLiteFillActorRepository(database)
    service, paths = make_service(tmp_path, repository)
    brand_path = paths.additional_brand_paths[0] / 'ABC'
    brand_path.mkdir()
    source = brand_path / 'ABC-001.mp4'
    source.write_bytes(b'video')
    plan = await service.create_plan(['actor'])
    candidate = plan.videos[0].move_candidates[0]
    await repository.save_move_journal(
        MoveJournalRecord(
            plan_id=plan.plan_id,
            candidate_id=candidate.candidate_id,
            state=MoveJournalState.PREPARED,
            updated_at=datetime.now(UTC),
        )
    )

    restarted_repository = SQLiteFillActorRepository(database)
    restarted, _ = make_service(tmp_path, restarted_repository, apply_enabled=False)

    assert await restarted.reconcile_moves() == ()
    assert source.read_bytes() == b'video'
    assert not (paths.move_in_path / source.name).exists()
    journal = await restarted_repository.get_move_journal(plan.plan_id, candidate.candidate_id)
    assert journal is not None
    assert journal.state is MoveJournalState.PREPARED


@pytest.mark.asyncio
async def test_sqlite_service_result_is_idempotent_after_restart(tmp_path: Path) -> None:
    database = tmp_path / 'state' / 'app.sqlite3'
    repository = SQLiteFillActorRepository(database)
    service, paths = make_service(tmp_path, repository)
    brand_path = paths.additional_brand_paths[0] / 'ABC'
    brand_path.mkdir()
    source = brand_path / 'ABC-001.mp4'
    source.write_bytes(b'video')
    plan = await service.create_plan(['actor'])
    candidate = plan.videos[0].move_candidates[0]
    first = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )

    restarted, _ = make_service(tmp_path, SQLiteFillActorRepository(database))
    second = await restarted.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )

    assert first == second


@pytest.mark.asyncio
async def test_cancelled_mutation_keeps_file_lock_until_native_move_finishes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / 'state' / 'app.sqlite3'
    repository = SQLiteFillActorRepository(database)
    service, paths = make_service(tmp_path, repository)
    brand_path = paths.additional_brand_paths[0] / 'ABC'
    brand_path.mkdir()
    source = brand_path / 'ABC-001.mp4'
    source.write_bytes(b'video')
    plan = await service.create_plan(['actor'])
    candidate = plan.videos[0].move_candidates[0]
    record = await repository.get_candidate(plan.plan_id, candidate.candidate_id)
    assert record is not None
    renamed = threading.Event()
    release = threading.Event()
    original_rename = fill_actor_service_module._rename_no_replace  # noqa: SLF001

    def delayed_rename(source_path: Path, destination_path: Path) -> None:
        original_rename(source_path, destination_path)
        renamed.set()
        if not release.wait(timeout=2):
            pytest.fail('native move was not released')

    monkeypatch.setattr(fill_actor_service_module, '_rename_no_replace', delayed_rename)
    move_task = asyncio.create_task(service._run_move(plan.plan_id, record))  # noqa: SLF001
    assert await asyncio.to_thread(renamed.wait, 2)
    move_task.cancel()

    restarted, _ = make_service(tmp_path, SQLiteFillActorRepository(database))
    reconcile_task = asyncio.create_task(restarted.reconcile_moves())
    await asyncio.sleep(0.03)
    assert not reconcile_task.done()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await move_task
    results = await reconcile_task
    assert results[0].state is MoveState.MOVED
    assert not source.exists()
    assert record.destination.read_bytes() == b'video'


@pytest.mark.asyncio
async def test_offline_root_keeps_journal_until_mount_identity_returns(tmp_path: Path) -> None:
    database = tmp_path / 'state' / 'app.sqlite3'
    repository = SQLiteFillActorRepository(database)
    service, paths = make_service(tmp_path, repository, root_sentinel='.root-ready')
    brand_path = paths.additional_brand_paths[0] / 'ABC'
    brand_path.mkdir()
    source = brand_path / 'ABC-001.mp4'
    source.write_bytes(b'video')
    plan = await service.create_plan(['actor'])
    candidate = plan.videos[0].move_candidates[0]
    await repository.save_move_journal(
        MoveJournalRecord(
            plan_id=plan.plan_id,
            candidate_id=candidate.candidate_id,
            state=MoveJournalState.PREPARED,
            updated_at=datetime.now(UTC),
        )
    )
    mounted_root = paths.additional_brand_paths[0]
    offline_root = tmp_path / 'additional-mounted'
    mounted_root.rename(offline_root)
    mounted_root.mkdir()

    assert await service.reconcile_moves() == ()
    journal = await repository.get_move_journal(plan.plan_id, candidate.candidate_id)
    assert journal is not None
    assert journal.state is MoveJournalState.PREPARED

    mounted_root.rmdir()
    offline_root.rename(mounted_root)
    results = await service.reconcile_moves()
    assert results[0].state is MoveState.MOVED


@pytest.mark.asyncio
async def test_prepared_reconcile_rolls_destination_replacement_back_to_source(tmp_path: Path) -> None:
    repository = SQLiteFillActorRepository(tmp_path / 'state' / 'app.sqlite3')
    service, paths = make_service(tmp_path, repository)
    brand_path = paths.additional_brand_paths[0] / 'ABC'
    brand_path.mkdir()
    source = brand_path / 'ABC-001.mp4'
    source.write_bytes(b'video')
    plan = await service.create_plan(['actor'])
    candidate = plan.videos[0].move_candidates[0]
    record = await repository.get_candidate(plan.plan_id, candidate.candidate_id)
    assert record is not None
    await repository.save_move_journal(
        MoveJournalRecord(
            plan_id=plan.plan_id,
            candidate_id=candidate.candidate_id,
            state=MoveJournalState.PREPARED,
            updated_at=datetime.now(UTC),
        )
    )
    fill_actor_service_module._rename_no_replace(record.source, record.destination)  # noqa: SLF001
    record.destination.unlink()
    record.destination.write_bytes(b'external-replacement')

    results = await service.reconcile_moves()

    assert results[0].state is MoveState.STALE
    assert results[0].error_code == 'source_changed_during_move'
    assert record.source.read_bytes() == b'external-replacement'
    assert not record.destination.exists()
    journal = await repository.get_move_journal(plan.plan_id, candidate.candidate_id)
    assert journal is not None
    assert journal.state is MoveJournalState.RECONCILED


@pytest.mark.asyncio
async def test_prepared_reconcile_preserves_journal_when_both_paths_were_replaced(tmp_path: Path) -> None:
    repository = SQLiteFillActorRepository(tmp_path / 'state' / 'app.sqlite3')
    service, paths = make_service(tmp_path, repository)
    brand_path = paths.additional_brand_paths[0] / 'ABC'
    brand_path.mkdir()
    source = brand_path / 'ABC-001.mp4'
    source.write_bytes(b'video')
    plan = await service.create_plan(['actor'])
    candidate = plan.videos[0].move_candidates[0]
    record = await repository.get_candidate(plan.plan_id, candidate.candidate_id)
    assert record is not None
    await repository.save_move_journal(
        MoveJournalRecord(
            plan_id=plan.plan_id,
            candidate_id=candidate.candidate_id,
            state=MoveJournalState.PREPARED,
            updated_at=datetime.now(UTC),
        )
    )
    fill_actor_service_module._rename_no_replace(record.source, record.destination)  # noqa: SLF001
    record.destination.unlink()
    record.destination.write_bytes(b'destination-replacement')
    record.source.write_bytes(b'source-replacement')

    results = await service.reconcile_moves()

    assert results[0].state is MoveState.FAILED
    assert results[0].error_code == 'reconcile_rollback_conflict'
    assert record.source.read_bytes() == b'source-replacement'
    assert record.destination.read_bytes() == b'destination-replacement'
    journal = await repository.get_move_journal(plan.plan_id, candidate.candidate_id)
    assert journal is not None
    assert journal.state is MoveJournalState.PREPARED


@pytest.mark.asyncio
async def test_legacy_link_reconcile_retries_when_quarantine_move_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = SQLiteFillActorRepository(tmp_path / 'state' / 'app.sqlite3')
    service, paths = make_service(tmp_path, repository)
    brand_path = paths.additional_brand_paths[0] / 'ABC'
    brand_path.mkdir()
    source = brand_path / 'ABC-001.mp4'
    source.write_bytes(b'video')
    plan = await service.create_plan(['actor'])
    candidate = plan.videos[0].move_candidates[0]
    record = await repository.get_candidate(plan.plan_id, candidate.candidate_id)
    assert record is not None
    await repository.save_move_journal(
        MoveJournalRecord(
            plan_id=plan.plan_id,
            candidate_id=candidate.candidate_id,
            state=MoveJournalState.PREPARED,
            updated_at=datetime.now(UTC),
        )
    )
    os.link(record.source, record.destination)

    def fail_quarantine(_source: Path, _destination: Path) -> None:
        msg = 'temporary failure'
        raise OSError(msg)

    monkeypatch.setattr(fill_actor_service_module, '_rename_no_replace', fail_quarantine)
    results = await service.reconcile_moves()

    assert results[0].error_code == 'reconcile_quarantine_failed'
    assert record.source.exists()
    assert record.destination.exists()
    journal = await repository.get_move_journal(plan.plan_id, candidate.candidate_id)
    assert journal is not None
    assert journal.state is MoveJournalState.PREPARED


@pytest.mark.asyncio
async def test_reconcile_never_restores_foreign_quarantine_file(tmp_path: Path) -> None:
    repository = SQLiteFillActorRepository(tmp_path / 'state' / 'app.sqlite3')
    service, paths = make_service(tmp_path, repository)
    brand_path = paths.additional_brand_paths[0] / 'ABC'
    brand_path.mkdir()
    source = brand_path / 'ABC-001.mp4'
    source.write_bytes(b'video')
    plan = await service.create_plan(['actor'])
    candidate = plan.videos[0].move_candidates[0]
    record = await repository.get_candidate(plan.plan_id, candidate.candidate_id)
    assert record is not None
    await repository.save_move_journal(
        MoveJournalRecord(
            plan_id=plan.plan_id,
            candidate_id=candidate.candidate_id,
            state=MoveJournalState.PREPARED,
            updated_at=datetime.now(UTC),
        )
    )
    quarantine = service._reconcile_quarantine_path(record)  # noqa: SLF001
    quarantine.write_bytes(b'foreign')

    results = await service.reconcile_moves()

    assert results[0].error_code == 'reconcile_quarantine_conflict'
    assert source.read_bytes() == b'video'
    assert quarantine.read_bytes() == b'foreign'
    journal = await repository.get_move_journal(plan.plan_id, candidate.candidate_id)
    assert journal is not None
    assert journal.state is MoveJournalState.PREPARED


@pytest.mark.asyncio
async def test_nfs_fallback_reconciles_legacy_both_linked_move(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = SQLiteFillActorRepository(tmp_path / 'state' / 'app.sqlite3')
    service, paths = make_service(tmp_path, repository)
    brand_path = paths.additional_brand_paths[0] / 'ABC'
    brand_path.mkdir()
    source = brand_path / 'ABC-001.mp4'
    source.write_bytes(b'video')
    plan = await service.create_plan(['actor'])
    candidate = plan.videos[0].move_candidates[0]
    record = await repository.get_candidate(plan.plan_id, candidate.candidate_id)
    assert record is not None
    await repository.save_move_journal(
        MoveJournalRecord(
            plan_id=plan.plan_id,
            candidate_id=candidate.candidate_id,
            state=MoveJournalState.PREPARED,
            updated_at=datetime.now(UTC),
        )
    )
    os.link(record.source, record.destination)

    def unsupported_rename(_source: Path, _destination: Path) -> None:
        raise OSError(errno.EINVAL, os.strerror(errno.EINVAL))

    monkeypatch.setattr(fill_actor_service_module, '_rename_no_replace', unsupported_rename)
    results = await service.reconcile_moves()

    assert results[0].state is MoveState.MOVED
    assert not record.source.exists()
    assert record.destination.read_bytes() == b'video'
    journal = await repository.get_move_journal(plan.plan_id, candidate.candidate_id)
    assert journal is not None
    assert journal.state is MoveJournalState.RECONCILED


@pytest.mark.asyncio
async def test_nfs_fallback_restores_replacement_from_private_quarantine(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = SQLiteFillActorRepository(tmp_path / 'state' / 'app.sqlite3')
    service, paths = make_service(tmp_path, repository)
    brand_path = paths.additional_brand_paths[0] / 'ABC'
    brand_path.mkdir()
    source = brand_path / 'ABC-001.mp4'
    source.write_bytes(b'video')
    plan = await service.create_plan(['actor'])
    candidate = plan.videos[0].move_candidates[0]
    record = await repository.get_candidate(plan.plan_id, candidate.candidate_id)
    assert record is not None
    await repository.save_move_journal(
        MoveJournalRecord(
            plan_id=plan.plan_id,
            candidate_id=candidate.candidate_id,
            state=MoveJournalState.PREPARED,
            updated_at=datetime.now(UTC),
        )
    )
    os.link(record.source, record.destination)
    record.source.unlink()
    quarantine = service._reconcile_quarantine_path(record)  # noqa: SLF001
    quarantine.write_bytes(b'external-replacement')

    def unsupported_rename(_source: Path, _destination: Path) -> None:
        raise OSError(errno.EINVAL, os.strerror(errno.EINVAL))

    monkeypatch.setattr(fill_actor_service_module, '_rename_no_replace', unsupported_rename)
    results = await service.reconcile_moves()

    assert results[0].state is MoveState.MOVED
    assert record.source.read_bytes() == b'external-replacement'
    assert record.destination.read_bytes() == b'video'
    assert not quarantine.exists()
    journal = await repository.get_move_journal(plan.plan_id, candidate.candidate_id)
    assert journal is not None
    assert journal.state is MoveJournalState.RECONCILED
