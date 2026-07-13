import asyncio
import errno
import os
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import embyx_web.fill_actor.service as fill_actor_service_module
from embyx_web.fill_actor import (
    ApplyState,
    ExpiredPlanError,
    FillActorPaths,
    FillActorService,
    InvalidActorIdError,
    MoveState,
    RevisionMismatchError,
    TooManyActorsError,
    UnknownCandidateError,
    UnknownPlanError,
    VideoState,
)
from embyx_web.fill_actor.persistence import JobProgressEvent, JobProgressUnit, JobStage


class FakeActorCatalog:
    def __init__(self, values: dict[str, list[str] | Exception]) -> None:
        self.values = values

    async def list_video_ids(self, actor_id: str) -> list[str]:
        value = self.values[actor_id]
        if isinstance(value, Exception):
            raise value
        return value


class PageProgressActorCatalog:
    async def list_video_ids(self, _actor_id: str, *, progress_callback=None) -> list[str]:
        assert progress_callback is not None
        await progress_callback(0, None, None)
        await progress_callback(0, 26, None)
        await progress_callback(1, 26, 1)
        await progress_callback(26, 26, 26)
        return ['ABC-001', 'ABC-002']


class FakeMagnetProvider:
    def __init__(self, values: dict[str, str | None | Exception] | None = None) -> None:
        self.values = values or {}
        self.calls: list[str] = []

    async def find_magnet(self, video_id: str) -> str | None:
        self.calls.append(video_id)
        value = self.values.get(video_id)
        if isinstance(value, Exception):
            raise value
        return value


class MappingBrandResolver:
    def __init__(self, values: dict[str, str | None]) -> None:
        self.values = values

    def resolve_brand(self, video_id: str) -> str | None:
        return self.values.get(video_id)


class TokenFactory:
    def __init__(self) -> None:
        self.value = 0

    def __call__(self) -> str:
        self.value += 1
        return f'token-{self.value}'


class MutableClock:
    def __init__(self) -> None:
        self.value = datetime(2026, 7, 12, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.value


@pytest.fixture
def paths(tmp_path: Path) -> FillActorPaths:
    actor = tmp_path / 'actor'
    additional_one = tmp_path / 'additional-one'
    additional_two = tmp_path / 'additional-two'
    move_in = tmp_path / 'move-in'
    for path in (actor, additional_one, additional_two, move_in):
        path.mkdir()
    return FillActorPaths.from_iterable(
        actor_brand_path=actor,
        additional_brand_paths=(additional_one, additional_two),
        move_in_path=move_in,
    )


def make_service(
    paths: FillActorPaths,
    *,
    catalog: dict[str, list[str] | Exception],
    brands: dict[str, str | None],
    magnets: dict[str, str | None | Exception] | None = None,
    clock: MutableClock | None = None,
    max_actors: int = 20,
    move_in_by_brand: bool = False,
) -> tuple[FillActorService, FakeMagnetProvider]:
    magnet_provider = FakeMagnetProvider(magnets)
    return (
        FillActorService(
            paths=paths,
            actor_catalog=FakeActorCatalog(catalog),
            magnet_provider=magnet_provider,
            brand_resolver=MappingBrandResolver(brands),
            clock=clock,
            token_factory=TokenFactory(),
            max_actors=max_actors,
            move_in_by_brand=move_in_by_brand,
        ),
        magnet_provider,
    )


@pytest.mark.asyncio
async def test_create_plan_reports_durable_stage_and_page_progress(paths: FillActorPaths) -> None:
    events: list[JobProgressEvent] = []
    service = FillActorService(
        paths=paths,
        actor_catalog=PageProgressActorCatalog(),
        magnet_provider=FakeMagnetProvider({'ABC-001': None, 'ABC-002': None}),
        brand_resolver=MappingBrandResolver({'ABC-001': 'ABC', 'ABC-002': 'ABC'}),
        token_factory=TokenFactory(),
    )

    async def report(event: JobProgressEvent) -> None:
        events.append(event)

    await service.create_plan(['actor'], progress=report)

    transitions: list[JobStage] = []
    for event in events:
        if not transitions or event.stage is not transitions[-1]:
            transitions.append(event.stage)
    assert transitions == [
        JobStage.ACTOR_CATALOG,
        JobStage.LIBRARY_SCAN,
        JobStage.MAGNET_LOOKUP,
        JobStage.PERSISTING,
    ]
    assert events[0].stage is JobStage.ACTOR_CATALOG
    page_events = [event for event in events if event.unit is JobProgressUnit.PAGES]
    assert [(event.completed, event.total) for event in page_events] == [
        (0, None),
        (0, 26),
        (1, 26),
        (26, 26),
    ]
    assert page_events[2].current is not None
    assert '页面 1/26' in page_events[2].current
    assert page_events[3].current is not None
    assert '页面 26/26' in page_events[3].current
    actor_events = [
        event for event in events if event.stage is JobStage.ACTOR_CATALOG and event.unit is JobProgressUnit.ACTORS
    ]
    assert actor_events[-1].completed == actor_events[-1].total == 1
    assert [
        (event.stage, event.completed, event.total) for event in events if event.stage is JobStage.LIBRARY_SCAN
    ] == [
        (JobStage.LIBRARY_SCAN, 0, 2),
        (JobStage.LIBRARY_SCAN, 1, 2),
        (JobStage.LIBRARY_SCAN, 2, 2),
    ]
    assert [
        (event.stage, event.completed, event.total) for event in events if event.stage is JobStage.MAGNET_LOOKUP
    ] == [
        (JobStage.MAGNET_LOOKUP, 0, 2),
        (JobStage.MAGNET_LOOKUP, 1, 2),
        (JobStage.MAGNET_LOOKUP, 2, 2),
    ]
    assert [(event.stage, event.completed, event.total) for event in events[-2:]] == [
        (JobStage.PERSISTING, 0, 1),
        (JobStage.PERSISTING, 1, 1),
    ]


@pytest.mark.asyncio
async def test_create_plan_classifies_and_sorts_results(paths: FillActorPaths) -> None:
    brand = 'ABC'
    actor_brand = paths.actor_brand_path / brand
    actor_brand.mkdir()
    (actor_brand / 'ABC-001.mp4').write_bytes(b'existing')
    for index, root in enumerate(paths.additional_brand_paths, start=1):
        brand_path = root / brand
        brand_path.mkdir()
        (brand_path / f'ABC-002-cd{index}.mp4').write_bytes(f'part-{index}'.encode())

    service, magnet_provider = make_service(
        paths,
        catalog={
            'actor-a': ['abc-003', 'abc-001_2026-07-01', 'abc-002', 'bad'],
            'actor-b': ['abc-002'],
        },
        brands={'ABC-001': brand, 'ABC-002': brand, 'ABC-003': brand, 'BAD': None},
        magnets={'ABC-003': 'magnet:?xt=urn:btih:test'},
    )

    plan = await service.create_plan(['actor-a', 'actor-b', 'actor-a'])

    assert [actor.actor_id for actor in plan.actors] == ['actor-a', 'actor-b']
    assert plan.actors[0].video_ids == ('ABC-001', 'ABC-002', 'ABC-003', 'BAD')
    assert [video.video_id for video in plan.videos] == ['ABC-001', 'ABC-002', 'ABC-003', 'BAD']
    videos = {video.video_id: video for video in plan.videos}
    assert videos['ABC-001'].state is VideoState.EXISTS
    assert videos['ABC-001'].existing_files == ('ABC-001.mp4',)
    assert videos['ABC-002'].state is VideoState.ADDITIONAL_FOUND
    assert [candidate.file_name for candidate in videos['ABC-002'].move_candidates] == [
        'ABC-002-cd1.mp4',
        'ABC-002-cd2.mp4',
    ]
    assert not any(candidate.destination_conflict for candidate in videos['ABC-002'].move_candidates)
    assert videos['ABC-002'].actor_ids == ('actor-a', 'actor-b')
    assert videos['ABC-003'].state is VideoState.MAGNET_FOUND
    assert videos['ABC-003'].magnet == 'magnet:?xt=urn:btih:test'
    assert videos['BAD'].state is VideoState.INVALID_VIDEO_ID
    assert magnet_provider.calls == ['ABC-003']
    assert str(paths.actor_brand_path) not in plan.model_dump_json()
    assert str(paths.additional_brand_paths[0]) not in plan.model_dump_json()


@pytest.mark.asyncio
async def test_create_plan_preserves_partial_external_failures(paths: FillActorPaths) -> None:
    service, _ = make_service(
        paths,
        catalog={
            'broken': RuntimeError('upstream unavailable'),
            'working': ['abc-001', 'abc-002'],
        },
        brands={'ABC-001': 'ABC', 'ABC-002': 'ABC'},
        magnets={'ABC-001': None, 'ABC-002': RuntimeError('search failed')},
    )

    plan = await service.create_plan(['broken', 'working'])

    assert plan.actors[0].error_code == 'actor_catalog_error'
    videos = {video.video_id: video for video in plan.videos}
    assert videos['ABC-001'].state is VideoState.MISSING
    assert videos['ABC-001'].warnings == ()
    assert videos['ABC-002'].state is VideoState.MISSING
    assert videos['ABC-002'].warnings == ('magnet_lookup_failed',)


@pytest.mark.asyncio
async def test_create_plan_rejects_invalid_magnet(paths: FillActorPaths) -> None:
    service, _ = make_service(
        paths,
        catalog={'actor': ['ABC-001']},
        brands={'ABC-001': 'ABC'},
        magnets={'ABC-001': 'https://example.invalid/not-a-magnet'},
    )

    plan = await service.create_plan(['actor'])

    assert plan.videos[0].state is VideoState.MISSING
    assert plan.videos[0].magnet is None
    assert plan.videos[0].warnings == ('invalid_magnet',)


@pytest.mark.asyncio
async def test_create_plan_rejects_brand_path_escape(paths: FillActorPaths) -> None:
    service, magnet_provider = make_service(
        paths,
        catalog={'actor': ['ABC-001']},
        brands={'ABC-001': '../../outside'},
    )

    plan = await service.create_plan(['actor'])

    assert plan.videos[0].state is VideoState.INVALID_VIDEO_ID
    assert magnet_provider.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize('root_kind', ['actor', 'additional'])
async def test_create_plan_does_not_follow_brand_directory_symlinks(
    paths: FillActorPaths,
    tmp_path: Path,
    root_kind: str,
) -> None:
    outside = tmp_path / f'outside-{root_kind}'
    outside.mkdir()
    outside_file_name = 'ABC-001-outside.mp4'
    (outside / outside_file_name).write_bytes(b'outside')
    root = paths.actor_brand_path if root_kind == 'actor' else paths.additional_brand_paths[0]
    (root / 'ABC').symlink_to(outside, target_is_directory=True)
    service, _ = make_service(
        paths,
        catalog={'actor': ['ABC-001']},
        brands={'ABC-001': 'ABC'},
    )

    plan = await service.create_plan(['actor'])

    assert plan.videos[0].state is VideoState.SCAN_FAILED
    assert plan.videos[0].warnings == ('scan_failed',)
    assert outside_file_name not in plan.model_dump_json()


@pytest.mark.asyncio
@pytest.mark.parametrize('root_kind', ['actor', 'additional'])
async def test_create_plan_reports_unavailable_scan_root(
    paths: FillActorPaths,
    root_kind: str,
) -> None:
    root = paths.actor_brand_path if root_kind == 'actor' else paths.additional_brand_paths[0]
    root.rmdir()
    service, magnet_provider = make_service(
        paths,
        catalog={'actor': ['ABC-001']},
        brands={'ABC-001': 'ABC'},
    )

    plan = await service.create_plan(['actor'])

    assert plan.videos[0].state is VideoState.SCAN_FAILED
    assert plan.videos[0].warnings == ('scan_failed',)
    assert magnet_provider.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize('actor_ids', [[], ['bad actor'], ['x' * 33]])
async def test_create_plan_rejects_invalid_actor_ids(paths: FillActorPaths, actor_ids: list[str]) -> None:
    service, _ = make_service(paths, catalog={}, brands={})

    with pytest.raises(InvalidActorIdError):
        await service.create_plan(actor_ids)


@pytest.mark.asyncio
async def test_create_plan_enforces_actor_limit(paths: FillActorPaths) -> None:
    service, _ = make_service(paths, catalog={}, brands={}, max_actors=1)

    with pytest.raises(TooManyActorsError):
        await service.create_plan(['actor-a', 'actor-b'])


def create_move_candidate(paths: FillActorPaths, *, video_id: str = 'ABC-001') -> Path:
    brand_path = paths.additional_brand_paths[0] / 'ABC'
    brand_path.mkdir(exist_ok=True)
    source = brand_path / f'{video_id}.mp4'
    source.write_bytes(b'video')
    return source


async def create_move_plan(
    paths: FillActorPaths,
    *,
    video_id: str = 'ABC-001',
    move_in_by_brand: bool = False,
):
    service, _ = make_service(
        paths,
        catalog={'actor': [video_id]},
        brands={video_id: 'ABC'},
        move_in_by_brand=move_in_by_brand,
    )
    plan = await service.create_plan(['actor'])
    candidate = plan.videos[0].move_candidates[0]
    return service, plan, candidate


@pytest.mark.asyncio
async def test_apply_moves_candidate_and_is_idempotent(paths: FillActorPaths) -> None:
    source = create_move_candidate(paths)
    service, plan, candidate = await create_move_plan(paths)

    first, second = await asyncio.gather(
        service.apply(plan_id=plan.plan_id, revision=plan.revision, candidate_ids=[candidate.candidate_id]),
        service.apply(plan_id=plan.plan_id, revision=plan.revision, candidate_ids=[candidate.candidate_id]),
    )

    assert first.state is ApplyState.SUCCEEDED
    assert second.state is ApplyState.SUCCEEDED
    assert first.results[0].state is MoveState.MOVED
    assert second.results[0].state is MoveState.MOVED
    assert not source.exists()
    assert (paths.move_in_path / source.name).read_bytes() == b'video'


@pytest.mark.asyncio
async def test_apply_can_move_into_brand_subdirectory(paths: FillActorPaths) -> None:
    source = create_move_candidate(paths)
    service, plan, candidate = await create_move_plan(paths, move_in_by_brand=True)

    result = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )

    destination = paths.move_in_path / 'ABC' / source.name
    assert result.state is ApplyState.SUCCEEDED
    assert not source.exists()
    assert destination.read_bytes() == b'video'


@pytest.mark.asyncio
async def test_apply_falls_back_to_hardlink_when_renameat2_is_unsupported(
    paths: FillActorPaths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = create_move_candidate(paths)
    service, plan, candidate = await create_move_plan(paths)

    def unsupported_rename(_source: Path, _destination: Path) -> None:
        raise OSError(errno.EINVAL, os.strerror(errno.EINVAL))

    monkeypatch.setattr(fill_actor_service_module, '_rename_no_replace', unsupported_rename)
    result = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )

    assert result.results[0].state is MoveState.MOVED
    assert not source.exists()
    assert (paths.move_in_path / source.name).read_bytes() == b'video'


@pytest.mark.asyncio
async def test_hardlink_fallback_preserves_source_replacement(
    paths: FillActorPaths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = create_move_candidate(paths)
    service, plan, candidate = await create_move_plan(paths)
    original_rename = os.rename

    def unsupported_rename(_source: Path, _destination: Path) -> None:
        raise OSError(errno.EINVAL, os.strerror(errno.EINVAL))

    def replace_source_before_quarantine(source_path: Path, destination_path: Path) -> None:
        if source_path == source:
            source_path.unlink()
            source_path.write_bytes(b'external-replacement')
        original_rename(source_path, destination_path)

    monkeypatch.setattr(fill_actor_service_module, '_rename_no_replace', unsupported_rename)
    monkeypatch.setattr(fill_actor_service_module.os, 'rename', replace_source_before_quarantine)
    result = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )

    assert result.results[0].state is MoveState.MOVED
    assert source.read_bytes() == b'external-replacement'
    assert (paths.move_in_path / source.name).read_bytes() == b'video'


@pytest.mark.asyncio
async def test_apply_reports_filesystem_without_rename_or_hardlink_support(
    paths: FillActorPaths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = create_move_candidate(paths)
    service, plan, candidate = await create_move_plan(paths)

    def unsupported_rename(_source: Path, _destination: Path) -> None:
        raise OSError(errno.EINVAL, os.strerror(errno.EINVAL))

    def unsupported_link(*_args, **_kwargs) -> None:
        raise PermissionError(errno.EPERM, os.strerror(errno.EPERM))

    monkeypatch.setattr(fill_actor_service_module, '_rename_no_replace', unsupported_rename)
    monkeypatch.setattr(fill_actor_service_module.os, 'link', unsupported_link)
    result = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )

    assert result.results[0].state is MoveState.FAILED
    assert result.results[0].error_code == 'move_unsupported'
    assert source.read_bytes() == b'video'
    assert not (paths.move_in_path / source.name).exists()


@pytest.mark.asyncio
async def test_plan_marks_duplicate_destinations_as_conflicts(paths: FillActorPaths) -> None:
    for root in paths.additional_brand_paths:
        brand_path = root / 'ABC'
        brand_path.mkdir()
        (brand_path / 'ABC-001.mp4').write_bytes(b'video')
    service, _ = make_service(
        paths,
        catalog={'actor': ['ABC-001']},
        brands={'ABC-001': 'ABC'},
    )

    plan = await service.create_plan(['actor'])

    candidates = plan.videos[0].move_candidates
    assert len(candidates) == 2
    assert all(candidate.destination_conflict for candidate in candidates)


@pytest.mark.asyncio
async def test_plan_marks_duplicate_destinations_across_video_results(paths: FillActorPaths) -> None:
    brand_path = paths.additional_brand_paths[0] / 'ABC'
    brand_path.mkdir()
    (brand_path / 'ABC-001-CD1.mp4').write_bytes(b'video')
    service, _ = make_service(
        paths,
        catalog={'actor': ['ABC-001', 'ABC-001-CD1']},
        brands={'ABC-001': 'ABC', 'ABC-001-CD1': 'ABC'},
    )

    plan = await service.create_plan(['actor'])

    candidates = [candidate for video in plan.videos for candidate in video.move_candidates]
    assert len(candidates) == 2
    assert all(candidate.destination_conflict for candidate in candidates)


@pytest.mark.asyncio
async def test_apply_reports_destination_conflict_without_overwrite(paths: FillActorPaths) -> None:
    source = create_move_candidate(paths)
    service, plan, candidate = await create_move_plan(paths)
    destination = paths.move_in_path / source.name
    destination.write_bytes(b'keep-me')

    result = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )

    assert result.state is ApplyState.FAILED
    assert result.results[0].state is MoveState.CONFLICT
    assert source.exists()
    assert destination.read_bytes() == b'keep-me'


@pytest.mark.asyncio
async def test_apply_reports_partial_failure(paths: FillActorPaths) -> None:
    first = create_move_candidate(paths, video_id='ABC-001')
    second = create_move_candidate(paths, video_id='ABC-002')
    service, _ = make_service(
        paths,
        catalog={'actor': ['ABC-001', 'ABC-002']},
        brands={'ABC-001': 'ABC', 'ABC-002': 'ABC'},
    )
    plan = await service.create_plan(['actor'])
    candidates = [video.move_candidates[0] for video in plan.videos]
    (paths.move_in_path / second.name).write_bytes(b'conflict')

    result = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id for candidate in candidates],
    )

    assert result.state is ApplyState.PARTIAL_FAILED
    assert [item.state for item in result.results] == [MoveState.MOVED, MoveState.CONFLICT]
    assert not first.exists()
    assert second.exists()


@pytest.mark.asyncio
async def test_apply_rejects_changed_source_as_stale(paths: FillActorPaths) -> None:
    source = create_move_candidate(paths)
    service, plan, candidate = await create_move_plan(paths)
    source.write_bytes(b'changed-size')

    result = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )

    assert result.results[0].state is MoveState.STALE
    assert result.results[0].error_code == 'source_changed'


@pytest.mark.asyncio
async def test_apply_rejects_same_size_replacement_as_stale(paths: FillActorPaths) -> None:
    source = create_move_candidate(paths)
    service, plan, candidate = await create_move_plan(paths)
    original_stat = source.stat()
    source.unlink()
    source.write_bytes(b'video')
    os.utime(source, ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns))

    result = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )

    assert result.results[0].state is MoveState.STALE
    assert result.results[0].error_code == 'source_changed'


@pytest.mark.asyncio
async def test_apply_rejects_symlink_candidate(paths: FillActorPaths, tmp_path: Path) -> None:
    source = create_move_candidate(paths)
    service, plan, candidate = await create_move_plan(paths)
    outside = tmp_path / 'outside.mp4'
    outside.write_bytes(b'outside')
    source.unlink()
    source.symlink_to(outside)

    result = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )

    assert result.results[0].state is MoveState.INVALID_PATH
    assert result.results[0].error_code == 'source_symlink'
    assert outside.read_bytes() == b'outside'


@pytest.mark.asyncio
async def test_apply_reports_missing_destination_root(paths: FillActorPaths) -> None:
    source = create_move_candidate(paths)
    service, plan, candidate = await create_move_plan(paths)
    paths.move_in_path.rmdir()

    result = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )

    assert result.results[0].state is MoveState.FAILED
    assert result.results[0].error_code == 'roots_unavailable'
    assert source.exists()


@pytest.mark.asyncio
async def test_apply_handles_destination_creation_race(
    paths: FillActorPaths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = create_move_candidate(paths)
    service, plan, candidate = await create_move_plan(paths)

    def raise_conflict(*_args, **_kwargs) -> None:
        raise FileExistsError

    monkeypatch.setattr(fill_actor_service_module, '_rename_no_replace', raise_conflict)

    result = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )

    assert result.results[0].state is MoveState.CONFLICT
    assert result.results[0].error_code == 'destination_exists'
    assert source.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize('replace_target', ['source', 'destination'])
async def test_atomic_move_never_unlinks_external_replacement(
    paths: FillActorPaths,
    monkeypatch: pytest.MonkeyPatch,
    replace_target: str,
) -> None:
    source = create_move_candidate(paths)
    service, plan, candidate = await create_move_plan(paths)
    destination = paths.move_in_path / source.name
    original_rename = fill_actor_service_module._rename_no_replace  # noqa: SLF001
    rename_calls = 0

    def replace_during_move(source_path: Path, destination_path: Path) -> None:
        nonlocal rename_calls
        rename_calls += 1
        if rename_calls == 1 and replace_target == 'source':
            source_path.unlink()
            source_path.write_bytes(b'new-content')
        original_rename(source_path, destination_path)
        if rename_calls == 1 and replace_target == 'destination':
            destination_path.unlink()
            destination_path.write_bytes(b'new-content')

    monkeypatch.setattr(fill_actor_service_module, '_rename_no_replace', replace_during_move)

    result = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )

    assert result.results[0].state is MoveState.STALE
    assert result.results[0].error_code == 'source_changed_during_move'
    assert source.read_bytes() == b'new-content'
    assert not destination.exists()


@pytest.mark.asyncio
async def test_apply_converts_unexpected_move_error_to_structured_failure(
    paths: FillActorPaths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    create_move_candidate(paths)
    service, plan, candidate = await create_move_plan(paths)

    def raise_unexpected(_record) -> None:
        msg = 'filesystem unavailable'
        raise OSError(msg)

    monkeypatch.setattr(service, '_apply_one', raise_unexpected)

    result = await service.apply(
        plan_id=plan.plan_id,
        revision=plan.revision,
        candidate_ids=[candidate.candidate_id],
    )

    assert result.state is ApplyState.FAILED
    assert result.results[0].state is MoveState.FAILED
    assert result.results[0].error_code == 'move_failed'


@pytest.mark.asyncio
async def test_apply_double_cancellation_keeps_one_in_flight_move(
    paths: FillActorPaths,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = create_move_candidate(paths)
    service, plan, candidate = await create_move_plan(paths)
    original_apply_one = service._apply_one  # noqa: SLF001
    started = threading.Event()
    release = threading.Event()
    apply_calls = 0

    def delayed_apply(record):
        nonlocal apply_calls
        apply_calls += 1
        started.set()
        if not release.wait(timeout=2):
            msg = 'test move did not receive release signal'
            raise RuntimeError(msg)
        return original_apply_one(record)

    monkeypatch.setattr(service, '_apply_one', delayed_apply)
    task = asyncio.create_task(
        service.apply(
            plan_id=plan.plan_id,
            revision=plan.revision,
            candidate_ids=[candidate.candidate_id],
        )
    )
    assert await asyncio.to_thread(started.wait, 2)
    task.cancel()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    retry_task = asyncio.create_task(
        service.apply(
            plan_id=plan.plan_id,
            revision=plan.revision,
            candidate_ids=[candidate.candidate_id],
        )
    )
    await asyncio.sleep(0)
    assert not retry_task.done()
    release.set()
    retry = await retry_task

    assert retry.results[0].state is MoveState.MOVED
    assert apply_calls == 1
    assert not source.exists()


@pytest.mark.asyncio
async def test_apply_validates_all_candidate_ids_before_moving(paths: FillActorPaths) -> None:
    source = create_move_candidate(paths)
    service, plan, candidate = await create_move_plan(paths)

    with pytest.raises(UnknownCandidateError):
        await service.apply(
            plan_id=plan.plan_id,
            revision=plan.revision,
            candidate_ids=[candidate.candidate_id, 'unknown'],
        )

    assert source.exists()


@pytest.mark.asyncio
async def test_apply_rejects_unknown_plan_and_revision(paths: FillActorPaths) -> None:
    create_move_candidate(paths)
    service, plan, candidate = await create_move_plan(paths)

    with pytest.raises(UnknownPlanError):
        await service.apply(plan_id='unknown', revision=plan.revision, candidate_ids=[])
    with pytest.raises(RevisionMismatchError):
        await service.apply(plan_id=plan.plan_id, revision='old', candidate_ids=[candidate.candidate_id])


@pytest.mark.asyncio
async def test_apply_rejects_expired_plan(paths: FillActorPaths) -> None:
    clock = MutableClock()
    create_move_candidate(paths)
    service, _ = make_service(
        paths,
        catalog={'actor': ['ABC-001']},
        brands={'ABC-001': 'ABC'},
        clock=clock,
    )
    plan = await service.create_plan(['actor'])
    clock.value += timedelta(hours=2)

    with pytest.raises(ExpiredPlanError):
        await service.apply(plan_id=plan.plan_id, revision=plan.revision, candidate_ids=[])
