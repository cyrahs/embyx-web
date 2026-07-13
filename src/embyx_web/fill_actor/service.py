import asyncio
import ctypes
import errno
import hashlib
import inspect
import os
import re
import secrets
import threading
from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable, Iterable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import partial
from pathlib import Path, PurePosixPath

from embyx_web.fill_actor.cloud_moves import (
    CloudFileMetadata,
    CloudFileMover,
    CloudMovePaths,
    CloudMoveResponse,
    InvalidStrmTargetError,
)
from embyx_web.fill_actor.errors import (
    ExpiredPlanError,
    InvalidActorIdError,
    LegacyPlanError,
    MoveDisabledError,
    RevisionMismatchError,
    TooManyActorsError,
    TooManyVideosError,
    UnknownCandidateError,
    UnknownPlanError,
)
from embyx_web.fill_actor.models import (
    ActorPlan,
    ApplyResult,
    ApplyState,
    FillActorPlan,
    MoveCandidate,
    MoveResult,
    MoveState,
    VideoPlan,
    VideoState,
)
from embyx_web.fill_actor.persistence import (
    CandidateKind,
    CandidateRecord,
    CloudMoveOperationRecord,
    CloudMoveOperationState,
    FileFingerprint,
    FillActorRepository,
    JobProgressEvent,
    JobProgressUnit,
    JobStage,
    MemoryFillActorRepository,
    MoveJournalRecord,
    MoveJournalState,
    PlanRecord,
)
from embyx_web.fill_actor.ports import ActorCatalog, BrandResolver, MagnetProvider
from embyx_web.locking import AsyncFileLock

ACTOR_ID_RE = re.compile(r'^[A-Za-z0-9_-]{1,32}$')
DATED_VIDEO_ID_RE = re.compile(r'^(.+)_\d{4}-\d{2}-\d{2}$')
MAX_MAGNET_LENGTH = 8192
CLOUD_VERIFY_ATTEMPTS = 5
CLOUD_HEALTH_SUCCESS_TTL_SECONDS = 30.0
CLOUD_HEALTH_FAILURE_TTL_SECONDS = 5.0
ProgressCallback = Callable[[JobProgressEvent], Awaitable[None]]


@dataclass(frozen=True)
class FillActorPaths:
    actor_brand_path: Path
    additional_brand_paths: tuple[Path, ...]
    move_in_path: Path

    @classmethod
    def from_iterable(
        cls,
        *,
        actor_brand_path: Path,
        additional_brand_paths: Iterable[Path],
        move_in_path: Path,
    ) -> 'FillActorPaths':
        return cls(
            actor_brand_path=actor_brand_path,
            additional_brand_paths=tuple(additional_brand_paths),
            move_in_path=move_in_path,
        )


_Fingerprint = FileFingerprint
_MoveRecord = CandidateRecord
_MUTATION_EXECUTOR = ThreadPoolExecutor(max_workers=8, thread_name_prefix='embyx-move')
_SCAN_EXECUTOR = ThreadPoolExecutor(max_workers=4, thread_name_prefix='embyx-scan')
_AT_FDCWD = -100
_RENAME_NOREPLACE = 1
_RENAME_NOREPLACE_UNSUPPORTED = {errno.EINVAL, errno.ENOSYS, errno.EOPNOTSUPP}


@asynccontextmanager
async def _unlocked() -> AsyncIterator[None]:
    yield


async def _await_executor_future[T](
    worker: Future[T],
    *,
    cancellation_wins: bool = False,
    on_cancel: Callable[[], None] | None = None,
    propagate_cancellation: bool = True,
) -> T:
    """Wait for an executor future that cannot be cancelled by event-loop task shutdown."""
    loop = asyncio.get_running_loop()
    completed = loop.create_future()

    def notify_done(_worker: Future[T]) -> None:
        with suppress(RuntimeError):
            loop.call_soon_threadsafe(completed.set_result, None)

    worker.add_done_callback(notify_done)
    cancelled = False
    while not completed.done():
        try:
            await asyncio.shield(completed)
        except asyncio.CancelledError:
            cancelled = True
            if on_cancel is not None:
                on_cancel()
    if cancelled and propagate_cancellation and cancellation_wins:
        worker.exception()
        raise asyncio.CancelledError
    if cancelled and propagate_cancellation and on_cancel is not None:
        # Scan cancellation wins even if the NFS operation reports an error
        # while unwinding. The worker has finished, so consuming its exception
        # here cannot leave executor work behind.
        with suppress(Exception):
            worker.result()
        raise asyncio.CancelledError
    result = worker.result()
    if cancelled and propagate_cancellation:
        raise asyncio.CancelledError
    return result


async def _await_mutation_future[T](worker: Future[T], *, propagate_cancellation: bool = True) -> T:
    return await _await_executor_future(worker, propagate_cancellation=propagate_cancellation)


async def _await_task_complete[T](task: asyncio.Task[T], *, propagate_cancellation: bool = True) -> T:
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
    result = task.result()
    if cancelled and propagate_cancellation:
        raise asyncio.CancelledError
    return result


def _rename_no_replace(source: Path, destination: Path) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, 'renameat2', None)
    if renameat2 is None:
        raise OSError(errno.ENOSYS, 'renameat2 is unavailable')
    renameat2.argtypes = (ctypes.c_int, ctypes.c_char_p, ctypes.c_int, ctypes.c_char_p, ctypes.c_uint)
    renameat2.restype = ctypes.c_int
    if (
        renameat2(
            _AT_FDCWD,
            os.fsencode(source),
            _AT_FDCWD,
            os.fsencode(destination),
            _RENAME_NOREPLACE,
        )
        == 0
    ):
        return
    error_number = ctypes.get_errno()
    if error_number == errno.EEXIST:
        raise FileExistsError(error_number, os.strerror(error_number), destination)
    raise OSError(error_number, os.strerror(error_number), source, destination)


class FillActorService:
    def __init__(  # noqa: PLR0913
        self,
        *,
        paths: FillActorPaths,
        actor_catalog: ActorCatalog,
        magnet_provider: MagnetProvider,
        brand_resolver: BrandResolver,
        max_actors: int = 20,
        max_videos: int = 2_000,
        magnet_concurrency: int = 8,
        plan_ttl: timedelta = timedelta(hours=1),
        clock: Callable[[], datetime] | None = None,
        token_factory: Callable[[], str] | None = None,
        repository: FillActorRepository | None = None,
        mutation_lock: AsyncFileLock | None = None,
        root_sentinel: str | None = None,
        move_in_by_brand: bool = False,
        apply_enabled: bool = False,
        cloud_file_mover: CloudFileMover | None = None,
        cloud_move_paths: CloudMovePaths | None = None,
    ) -> None:
        if max_actors < 1:
            msg = 'max_actors must be positive'
            raise ValueError(msg)
        if magnet_concurrency < 1:
            msg = 'magnet_concurrency must be positive'
            raise ValueError(msg)
        if max_videos < 1:
            msg = 'max_videos must be positive'
            raise ValueError(msg)
        if plan_ttl <= timedelta(0):
            msg = 'plan_ttl must be positive'
            raise ValueError(msg)
        if (cloud_file_mover is None) is not (cloud_move_paths is None):
            msg = 'CloudDrive mover and path mapping must be configured together'
            raise ValueError(msg)
        if cloud_move_paths is not None and len(cloud_move_paths.source_api_roots) != len(paths.additional_brand_paths):
            msg = 'CloudDrive source roots must match additional roots one-for-one'
            raise ValueError(msg)

        self._paths = paths
        self._actor_catalog = actor_catalog
        self._magnet_provider = magnet_provider
        self._brand_resolver = brand_resolver
        self._max_actors = max_actors
        self._max_videos = max_videos
        self._magnet_concurrency = magnet_concurrency
        self._magnet_semaphore = asyncio.Semaphore(magnet_concurrency)
        self._plan_ttl = plan_ttl
        self._clock = clock or (lambda: datetime.now(UTC))
        self._token_factory = token_factory or (lambda: secrets.token_urlsafe(18))
        self._repository = repository or MemoryFillActorRepository()
        self._mutation_lock = mutation_lock
        if root_sentinel is not None and not self._is_safe_segment(root_sentinel):
            msg = 'root_sentinel must be a safe path segment'
            raise ValueError(msg)
        self._root_sentinel = root_sentinel
        self._move_in_by_brand = move_in_by_brand
        self._apply_enabled = apply_enabled
        self._cloud_file_mover = cloud_file_mover
        self._cloud_move_paths = cloud_move_paths
        self._in_flight: dict[tuple[str, str], asyncio.Task[MoveResult]] = {}
        self._mutation_futures: set[Future[MoveResult]] = set()
        self._scan_futures: dict[Future[object], threading.Event] = {}
        self._apply_lock = asyncio.Lock()
        self._cloud_health_lock = asyncio.Lock()
        self._cloud_health_cache: tuple[float, bool] | None = None

    async def create_plan(  # noqa: C901, PLR0915
        self,
        actor_ids: Sequence[str],
        *,
        plan_id: str | None = None,
        revision: str | None = None,
        progress: ProgressCallback | None = None,
    ) -> FillActorPlan:
        normalized_actor_ids = self._validate_actor_ids(actor_ids)
        actor_plans: list[ActorPlan] = []
        video_actors: dict[str, set[str]] = {}
        actor_total = len(normalized_actor_ids)

        await self._report_progress(
            progress,
            JobProgressEvent(
                stage=JobStage.ACTOR_CATALOG,
                completed=0,
                total=actor_total,
                unit=JobProgressUnit.ACTORS,
                current=f'演员 1/{actor_total}',
            ),
        )

        for actor_index, actor_id in enumerate(normalized_actor_ids, start=1):

            async def report_page(
                page_completed: int,
                page_total: int | None,
                current_page: int | None,
                *,
                _actor_index: int = actor_index,
                _actor_id: str = actor_id,
            ) -> None:
                if current_page is not None:
                    page = f'页面 {current_page}/{page_total}' if page_total is not None else f'页面 {current_page}'
                elif page_total is not None:
                    page = f'已发现 {page_total} 页'
                elif page_completed:
                    page = f'已完成 {page_completed} 页'
                else:
                    page = '正在发现页面'
                await self._report_progress(
                    progress,
                    JobProgressEvent(
                        stage=JobStage.ACTOR_CATALOG,
                        completed=page_completed,
                        total=page_total,
                        unit=JobProgressUnit.PAGES,
                        current=f'演员 {_actor_index}/{actor_total} · {_actor_id} · {page}',
                    ),
                )

            try:
                list_video_ids = self._actor_catalog.list_video_ids
                if self._accepts_keyword(list_video_ids, 'progress_callback'):
                    raw_video_ids = tuple(await list_video_ids(actor_id, progress_callback=report_page))
                else:
                    raw_video_ids = tuple(await list_video_ids(actor_id))
            except Exception:  # noqa: BLE001
                actor_plans.append(ActorPlan(actor_id=actor_id, scraped_count=0, error_code='actor_catalog_error'))
            else:
                video_ids = sorted(
                    {self._normalize_video_id(video_id) for video_id in raw_video_ids if video_id.strip()}
                )
                actor_plans.append(
                    ActorPlan(actor_id=actor_id, scraped_count=len(set(raw_video_ids)), video_ids=tuple(video_ids))
                )
                for video_id in video_ids:
                    video_actors.setdefault(video_id, set()).add(actor_id)
                if len(video_actors) > self._max_videos:
                    raise TooManyVideosError(str(len(video_actors)))
            await self._report_progress(
                progress,
                JobProgressEvent(
                    stage=JobStage.ACTOR_CATALOG,
                    completed=actor_index,
                    total=actor_total,
                    unit=JobProgressUnit.ACTORS,
                    current=f'演员 {actor_index}/{actor_total} · {actor_id}',
                ),
            )

        public_videos: dict[str, VideoPlan] = {}
        records: dict[str, _MoveRecord] = {}
        cloud_directory_cache: dict[str, tuple[CloudFileMetadata, ...] | None] = {}
        magnet_video_ids: list[str] = []
        video_total = len(video_actors)

        await self._report_progress(
            progress,
            JobProgressEvent(
                stage=JobStage.LIBRARY_SCAN,
                completed=0,
                total=video_total,
                unit=JobProgressUnit.VIDEOS,
            ),
        )

        for video_index, video_id in enumerate(sorted(video_actors), start=1):
            actor_membership = tuple(sorted(video_actors[video_id]))
            try:
                video_plan, video_records, needs_magnet = await self._create_video_plan(
                    video_id,
                    actor_membership,
                    cloud_directory_cache=cloud_directory_cache,
                )
            except Exception:  # noqa: BLE001
                public_videos[video_id] = VideoPlan(
                    video_id=video_id,
                    actor_ids=actor_membership,
                    state=VideoState.SCAN_FAILED,
                    warnings=('scan_failed',),
                )
            else:
                public_videos[video_id] = video_plan
                records.update({record.candidate_id: record for record in video_records})
                if needs_magnet:
                    magnet_video_ids.append(video_id)
            await self._report_progress(
                progress,
                JobProgressEvent(
                    stage=JobStage.LIBRARY_SCAN,
                    completed=video_index,
                    total=video_total,
                    unit=JobProgressUnit.VIDEOS,
                    current=video_id,
                ),
            )

        self._mark_duplicate_destination_conflicts(public_videos, records)
        magnet_total = len(magnet_video_ids)
        await self._report_progress(
            progress,
            JobProgressEvent(
                stage=JobStage.MAGNET_LOOKUP,
                completed=0,
                total=magnet_total,
                unit=JobProgressUnit.MAGNETS,
            ),
        )
        magnet_results = await self._find_magnets(magnet_video_ids, progress=progress)
        for video_id, magnet, warning in magnet_results:
            current = public_videos[video_id]
            public_videos[video_id] = current.model_copy(
                update={
                    'state': VideoState.MAGNET_FOUND if magnet else VideoState.MISSING,
                    'magnet': magnet,
                    'warnings': (warning,) if warning else (),
                },
            )

        created_at = self._now()
        plan_id = plan_id or self._token_factory()
        revision = revision or self._token_factory()
        plan = FillActorPlan(
            plan_id=plan_id,
            revision=revision,
            created_at=created_at,
            expires_at=created_at + self._plan_ttl,
            actors=tuple(actor_plans),
            videos=tuple(public_videos[video_id] for video_id in sorted(public_videos)),
        )
        await self._report_progress(
            progress,
            JobProgressEvent(
                stage=JobStage.PERSISTING,
                completed=0,
                total=1,
                unit=JobProgressUnit.STEPS,
                current='保存扫描结果',
            ),
        )
        await self._repository.save_plan(PlanRecord(public=plan, candidates=tuple(records.values())))
        await self._report_progress(
            progress,
            JobProgressEvent(
                stage=JobStage.PERSISTING,
                completed=1,
                total=1,
                unit=JobProgressUnit.STEPS,
                current='扫描结果已保存',
            ),
        )
        return plan

    def validate_actor_ids(self, actor_ids: Sequence[str]) -> tuple[str, ...]:
        return self._validate_actor_ids(actor_ids)

    async def _create_video_plan(
        self,
        video_id: str,
        actor_membership: tuple[str, ...],
        *,
        cloud_directory_cache: dict[str, tuple[CloudFileMetadata, ...] | None],
    ) -> tuple[VideoPlan, tuple[_MoveRecord, ...], bool]:
        brand = self._brand_resolver.resolve_brand(video_id)
        if not brand or not self._is_safe_segment(brand):
            return (
                VideoPlan(
                    video_id=video_id,
                    actor_ids=actor_membership,
                    state=VideoState.INVALID_VIDEO_ID,
                    warnings=('brand_not_found',),
                ),
                (),
                False,
            )

        existing = await self._run_scan(
            self._find_matching_files,
            self._paths.actor_brand_path,
            brand,
            video_id,
        )
        if existing:
            return (
                VideoPlan(
                    video_id=video_id,
                    actor_ids=actor_membership,
                    state=VideoState.EXISTS,
                    existing_files=tuple(path.name for path in existing),
                ),
                (),
                False,
            )

        additional = await self._run_scan(self._find_additional_files, brand, video_id)
        if not additional:
            return (
                VideoPlan(video_id=video_id, actor_ids=actor_membership, state=VideoState.MISSING),
                (),
                True,
            )

        if self._cloud_file_mover is not None and self._cloud_move_paths is not None:
            return await self._create_cloud_video_plan(
                video_id=video_id,
                actor_membership=actor_membership,
                brand=brand,
                additional=additional,
                directory_cache=cloud_directory_cache,
            )

        candidates: list[MoveCandidate] = []
        records: list[_MoveRecord] = []
        for source_index, source in additional:
            candidate_id = self._token_factory()
            destination_root = self._paths.move_in_path / brand if self._move_in_by_brand else self._paths.move_in_path
            destination = destination_root / source.name
            records.append(
                _MoveRecord(
                    candidate_id=candidate_id,
                    video_id=video_id,
                    source=source,
                    source_root=self._paths.additional_brand_paths[source_index],
                    destination=destination,
                    fingerprint=self._fingerprint(source),
                )
            )
            candidates.append(
                MoveCandidate(
                    candidate_id=candidate_id,
                    video_id=video_id,
                    file_name=source.name,
                    source_label=f'additional-{source_index + 1}',
                    destination_conflict=destination.exists(),
                )
            )
        return (
            VideoPlan(
                video_id=video_id,
                actor_ids=actor_membership,
                state=VideoState.ADDITIONAL_FOUND,
                move_candidates=tuple(candidates),
            ),
            tuple(records),
            False,
        )

    async def _create_cloud_video_plan(
        self,
        *,
        video_id: str,
        actor_membership: tuple[str, ...],
        brand: str,
        additional: tuple[tuple[int, Path], ...],
        directory_cache: dict[str, tuple[CloudFileMetadata, ...] | None],
    ) -> tuple[VideoPlan, tuple[_MoveRecord, ...], bool]:
        if self._cloud_move_paths is None:
            msg = 'CloudDrive paths are not configured'
            raise RuntimeError(msg)
        candidates: list[MoveCandidate] = []
        records: list[_MoveRecord] = []
        existing_files: list[str] = []
        warnings: list[str] = []
        destination_dir = self._cloud_move_paths.destination_directory(brand)
        destination_listing = await self._list_cloud_directory_cached(destination_dir, directory_cache)

        for source_index, mapping_path in additional:
            if mapping_path.suffix.casefold() != '.strm':
                warnings.append('cloud_mapping_not_strm')
                continue
            try:
                parsed = self._cloud_move_paths.parse_mapping(mapping_path, source_index=source_index)
            except (InvalidStrmTargetError, ValueError):
                warnings.append('invalid_strm_target')
                continue
            cloud_file = await self._cloud_file_cached(parsed.api_path, directory_cache)
            destination_path = str(PurePosixPath(destination_dir) / PurePosixPath(parsed.api_path).name)
            destination_file = self._find_cloud_file(destination_path, destination_listing)
            if cloud_file is None:
                if destination_file is not None:
                    existing_files.append(destination_file.name)
                else:
                    warnings.append('cloud_source_missing')
                continue
            if not self._matches_video_file_name(cloud_file.name, video_id):
                warnings.append('cloud_source_name_mismatch')
                continue

            candidate_id = self._token_factory()
            mapping_destination_root = (
                self._paths.move_in_path / brand if self._move_in_by_brand else self._paths.move_in_path
            )
            records.append(
                _MoveRecord(
                    candidate_id=candidate_id,
                    video_id=video_id,
                    source=mapping_path,
                    source_root=self._paths.additional_brand_paths[source_index],
                    destination=mapping_destination_root / mapping_path.name,
                    fingerprint=self._fingerprint(mapping_path),
                    kind=CandidateKind.CLOUD_STRM,
                    mapping_sha256=parsed.mapping_sha256,
                    cloud_source_path=parsed.api_path,
                    cloud_destination_dir=destination_dir,
                    cloud_file=cloud_file,
                )
            )
            candidates.append(
                MoveCandidate(
                    candidate_id=candidate_id,
                    video_id=video_id,
                    file_name=cloud_file.name,
                    source_label=f'additional-{source_index + 1}',
                    destination_conflict=destination_file is not None,
                )
            )

        if candidates:
            return (
                VideoPlan(
                    video_id=video_id,
                    actor_ids=actor_membership,
                    state=VideoState.ADDITIONAL_FOUND,
                    existing_files=tuple(sorted(set(existing_files))),
                    move_candidates=tuple(candidates),
                    warnings=tuple(dict.fromkeys(warnings)),
                ),
                tuple(records),
                False,
            )
        if existing_files:
            return (
                VideoPlan(
                    video_id=video_id,
                    actor_ids=actor_membership,
                    state=VideoState.EXISTS,
                    existing_files=tuple(sorted(set(existing_files))),
                    warnings=('mapping_convergence_pending',),
                ),
                (),
                False,
            )
        return (
            VideoPlan(
                video_id=video_id,
                actor_ids=actor_membership,
                state=VideoState.SCAN_FAILED,
                warnings=tuple(dict.fromkeys(warnings)) or ('cloud_scan_failed',),
            ),
            (),
            False,
        )

    async def _list_cloud_directory_cached(
        self,
        api_directory: str,
        cache: dict[str, tuple[CloudFileMetadata, ...] | None],
    ) -> tuple[CloudFileMetadata, ...] | None:
        if api_directory not in cache:
            if self._cloud_file_mover is None:
                msg = 'CloudDrive mover is not configured'
                raise RuntimeError(msg)
            try:
                cache[api_directory] = await self._cloud_file_mover.list_directory(api_directory)
            except FileNotFoundError:
                cache[api_directory] = None
        return cache[api_directory]

    async def _cloud_file_cached(
        self,
        api_path: str,
        cache: dict[str, tuple[CloudFileMetadata, ...] | None],
    ) -> CloudFileMetadata | None:
        parent = str(PurePosixPath(api_path).parent)
        return self._find_cloud_file(api_path, await self._list_cloud_directory_cached(parent, cache))

    @staticmethod
    def _find_cloud_file(
        api_path: str,
        listing: tuple[CloudFileMetadata, ...] | None,
    ) -> CloudFileMetadata | None:
        if listing is None:
            return None
        return next((file for file in listing if file.path == api_path), None)

    @staticmethod
    def _matches_video_file_name(file_name: str, video_id: str) -> bool:
        pattern = re.compile(rf'^{re.escape(video_id)}(?:-cd\d{{1,2}})?\..+', re.IGNORECASE)
        return bool(pattern.fullmatch(file_name))

    async def apply(self, *, plan_id: str, revision: str, candidate_ids: Sequence[str]) -> ApplyResult:
        if not self._apply_enabled:
            raise MoveDisabledError
        async with self._apply_lock:
            stored = await self._get_plan(plan_id, revision)
            selected = tuple(dict.fromkeys(candidate_ids))
            candidates = {candidate.candidate_id: candidate for candidate in stored.candidates}
            unknown = [candidate_id for candidate_id in selected if candidate_id not in candidates]
            if unknown:
                raise UnknownCandidateError(unknown[0])
            if self._cloud_file_mover is not None and any(
                candidates[candidate_id].kind is not CandidateKind.CLOUD_STRM for candidate_id in selected
            ):
                raise LegacyPlanError

            results: list[MoveResult] = []
            for candidate_id in selected:
                cached = await self._repository.get_move_result(plan_id, candidate_id)
                if cached is not None:
                    results.append(cached)
                    continue
                record = candidates[candidate_id]
                task_key = (plan_id, candidate_id)
                worker = self._in_flight.get(task_key)
                if worker is None:
                    worker = asyncio.create_task(self._run_move(plan_id, record))
                    self._in_flight[task_key] = worker
                    worker.add_done_callback(partial(self._discard_in_flight, task_key))
                result = await asyncio.shield(worker)
                results.append(result)

        return ApplyResult(
            plan_id=plan_id,
            revision=revision,
            state=self._get_apply_state(results),
            results=tuple(results),
        )

    def _validate_actor_ids(self, actor_ids: Sequence[str]) -> tuple[str, ...]:
        normalized = tuple(dict.fromkeys(actor_id.strip() for actor_id in actor_ids))
        if not normalized:
            empty_actor_id = ''
            raise InvalidActorIdError(empty_actor_id)
        if len(normalized) > self._max_actors:
            raise TooManyActorsError(str(len(normalized)))
        for actor_id in normalized:
            if not ACTOR_ID_RE.fullmatch(actor_id):
                raise InvalidActorIdError(actor_id)
        return normalized

    @staticmethod
    def _normalize_video_id(video_id: str) -> str:
        cleaned = video_id.strip()
        if match := DATED_VIDEO_ID_RE.fullmatch(cleaned):
            cleaned = match.group(1)
        return cleaned.upper()

    @staticmethod
    def _find_matching_files(
        root: Path,
        brand: str,
        video_id: str,
        stop_requested: threading.Event | None = None,
    ) -> tuple[Path, ...]:
        if stop_requested is not None and stop_requested.is_set():
            return ()
        if not root.is_dir() or root.is_symlink():
            msg = 'scan root unavailable'
            raise OSError(msg)
        if stop_requested is not None and stop_requested.is_set():
            return ()
        brand_path = root / brand
        if not brand_path.is_dir():
            return ()
        if FillActorService._has_symlink_component(brand_path, root) or not FillActorService._is_within(
            brand_path,
            root,
        ):
            msg = 'unsafe brand path'
            raise ValueError(msg)
        pattern = re.compile(rf'^{re.escape(video_id)}(?:-cd\d{{1,2}})?\..+', re.IGNORECASE)
        matches: list[Path] = []
        for path in brand_path.iterdir():
            if stop_requested is not None and stop_requested.is_set():
                return ()
            if not path.is_symlink() and path.is_file() and pattern.fullmatch(path.name):
                matches.append(path)
        return tuple(sorted(matches, key=lambda path: path.name))

    def _find_additional_files(
        self,
        brand: str,
        video_id: str,
        stop_requested: threading.Event | None = None,
    ) -> tuple[tuple[int, Path], ...]:
        result: list[tuple[int, Path]] = []
        for index, root in enumerate(self._paths.additional_brand_paths):
            if stop_requested is not None and stop_requested.is_set():
                return ()
            matches = self._find_matching_files(root, brand, video_id, stop_requested)
            result.extend((index, path) for path in matches)
        return tuple(sorted(result, key=lambda item: (item[0], item[1].name)))

    @staticmethod
    def _mark_duplicate_destination_conflicts(
        videos: dict[str, VideoPlan],
        records: dict[str, _MoveRecord],
    ) -> None:
        destination_counts = Counter(FillActorService._candidate_destination_key(record) for record in records.values())
        for video_id, video in videos.items():
            updated_candidates = tuple(
                candidate.model_copy(
                    update={
                        'destination_conflict': candidate.destination_conflict
                        or destination_counts[
                            FillActorService._candidate_destination_key(records[candidate.candidate_id])
                        ]
                        > 1,
                    }
                )
                for candidate in video.move_candidates
            )
            if updated_candidates:
                videos[video_id] = video.model_copy(update={'move_candidates': updated_candidates})

    @staticmethod
    def _candidate_destination_key(record: _MoveRecord) -> str:
        if record.kind is CandidateKind.CLOUD_STRM:
            if record.cloud_destination_dir is None or record.cloud_file is None:
                msg = 'CloudDrive candidate is incomplete'
                raise ValueError(msg)
            return str(PurePosixPath(record.cloud_destination_dir) / record.cloud_file.name)
        return str(record.destination)

    async def _find_magnets(
        self,
        video_ids: Sequence[str],
        *,
        progress: ProgressCallback | None = None,
    ) -> list[tuple[str, str | None, str | None]]:
        async def find(video_id: str) -> tuple[str, str | None, str | None]:
            async with self._magnet_semaphore:
                try:
                    raw_magnet = await self._magnet_provider.find_magnet(video_id)
                    magnet = self._sanitize_magnet(raw_magnet)
                except Exception:  # noqa: BLE001
                    return video_id, None, 'magnet_lookup_failed'
                warning = 'invalid_magnet' if raw_magnet is not None and magnet is None else None
                return video_id, magnet, warning

        queue = asyncio.Queue[str]()
        for video_id in video_ids:
            queue.put_nowait(video_id)
        results: dict[str, tuple[str, str | None, str | None]] = {}
        progress_lock = asyncio.Lock()
        completed = 0

        async def worker() -> None:
            nonlocal completed
            while True:
                try:
                    video_id = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                results[video_id] = await find(video_id)
                async with progress_lock:
                    completed += 1
                    await self._report_progress(
                        progress,
                        JobProgressEvent(
                            stage=JobStage.MAGNET_LOOKUP,
                            completed=completed,
                            total=len(video_ids),
                            unit=JobProgressUnit.MAGNETS,
                            current=video_id,
                        ),
                    )

        worker_count = min(self._magnet_concurrency, len(video_ids))
        await asyncio.gather(*(worker() for _ in range(worker_count)))
        return [results[video_id] for video_id in video_ids]

    @staticmethod
    async def _report_progress(progress: ProgressCallback | None, event: JobProgressEvent) -> None:
        if progress is not None:
            await progress(event)

    @staticmethod
    def _accepts_keyword(function: Callable[..., object], name: str) -> bool:
        parameters = inspect.signature(function).parameters
        parameter = parameters.get(name)
        return (
            parameter is not None
            and parameter.kind
            in {
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            }
        ) or any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters.values())

    async def get_plan(self, plan_id: str) -> FillActorPlan:
        return (await self._get_plan(plan_id)).public

    async def _get_plan(self, plan_id: str, revision: str | None = None) -> PlanRecord:
        stored = await self._repository.get_plan(plan_id)
        if stored is None:
            raise UnknownPlanError(plan_id)
        if revision is not None and stored.public.revision != revision:
            raise RevisionMismatchError(revision)
        now = self._now()
        if now >= stored.public.expires_at:
            raise ExpiredPlanError(plan_id)
        return stored

    def _now(self) -> datetime:
        now = self._clock()
        return now if now.tzinfo is not None else now.replace(tzinfo=UTC)

    def _apply_one(self, record: _MoveRecord) -> MoveResult:  # noqa: C901, PLR0911, PLR0912
        base = {
            'candidate_id': record.candidate_id,
            'video_id': record.video_id,
            'file_name': record.source.name,
        }
        if self._has_symlink_component(record.source, record.source_root):
            return MoveResult(**base, state=MoveState.INVALID_PATH, error_code='source_symlink')
        if self._has_symlink_component(record.destination, self._paths.move_in_path):
            return MoveResult(**base, state=MoveState.INVALID_PATH, error_code='destination_symlink')
        if not self._paths.move_in_path.is_dir():
            return MoveResult(**base, state=MoveState.INVALID_PATH, error_code='destination_root_missing')
        if not self._is_within(record.source, record.source_root) or not self._is_within(
            record.destination,
            self._paths.move_in_path,
        ):
            return MoveResult(**base, state=MoveState.INVALID_PATH, error_code='path_outside_root')
        if not record.source.is_file():
            return MoveResult(**base, state=MoveState.STALE, error_code='source_missing')
        if self._fingerprint(record.source) != record.fingerprint:
            return MoveResult(**base, state=MoveState.STALE, error_code='source_changed')
        if record.destination.exists():
            return MoveResult(**base, state=MoveState.CONFLICT, error_code='destination_exists')
        if not self._prepare_destination_parent(record.destination):
            return MoveResult(**base, state=MoveState.INVALID_PATH, error_code='destination_parent_unavailable')
        if self._has_symlink_component(record.destination, self._paths.move_in_path):
            return MoveResult(**base, state=MoveState.INVALID_PATH, error_code='destination_symlink')
        try:
            _rename_no_replace(record.source, record.destination)
        except FileExistsError:
            return MoveResult(**base, state=MoveState.CONFLICT, error_code='destination_exists')
        except OSError as exc:
            if exc.errno in _RENAME_NOREPLACE_UNSUPPORTED:
                return self._apply_hardlink_fallback(record, base)
            error_code = 'cross_device_move' if exc.errno == errno.EXDEV else 'move_failed'
            return MoveResult(**base, state=MoveState.FAILED, error_code=error_code)
        try:
            destination_matches = not record.destination.is_symlink() and self._matches_linked_identity(
                record.destination,
                record.fingerprint,
            )
        except OSError:
            destination_matches = False
        if not destination_matches:
            try:
                _rename_no_replace(record.destination, record.source)
            except OSError:
                return MoveResult(**base, state=MoveState.FAILED, error_code='move_rollback_failed')
            return MoveResult(**base, state=MoveState.STALE, error_code='source_changed_during_move')
        return MoveResult(**base, state=MoveState.MOVED)

    def _apply_hardlink_fallback(  # noqa: PLR0911
        self,
        record: _MoveRecord,
        base: dict[str, str],
    ) -> MoveResult:
        """Move with a no-overwrite hard link when NFS rejects renameat2 flags."""
        quarantine = self._reconcile_quarantine_path(record)
        if quarantine.exists():
            return MoveResult(**base, state=MoveState.FAILED, error_code='move_rollback_failed')
        try:
            os.link(record.source, record.destination, follow_symlinks=False)
        except FileExistsError:
            return MoveResult(**base, state=MoveState.CONFLICT, error_code='destination_exists')
        except OSError as exc:
            if exc.errno == errno.EXDEV:
                error_code = 'cross_device_move'
            elif exc.errno in {errno.EPERM, errno.EOPNOTSUPP, errno.ENOSYS}:
                error_code = 'move_unsupported'
            else:
                error_code = 'move_failed'
            return MoveResult(**base, state=MoveState.FAILED, error_code=error_code)

        destination_matches = self._matches_linked_fingerprint_if_present(record.destination, record.fingerprint)
        source_matches = self._matches_linked_fingerprint_if_present(record.source, record.fingerprint)
        if not destination_matches:
            return self._rollback_unexpected_link(record, quarantine, base)
        if not record.source.exists() or not source_matches:
            return MoveResult(**base, state=MoveState.MOVED)
        try:
            self._move_to_quarantine(record.source, quarantine)
        except OSError:
            return MoveResult(**base, state=MoveState.FAILED, error_code='move_rollback_failed')
        return self._finish_quarantined_reconcile(record, quarantine, base)

    def _rollback_unexpected_link(
        self,
        record: _MoveRecord,
        quarantine: Path,
        base: dict[str, str],
    ) -> MoveResult:
        try:
            self._move_to_quarantine(record.destination, quarantine)
        except OSError:
            return MoveResult(**base, state=MoveState.FAILED, error_code='move_rollback_failed')
        if record.source.exists() and self._same_inode(quarantine, record.source):
            try:
                quarantine.unlink()
            except OSError:
                return MoveResult(**base, state=MoveState.FAILED, error_code='move_rollback_failed')
            return MoveResult(**base, state=MoveState.STALE, error_code='source_changed_during_move')
        try:
            self._restore_quarantine_no_replace(quarantine, record.destination)
        except OSError:
            return MoveResult(**base, state=MoveState.FAILED, error_code='move_rollback_failed')
        return MoveResult(**base, state=MoveState.CONFLICT, error_code='destination_changed_during_move')

    def _prepare_destination_parent(self, destination: Path) -> bool:
        parent = destination.parent
        if parent == self._paths.move_in_path:
            return True
        if not self._is_within(parent, self._paths.move_in_path) or parent.parent != self._paths.move_in_path:
            return False
        try:
            parent.mkdir(mode=0o755, exist_ok=True)
        except OSError:
            return False
        return parent.is_dir() and not parent.is_symlink()

    @staticmethod
    def _fingerprint(path: Path) -> _Fingerprint:
        stat = path.stat()
        return _Fingerprint(
            device=stat.st_dev,
            inode=stat.st_ino,
            size=stat.st_size,
            mtime_ns=stat.st_mtime_ns,
            ctime_ns=stat.st_ctime_ns,
        )

    @staticmethod
    def _matches_linked_identity(path: Path, fingerprint: _Fingerprint) -> bool:
        stat = path.stat()
        return (
            stat.st_dev == fingerprint.device
            and stat.st_ino == fingerprint.inode
            and stat.st_size == fingerprint.size
            and stat.st_mtime_ns == fingerprint.mtime_ns
        )

    @staticmethod
    def _sanitize_magnet(value: str | None) -> str | None:
        if not isinstance(value, str) or len(value) > MAX_MAGNET_LENGTH or not value.lower().startswith('magnet:'):
            return None
        return value

    @staticmethod
    def _is_safe_segment(value: str) -> bool:
        if not value or value in {'.', '..'} or '/' in value or '\\' in value or '\x00' in value:
            return False
        path = Path(value)
        return not path.is_absolute() and path.name == value

    @staticmethod
    def _is_within(path: Path, root: Path) -> bool:
        try:
            path.resolve(strict=False).relative_to(root.resolve(strict=True))
        except (FileNotFoundError, ValueError):
            return False
        return True

    @staticmethod
    def _has_symlink_component(path: Path, root: Path) -> bool:
        try:
            relative = path.absolute().relative_to(root.absolute())
        except ValueError:
            return True
        current = root
        if current.is_symlink():
            return True
        for part in relative.parts:
            current /= part
            if current.is_symlink():
                return True
        return False

    @staticmethod
    def _get_apply_state(results: Sequence[MoveResult]) -> ApplyState:
        if not results or all(result.state is MoveState.MOVED for result in results):
            return ApplyState.SUCCEEDED
        if any(result.state is MoveState.MOVED for result in results):
            return ApplyState.PARTIAL_FAILED
        return ApplyState.FAILED

    async def reconcile_moves(self) -> tuple[MoveResult, ...]:
        if not self._apply_enabled:
            return ()
        if not await self.roots_ready():
            return ()
        if self._cloud_file_mover is not None:
            results: list[MoveResult] = []
            for operation in await self._repository.list_unresolved_cloud_moves():
                record = await self._repository.get_candidate(operation.plan_id, operation.candidate_id)
                if record is None or record.kind is not CandidateKind.CLOUD_STRM:
                    continue
                context = self._mutation_lock.acquire() if self._mutation_lock is not None else _unlocked()
                async with context:
                    current = await self._repository.get_cloud_move_operation(
                        operation.plan_id,
                        operation.candidate_id,
                    )
                    if current is None or current.state.terminal:
                        continue
                    results.append(await self._recover_cloud_move(operation.plan_id, record, current))
            return tuple(results)
        results: list[MoveResult] = []
        for journal in await self._repository.list_unreconciled_moves():
            record = await self._repository.get_candidate(journal.plan_id, journal.candidate_id)
            if record is None:
                continue
            context = self._mutation_lock.acquire() if self._mutation_lock is not None else _unlocked()
            async with context:
                current = await self._repository.get_move_journal(journal.plan_id, journal.candidate_id)
                if current is None or current.state is MoveJournalState.RECONCILED:
                    continue
                result = await self._reconcile_candidate(journal.plan_id, record, current)
                results.append(result)
        return tuple(results)

    @property
    def apply_enabled(self) -> bool:
        return self._apply_enabled

    async def ready(self) -> bool:
        return await self.scan_ready()

    async def scan_ready(self) -> bool:
        return await self.roots_ready() and await self.cloud_ready()

    async def apply_ready(self) -> bool:
        return await self.scan_ready() and await self.legacy_journal_ready()

    async def cloud_ready(self) -> bool:
        if self._cloud_file_mover is None:
            return self._cloud_move_paths is None
        if self._cloud_move_paths is None:
            return False
        now = asyncio.get_running_loop().time()
        cached = self._cloud_health_cache
        if cached is not None and now < cached[0]:
            return cached[1]
        async with self._cloud_health_lock:
            now = asyncio.get_running_loop().time()
            cached = self._cloud_health_cache
            if cached is not None and now < cached[0]:
                return cached[1]
            try:
                roots = (*self._cloud_move_paths.source_api_roots, self._cloud_move_paths.move_in_api_root)
                for root in roots:
                    await self._cloud_file_mover.list_directory(str(root))
            except Exception:  # noqa: BLE001
                result = False
                ttl = CLOUD_HEALTH_FAILURE_TTL_SECONDS
            else:
                result = True
                ttl = CLOUD_HEALTH_SUCCESS_TTL_SECONDS
            self._cloud_health_cache = (now + ttl, result)
            return result

    async def legacy_journal_ready(self) -> bool:
        if self._cloud_file_mover is None:
            return True
        return not await self._repository.list_unreconciled_moves()

    async def roots_ready(self) -> bool:
        roots = (
            self._paths.actor_brand_path,
            *self._paths.additional_brand_paths,
            self._paths.move_in_path,
        )

        def check() -> bool:
            if not all(root.is_dir() and not root.is_symlink() for root in roots):
                return False
            if self._root_sentinel is not None and not all(
                (root / self._root_sentinel).is_file() and not (root / self._root_sentinel).is_symlink()
                for root in roots
            ):
                return False
            try:
                destination_device = self._paths.move_in_path.stat().st_dev
                return all(root.stat().st_dev == destination_device for root in self._paths.additional_brand_paths)
            except OSError:
                return False

        return await asyncio.to_thread(check)

    async def aclose(self) -> None:
        scan_futures = tuple(self._scan_futures.items())
        for _worker, stop_requested in scan_futures:
            stop_requested.set()
        for worker, _stop_requested in scan_futures:
            await _await_executor_future(worker, propagate_cancellation=False)
        for worker in tuple(self._mutation_futures):
            await _await_mutation_future(worker, propagate_cancellation=False)
        for task in tuple(self._in_flight.values()):
            with suppress(asyncio.CancelledError, Exception):
                await _await_task_complete(task, propagate_cancellation=False)

    async def _run_cloud_move(  # noqa: C901, PLR0911, PLR0912, PLR0915
        self,
        plan_id: str,
        record: _MoveRecord,
    ) -> MoveResult:
        base = self._cloud_result_base(record)
        if self._cloud_file_mover is None or self._cloud_move_paths is None or record.cloud_file is None:
            return MoveResult(**base, state=MoveState.FAILED, error_code='cloud_move_not_configured')
        if not await self.roots_ready():
            return MoveResult(**base, state=MoveState.FAILED, error_code='root_unavailable')
        context = self._mutation_lock.acquire() if self._mutation_lock is not None else _unlocked()
        async with context:
            cached = await self._repository.get_move_result(plan_id, record.candidate_id)
            if cached is not None:
                return cached
            operation = await self._repository.get_cloud_move_operation(plan_id, record.candidate_id)
            if operation is not None:
                return await self._recover_cloud_move(plan_id, record, operation)

            mapping_error = self._validate_cloud_mapping(record)
            if mapping_error is not None:
                result = MoveResult(**base, state=MoveState.STALE, error_code=mapping_error)
                await self._repository.save_move_result(plan_id, result)
                return result

            try:
                source = await self._stat_cloud_file(record.cloud_source_path)
                destination_listing = await self._list_cloud_directory(record.cloud_destination_dir)
            except Exception:  # noqa: BLE001
                return MoveResult(**base, state=MoveState.FAILED, error_code='cloud_preflight_failed')
            if source is None:
                result = MoveResult(**base, state=MoveState.STALE, error_code='cloud_source_missing')
                await self._repository.save_move_result(plan_id, result)
                return result
            if not record.cloud_file.matches_identity(source):
                result = MoveResult(**base, state=MoveState.STALE, error_code='cloud_source_changed')
                await self._repository.save_move_result(plan_id, result)
                return result
            if destination_listing is None:
                destination = self._cloud_destination_components(record)
                if destination is None:
                    result = MoveResult(
                        **base,
                        state=MoveState.INVALID_PATH,
                        error_code='cloud_destination_changed',
                    )
                    await self._repository.save_move_result(plan_id, result)
                    return result
                destination_parent, destination_brand, _destination_dir = destination
                try:
                    ensured = await self._cloud_file_mover.ensure_directory(
                        destination_parent,
                        destination_brand,
                    )
                    destination_listing = (
                        await self._list_cloud_directory(record.cloud_destination_dir) if ensured else None
                    )
                except Exception:  # noqa: BLE001
                    destination_listing = None
                if destination_listing is None:
                    result = MoveResult(
                        **base,
                        state=MoveState.INVALID_PATH,
                        error_code='cloud_destination_missing',
                    )
                    await self._repository.save_move_result(plan_id, result)
                    return result
            destination_path = str(PurePosixPath(record.cloud_destination_dir) / record.cloud_file.name)
            if self._find_cloud_file(destination_path, destination_listing) is not None:
                result = MoveResult(**base, state=MoveState.CONFLICT, error_code='cloud_destination_exists')
                await self._repository.save_move_result(plan_id, result)
                return result

            # MoveFile accepts only a path, not an expected file id. Recheck immediately
            # before journaling/submission to minimize the unavoidable remote TOCTOU window.
            try:
                final_source = await self._stat_cloud_file(record.cloud_source_path)
                final_destination = await self._stat_cloud_file(destination_path)
            except Exception:  # noqa: BLE001
                return MoveResult(**base, state=MoveState.FAILED, error_code='cloud_preflight_failed')
            if final_source is None or not record.cloud_file.matches_identity(final_source):
                result = MoveResult(**base, state=MoveState.STALE, error_code='cloud_source_changed')
                await self._repository.save_move_result(plan_id, result)
                return result
            if final_destination is not None:
                result = MoveResult(**base, state=MoveState.CONFLICT, error_code='cloud_destination_exists')
                await self._repository.save_move_result(plan_id, result)
                return result

            operation = CloudMoveOperationRecord(
                plan_id=plan_id,
                candidate_id=record.candidate_id,
                attempt_id=self._token_factory(),
                source_path=record.cloud_source_path,
                destination_dir=record.cloud_destination_dir,
                state=CloudMoveOperationState.PREPARED,
                updated_at=self._now(),
            )
            try:
                await self._repository.save_cloud_move_operation(operation)
            except ValueError:
                return MoveResult(**base, state=MoveState.FAILED, error_code='cloud_move_in_progress')
            operation = await self._advance_cloud_operation(operation, CloudMoveOperationState.SUBMITTING)
            try:
                response = await self._cloud_file_mover.move_file(
                    record.cloud_source_path,
                    record.cloud_destination_dir,
                )
            except Exception:  # noqa: BLE001
                operation = await self._advance_cloud_operation(
                    operation,
                    CloudMoveOperationState.UNKNOWN,
                    error_code='cloud_move_transport_unknown',
                )
                return await self._observe_cloud_move(plan_id, record, operation)

            operation = await self._advance_cloud_operation(operation, CloudMoveOperationState.VERIFYING)
            for attempt in range(CLOUD_VERIFY_ATTEMPTS):
                result = await self._observe_cloud_move(
                    plan_id,
                    record,
                    operation,
                    response=response,
                    final=attempt == CLOUD_VERIFY_ATTEMPTS - 1,
                )
                if result.error_code != 'cloud_move_verifying':
                    return result
                await asyncio.sleep(0.5)
            return MoveResult(**base, state=MoveState.FAILED, error_code='cloud_move_status_unknown')

    async def _recover_cloud_move(
        self,
        plan_id: str,
        record: _MoveRecord,
        operation: CloudMoveOperationRecord,
    ) -> MoveResult:
        if operation.state.terminal:
            result = self._terminal_cloud_result(record, operation)
            await self._repository.finalize_cloud_move(operation, result)
            return result
        if operation.state is CloudMoveOperationState.PREPARED:
            return await self._complete_cloud_move(
                plan_id,
                record,
                operation,
                operation_state=CloudMoveOperationState.FAILED,
                move_state=MoveState.FAILED,
                error_code='cloud_move_not_submitted',
            )
        if operation.state is CloudMoveOperationState.SUBMITTING:
            operation = await self._advance_cloud_operation(
                operation,
                CloudMoveOperationState.UNKNOWN,
                error_code='cloud_move_interrupted',
            )
        return await self._observe_cloud_move(plan_id, record, operation, final=True)

    async def _observe_cloud_move(  # noqa: PLR0911
        self,
        plan_id: str,
        record: _MoveRecord,
        operation: CloudMoveOperationRecord,
        *,
        response: CloudMoveResponse | None = None,
        final: bool = True,
    ) -> MoveResult:
        base = self._cloud_result_base(record)
        if record.cloud_file is None:
            return MoveResult(**base, state=MoveState.FAILED, error_code='cloud_candidate_incomplete')
        destination_path = str(PurePosixPath(operation.destination_dir) / record.cloud_file.name)
        try:
            source = await self._stat_cloud_file(operation.source_path)
            destination = await self._stat_cloud_file(destination_path)
        except Exception:  # noqa: BLE001
            if operation.state is not CloudMoveOperationState.UNKNOWN:
                operation = await self._advance_cloud_operation(
                    operation,
                    CloudMoveOperationState.UNKNOWN,
                    error_code='cloud_observation_failed',
                )
            return MoveResult(**base, state=MoveState.FAILED, error_code='cloud_move_status_unknown')

        source_matches = source is not None and record.cloud_file.matches_identity(source)
        destination_matches = destination is not None and record.cloud_file.matches_identity(destination)
        if source is None and destination_matches:
            return await self._complete_cloud_move(
                plan_id,
                record,
                operation,
                operation_state=CloudMoveOperationState.SUCCEEDED,
                move_state=MoveState.MOVED,
                error_code=None,
            )
        if source_matches and destination is not None and not destination_matches:
            return await self._complete_cloud_move(
                plan_id,
                record,
                operation,
                operation_state=CloudMoveOperationState.CONFLICT,
                move_state=MoveState.CONFLICT,
                error_code='cloud_destination_exists',
            )
        if source is not None and not source_matches:
            return await self._complete_cloud_move(
                plan_id,
                record,
                operation,
                operation_state=CloudMoveOperationState.FAILED,
                move_state=MoveState.STALE,
                error_code='cloud_source_changed',
            )
        if response is not None and not response.success and source_matches and destination is None:
            return await self._complete_cloud_move(
                plan_id,
                record,
                operation,
                operation_state=CloudMoveOperationState.FAILED,
                move_state=MoveState.FAILED,
                error_code='cloud_move_rejected',
            )
        if not final:
            return MoveResult(**base, state=MoveState.FAILED, error_code='cloud_move_verifying')
        if operation.state is not CloudMoveOperationState.UNKNOWN:
            operation = await self._advance_cloud_operation(
                operation,
                CloudMoveOperationState.UNKNOWN,
                error_code='cloud_move_status_unknown',
            )
        return MoveResult(**base, state=MoveState.FAILED, error_code='cloud_move_status_unknown')

    async def _complete_cloud_move(  # noqa: PLR0913
        self,
        plan_id: str,
        record: _MoveRecord,
        operation: CloudMoveOperationRecord,
        *,
        operation_state: CloudMoveOperationState,
        move_state: MoveState,
        error_code: str | None,
    ) -> MoveResult:
        if operation.plan_id != plan_id:
            msg = 'CloudDrive operation plan mismatch'
            raise ValueError(msg)
        operation = CloudMoveOperationRecord(
            plan_id=operation.plan_id,
            candidate_id=operation.candidate_id,
            attempt_id=operation.attempt_id,
            source_path=operation.source_path,
            destination_dir=operation.destination_dir,
            state=operation_state,
            updated_at=self._now(),
            error_code=error_code if operation_state is not CloudMoveOperationState.SUCCEEDED else None,
        )
        result = MoveResult(
            **self._cloud_result_base(record),
            state=move_state,
            error_code=error_code,
        )
        await self._repository.finalize_cloud_move(operation, result)
        return result

    async def _advance_cloud_operation(
        self,
        operation: CloudMoveOperationRecord,
        state: CloudMoveOperationState,
        *,
        error_code: str | None = None,
    ) -> CloudMoveOperationRecord:
        updated = CloudMoveOperationRecord(
            plan_id=operation.plan_id,
            candidate_id=operation.candidate_id,
            attempt_id=operation.attempt_id,
            source_path=operation.source_path,
            destination_dir=operation.destination_dir,
            state=state,
            updated_at=self._now(),
            error_code=error_code,
        )
        await self._repository.save_cloud_move_operation(updated)
        return updated

    async def _list_cloud_directory(self, api_directory: str | None) -> tuple[CloudFileMetadata, ...] | None:
        if api_directory is None or self._cloud_file_mover is None:
            return None
        try:
            return await self._cloud_file_mover.list_directory(api_directory)
        except FileNotFoundError:
            return None

    async def _stat_cloud_file(self, api_path: str | None) -> CloudFileMetadata | None:
        if api_path is None:
            return None
        listing = await self._list_cloud_directory(str(PurePosixPath(api_path).parent))
        return self._find_cloud_file(api_path, listing)

    def _validate_cloud_mapping(self, record: _MoveRecord) -> str | None:  # noqa: PLR0911
        if self._cloud_move_paths is None or record.mapping_sha256 is None or record.cloud_source_path is None:
            return 'cloud_candidate_incomplete'
        if self._has_symlink_component(record.source, record.source_root):
            return 'source_symlink'
        if not self._is_within(record.source, record.source_root) or not record.source.is_file():
            return 'source_missing'
        try:
            if self._fingerprint(record.source) != record.fingerprint:
                return 'source_changed'
            source_index = self._paths.additional_brand_paths.index(record.source_root)
            parsed = self._cloud_move_paths.parse_mapping(record.source, source_index=source_index)
        except (OSError, ValueError, InvalidStrmTargetError):
            return 'invalid_strm_target'
        if parsed.mapping_sha256 != record.mapping_sha256 or parsed.api_path != record.cloud_source_path:
            return 'strm_target_changed'
        if self._cloud_destination_components(record) is None:
            return 'cloud_destination_changed'
        return None

    def _cloud_destination_components(self, record: _MoveRecord) -> tuple[str, str, str] | None:
        if self._cloud_move_paths is None or record.cloud_destination_dir is None:
            return None
        brand = self._brand_resolver.resolve_brand(record.video_id)
        if not brand or not self._is_safe_segment(brand):
            return None
        expected = self._cloud_move_paths.destination_directory(brand)
        if expected != record.cloud_destination_dir:
            return None
        return str(self._cloud_move_paths.move_in_api_root), brand, expected

    @staticmethod
    def _cloud_result_base(record: _MoveRecord) -> dict[str, str]:
        file_name = record.cloud_file.name if record.cloud_file is not None else record.source.name
        return {
            'candidate_id': record.candidate_id,
            'video_id': record.video_id,
            'file_name': file_name,
        }

    def _terminal_cloud_result(
        self,
        record: _MoveRecord,
        operation: CloudMoveOperationRecord,
    ) -> MoveResult:
        mapping = {
            CloudMoveOperationState.SUCCEEDED: (MoveState.MOVED, None),
            CloudMoveOperationState.CONFLICT: (MoveState.CONFLICT, operation.error_code),
            CloudMoveOperationState.FAILED: (MoveState.FAILED, operation.error_code),
        }
        move_state, error_code = mapping[operation.state]
        return MoveResult(**self._cloud_result_base(record), state=move_state, error_code=error_code)

    async def _run_move(self, plan_id: str, record: _MoveRecord) -> MoveResult:  # noqa: PLR0911
        if record.kind is CandidateKind.CLOUD_STRM:
            return await self._run_cloud_move(plan_id, record)
        if not await self._candidate_roots_ready(record):
            return self._root_unavailable(record)
        context = self._mutation_lock.acquire() if self._mutation_lock is not None else _unlocked()
        try:
            async with context:
                if not await self._candidate_roots_ready(record):
                    return self._root_unavailable(record)
                cached = await self._repository.get_move_result(plan_id, record.candidate_id)
                if cached is not None:
                    return cached
                journal = await self._repository.get_move_journal(plan_id, record.candidate_id)
                if journal is not None and journal.state is not MoveJournalState.RECONCILED:
                    return await self._reconcile_candidate(plan_id, record, journal)
                await self._save_journal(plan_id, record.candidate_id, MoveJournalState.PREPARED)
                result = await self._run_mutation(self._apply_one, record)
                if result.state is not MoveState.MOVED and not await self._candidate_roots_ready(record):
                    return result
                if result.error_code == 'move_rollback_failed' or (
                    result.error_code is not None and result.error_code.startswith('reconcile_quarantine_')
                ):
                    return result
                await self._persist_move_completion(plan_id, record, result, MoveJournalState.PREPARED)
                return result
        except Exception:  # noqa: BLE001
            return self._unexpected_move_failure(record)

    async def _reconcile_candidate(
        self,
        plan_id: str,
        record: _MoveRecord,
        journal: MoveJournalRecord,
    ) -> MoveResult:
        if not await self._candidate_roots_ready(record):
            return self._root_unavailable(record)
        cached = await self._repository.get_move_result(plan_id, record.candidate_id)
        if cached is not None:
            await self._save_journal(plan_id, record.candidate_id, MoveJournalState.RECONCILED)
            return cached

        result = await self._run_mutation(self._reconcile_filesystem, record, journal.state)
        if result.state is not MoveState.MOVED and not await self._candidate_roots_ready(record):
            return result
        if result.error_code in {'reconcile_rollback_conflict', 'reconcile_rollback_failed'} or (
            result.error_code is not None and result.error_code.startswith('reconcile_quarantine_')
        ):
            return result
        await self._persist_move_completion(plan_id, record, result, journal.state)
        return result

    def _reconcile_filesystem(  # noqa: C901, PLR0911, PLR0912
        self,
        record: _MoveRecord,
        state: MoveJournalState,
    ) -> MoveResult:
        base = {
            'candidate_id': record.candidate_id,
            'video_id': record.video_id,
            'file_name': record.source.name,
        }
        if self._has_symlink_component(record.source, record.source_root) or self._has_symlink_component(
            record.destination,
            self._paths.move_in_path,
        ):
            return MoveResult(**base, state=MoveState.INVALID_PATH, error_code='reconcile_symlink')
        if not self._is_within(record.source, record.source_root) or not self._is_within(
            record.destination,
            self._paths.move_in_path,
        ):
            return MoveResult(**base, state=MoveState.INVALID_PATH, error_code='path_outside_root')

        source_matches = self._matches_fingerprint_if_present(record.source, record.fingerprint)
        source_matches_linked = self._matches_linked_fingerprint_if_present(record.source, record.fingerprint)
        destination_matches = self._matches_linked_fingerprint_if_present(record.destination, record.fingerprint)
        source_exists = record.source.exists()
        destination_exists = record.destination.exists()
        quarantine = self._reconcile_quarantine_path(record)

        if quarantine.exists():
            return self._finish_quarantined_reconcile(record, quarantine, base)

        if source_matches_linked and destination_matches and self._same_inode(record.source, record.destination):
            try:
                self._move_to_quarantine(record.source, quarantine)
            except OSError:
                return MoveResult(**base, state=MoveState.FAILED, error_code='reconcile_quarantine_failed')
            return self._finish_quarantined_reconcile(record, quarantine, base)
        if destination_matches and (not source_exists or not source_matches_linked):
            return MoveResult(**base, state=MoveState.MOVED)
        if source_matches and not destination_exists and state is MoveJournalState.PREPARED:
            return self._apply_one(record)
        if state is MoveJournalState.PREPARED and destination_exists and not destination_matches:
            if source_exists:
                return MoveResult(**base, state=MoveState.FAILED, error_code='reconcile_rollback_conflict')
            try:
                _rename_no_replace(record.destination, record.source)
            except FileExistsError:
                return MoveResult(**base, state=MoveState.FAILED, error_code='reconcile_rollback_conflict')
            except OSError:
                return MoveResult(**base, state=MoveState.FAILED, error_code='reconcile_rollback_failed')
            return MoveResult(**base, state=MoveState.STALE, error_code='source_changed_during_move')
        if source_exists and not source_matches_linked:
            return MoveResult(**base, state=MoveState.STALE, error_code='source_changed')
        if destination_exists:
            return MoveResult(**base, state=MoveState.CONFLICT, error_code='reconcile_destination_conflict')
        if state in {MoveJournalState.LINKED, MoveJournalState.SOURCE_REMOVED}:
            return MoveResult(**base, state=MoveState.FAILED, error_code='reconcile_destination_missing')
        return MoveResult(**base, state=MoveState.STALE, error_code='source_missing')

    def _finish_quarantined_reconcile(  # noqa: PLR0911
        self,
        record: _MoveRecord,
        quarantine: Path,
        base: dict[str, str],
    ) -> MoveResult:
        quarantine_matches = self._matches_linked_fingerprint_if_present(quarantine, record.fingerprint)
        destination_matches = self._matches_linked_fingerprint_if_present(record.destination, record.fingerprint)
        if not quarantine_matches:
            if not destination_matches:
                return MoveResult(**base, state=MoveState.FAILED, error_code='reconcile_quarantine_conflict')
            try:
                self._restore_quarantine_no_replace(quarantine, record.source)
            except OSError:
                return MoveResult(**base, state=MoveState.FAILED, error_code='reconcile_quarantine_conflict')
            return MoveResult(**base, state=MoveState.MOVED)
        if quarantine_matches and destination_matches and self._same_inode(quarantine, record.destination):
            try:
                quarantine.unlink()
            except OSError:
                return MoveResult(**base, state=MoveState.FAILED, error_code='reconcile_quarantine_cleanup_failed')
            return MoveResult(**base, state=MoveState.MOVED)
        try:
            self._restore_quarantine_no_replace(quarantine, record.source)
        except OSError:
            return MoveResult(**base, state=MoveState.FAILED, error_code='reconcile_quarantine_conflict')
        return MoveResult(**base, state=MoveState.STALE, error_code='source_changed_during_reconcile')

    @staticmethod
    def _move_to_quarantine(source: Path, quarantine: Path) -> None:
        try:
            _rename_no_replace(source, quarantine)
        except OSError as exc:
            if exc.errno not in _RENAME_NOREPLACE_UNSUPPORTED:
                raise
            if quarantine.exists():
                raise FileExistsError(errno.EEXIST, os.strerror(errno.EEXIST), quarantine) from exc
            source.rename(quarantine)

    @staticmethod
    def _restore_quarantine_no_replace(quarantine: Path, destination: Path) -> None:
        if destination.exists():
            if FillActorService._same_inode(quarantine, destination):
                quarantine.unlink()
                return
            raise FileExistsError(errno.EEXIST, os.strerror(errno.EEXIST), destination)
        try:
            _rename_no_replace(quarantine, destination)
        except OSError as exc:
            if exc.errno not in _RENAME_NOREPLACE_UNSUPPORTED:
                raise
        else:
            return
        os.link(quarantine, destination, follow_symlinks=False)
        if not FillActorService._same_inode(quarantine, destination):
            msg = 'restored quarantine identity mismatch'
            raise OSError(msg)
        quarantine.unlink()

    def _reconcile_quarantine_path(self, record: _MoveRecord) -> Path:
        digest = hashlib.sha256(record.candidate_id.encode()).hexdigest()
        return self._paths.move_in_path / f'.embyx-reconcile-{digest}'

    async def _persist_move_completion(
        self,
        plan_id: str,
        record: _MoveRecord,
        result: MoveResult,
        journal_state: MoveJournalState,
    ) -> None:
        if result.state is MoveState.MOVED:
            if journal_state is MoveJournalState.PREPARED:
                await self._save_journal(plan_id, record.candidate_id, MoveJournalState.LINKED)
                journal_state = MoveJournalState.LINKED
            if journal_state is MoveJournalState.LINKED:
                await self._save_journal(plan_id, record.candidate_id, MoveJournalState.SOURCE_REMOVED)
        await self._repository.save_move_result(plan_id, result)
        await self._save_journal(plan_id, record.candidate_id, MoveJournalState.RECONCILED)

    async def _save_journal(self, plan_id: str, candidate_id: str, state: MoveJournalState) -> None:
        await self._repository.save_move_journal(
            MoveJournalRecord(
                plan_id=plan_id,
                candidate_id=candidate_id,
                state=state,
                updated_at=self._now(),
            )
        )

    @staticmethod
    def _matches_fingerprint_if_present(path: Path, fingerprint: _Fingerprint) -> bool:
        try:
            return not path.is_symlink() and FillActorService._fingerprint(path) == fingerprint
        except OSError:
            return False

    @staticmethod
    def _matches_linked_fingerprint_if_present(path: Path, fingerprint: _Fingerprint) -> bool:
        try:
            return not path.is_symlink() and FillActorService._matches_linked_identity(path, fingerprint)
        except OSError:
            return False

    @staticmethod
    def _same_inode(first: Path, second: Path) -> bool:
        try:
            first_stat = first.stat()
            second_stat = second.stat()
        except OSError:
            return False
        return first_stat.st_dev == second_stat.st_dev and first_stat.st_ino == second_stat.st_ino

    def _discard_in_flight(self, task_key: tuple[str, str], _worker: asyncio.Task[MoveResult]) -> None:
        self._in_flight.pop(task_key, None)

    async def _run_mutation(
        self,
        function: Callable[..., MoveResult],
        *args: object,
    ) -> MoveResult:
        worker = _MUTATION_EXECUTOR.submit(function, *args)
        self._mutation_futures.add(worker)
        try:
            return await _await_mutation_future(worker)
        finally:
            self._mutation_futures.discard(worker)

    async def _run_scan[T](self, function: Callable[..., T], *args: object) -> T:
        stop_requested = threading.Event()
        worker = _SCAN_EXECUTOR.submit(function, *args, stop_requested)
        self._scan_futures[worker] = stop_requested
        try:
            return await _await_executor_future(
                worker,
                cancellation_wins=True,
                on_cancel=stop_requested.set,
            )
        finally:
            self._scan_futures.pop(worker, None)

    async def _candidate_roots_ready(self, record: _MoveRecord) -> bool:
        def check() -> bool:
            try:
                source_root_stat = record.source_root.stat()
                destination_root_stat = self._paths.move_in_path.stat()
            except OSError:
                return False
            return (
                record.source_root.is_dir()
                and not record.source_root.is_symlink()
                and self._paths.move_in_path.is_dir()
                and not self._paths.move_in_path.is_symlink()
                and (
                    self._root_sentinel is None
                    or (
                        (record.source_root / self._root_sentinel).is_file()
                        and not (record.source_root / self._root_sentinel).is_symlink()
                        and (self._paths.move_in_path / self._root_sentinel).is_file()
                        and not (self._paths.move_in_path / self._root_sentinel).is_symlink()
                    )
                )
                and source_root_stat.st_dev == record.fingerprint.device
                and destination_root_stat.st_dev == record.fingerprint.device
            )

        return await asyncio.to_thread(check)

    @staticmethod
    def _unexpected_move_failure(record: _MoveRecord) -> MoveResult:
        return MoveResult(
            candidate_id=record.candidate_id,
            video_id=record.video_id,
            file_name=record.source.name,
            state=MoveState.FAILED,
            error_code='move_failed',
        )

    @staticmethod
    def _root_unavailable(record: _MoveRecord) -> MoveResult:
        return MoveResult(
            candidate_id=record.candidate_id,
            video_id=record.video_id,
            file_name=record.source.name,
            state=MoveState.FAILED,
            error_code='roots_unavailable',
        )
