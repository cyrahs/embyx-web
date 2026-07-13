from pathlib import Path

from fastapi import FastAPI

from embyx_web.api import create_app
from embyx_web.fill_actor.jobs import FillActorJobManager
from embyx_web.fill_actor.service import FillActorPaths, FillActorService
from embyx_web.fill_actor.sqlite_repository import SQLiteFillActorRepository
from embyx_web.locking import AsyncFileLock
from embyx_web.runtime_adapters import load_runtime_adapters
from embyx_web.settings import Settings


def build_app(settings: Settings) -> FastAPI:
    settings.validate_exposure()
    actor_root, additional_roots, move_in_root = settings.require_fill_actor_paths()
    if settings.embyx_runtime_path is None:
        msg = 'EMBYX_WEB_RUNTIME_ROOT must be configured'
        raise ValueError(msg)
    runtime = load_runtime_adapters(
        runtime_root=settings.embyx_runtime_path,
        module_name=settings.embyx_runtime_module,
    )
    repository = SQLiteFillActorRepository(settings.database_path)
    service = FillActorService(
        paths=FillActorPaths.from_iterable(
            actor_brand_path=actor_root,
            additional_brand_paths=additional_roots,
            move_in_path=move_in_root,
        ),
        actor_catalog=runtime.actor_catalog,
        magnet_provider=runtime.magnet_provider,
        brand_resolver=runtime.brand_resolver,
        max_actors=settings.max_actors,
        max_videos=settings.max_videos,
        magnet_concurrency=settings.magnet_concurrency,
        root_sentinel=settings.root_sentinel,
        move_in_by_brand=settings.move_in_by_brand,
        repository=repository,
        mutation_lock=AsyncFileLock(settings.mutation_lock_path),
    )
    jobs = FillActorJobManager(service=service, repository=repository)
    frontend_dist = Path(__file__).resolve().parent / 'static'
    return create_app(
        service=service,
        repository=repository,
        jobs=jobs,
        api_token=settings.api_token,
        max_request_bytes=settings.max_request_bytes,
        runtime_close=runtime.aclose,
        frontend_dist=frontend_dist,
    )
