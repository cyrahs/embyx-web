import inspect
import sys
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from importlib.machinery import PathFinder
from importlib.util import module_from_spec
from pathlib import Path
from types import ModuleType
from typing import cast

from embyx_web.fill_actor.cloud_moves import CloudFileMetadata, CloudFileMover, CloudMoveResponse
from embyx_web.fill_actor.ports import PageProgressCallback

ActorCallable = Callable[..., Awaitable[Iterable[str]]]
MagnetCallable = Callable[[str], Awaitable[str | None]]
BrandCallable = Callable[[str], str | None]
CloseCallable = Callable[[], Awaitable[None]]
ListCloudDirectoryCallable = Callable[[str], Awaitable[Iterable[Mapping[str, object]]]]
MoveCloudFileCallable = Callable[[str, str], Awaitable[Mapping[str, object]]]
EnsureCloudDirectoryCallable = Callable[[str, str], Awaitable[Mapping[str, object]]]
NANOSECONDS_PER_SECOND = 1_000_000_000


@dataclass(frozen=True)
class CallableActorCatalog:
    function: ActorCallable
    supports_progress: bool = False

    async def list_video_ids(
        self,
        actor_id: str,
        *,
        progress_callback: PageProgressCallback | None = None,
    ) -> Iterable[str]:
        if progress_callback is not None and self.supports_progress:
            return await self.function(actor_id, progress_callback=progress_callback)
        return await self.function(actor_id)


@dataclass(frozen=True)
class CallableMagnetProvider:
    function: MagnetCallable

    async def find_magnet(self, video_id: str) -> str | None:
        return await self.function(video_id)


@dataclass(frozen=True)
class CallableBrandResolver:
    function: BrandCallable

    def resolve_brand(self, video_id: str) -> str | None:
        return self.function(video_id)


@dataclass(frozen=True)
class CallableCloudFileMover(CloudFileMover):
    list_function: ListCloudDirectoryCallable
    ensure_function: EnsureCloudDirectoryCallable
    move_function: MoveCloudFileCallable

    async def list_directory(self, api_directory: str) -> tuple[CloudFileMetadata, ...]:
        values = await self.list_function(api_directory)
        files: list[CloudFileMetadata] = []
        for value in values:
            is_directory = value.get('is_directory')
            if not isinstance(is_directory, bool):
                msg = 'embyx runtime returned invalid CloudDrive file type'
                raise TypeError(msg)
            if is_directory:
                continue
            full_path = value.get('full_path')
            write_time = value.get('write_time')
            if not isinstance(write_time, Mapping):
                msg = 'embyx runtime returned invalid CloudDrive write time'
                raise TypeError(msg)
            seconds = write_time.get('seconds')
            nanos = write_time.get('nanos')
            if (
                not isinstance(seconds, int)
                or isinstance(seconds, bool)
                or not isinstance(nanos, int)
                or isinstance(nanos, bool)
                or nanos < 0
                or nanos >= NANOSECONDS_PER_SECOND
            ):
                msg = 'embyx runtime returned invalid CloudDrive write time'
                raise TypeError(msg)
            files.append(
                CloudFileMetadata.from_mapping(
                    {
                        'path': full_path,
                        'id': value.get('id'),
                        'name': value.get('name'),
                        'size': value.get('size'),
                        'write_time': seconds * NANOSECONDS_PER_SECOND + nanos,
                        'hashes': value.get('hashes', {}),
                    }
                )
            )
        return tuple(files)

    async def ensure_directory(self, parent_api_directory: str, folder_name: str) -> bool:
        value = await self.ensure_function(parent_api_directory, folder_name)
        success = value.get('success')
        if not isinstance(success, bool):
            msg = 'embyx runtime returned invalid CloudDrive directory result'
            raise TypeError(msg)
        return success

    async def move_file(self, source_api_path: str, destination_api_directory: str) -> CloudMoveResponse:
        value = await self.move_function(source_api_path, destination_api_directory)
        return CloudMoveResponse.from_mapping(
            {
                'success': value.get('success'),
                'error_message': value.get('error_message'),
                'result_paths': value.get('result_file_paths', ()),
            }
        )


@dataclass(frozen=True)
class RuntimeAdapters:
    actor_catalog: CallableActorCatalog
    magnet_provider: CallableMagnetProvider
    brand_resolver: CallableBrandResolver
    close_function: CloseCallable
    cloud_file_mover: CallableCloudFileMover | None = None

    async def aclose(self) -> None:
        await self.close_function()


def load_runtime_adapters(*, runtime_root: Path, module_name: str) -> RuntimeAdapters:
    """Load the narrow embyx compatibility API without importing legacy workflow globals here."""
    root = runtime_root.expanduser().resolve(strict=True)
    if not root.is_dir():
        msg = 'embyx runtime root must be a directory'
        raise RuntimeError(msg)
    module = _load_module_from_root(module_name, root)
    list_video_ids = _require_async_callable(module, 'list_actor_video_ids')
    find_magnet = _require_async_callable(module, 'find_sukebei_magnet')
    resolve_brand = _require_callable(module, 'resolve_brand')
    close = _require_async_callable(module, 'aclose')
    cloud_function_names = ('list_cloud_directory', 'ensure_cloud_directory', 'move_cloud_file')
    cloud_functions = tuple(getattr(module, name, None) for name in cloud_function_names)
    if any(function is not None for function in cloud_functions):
        if not all(callable(function) and inspect.iscoroutinefunction(function) for function in cloud_functions):
            msg = 'embyx runtime CloudDrive compatibility callables must all be async'
            raise TypeError(msg)
        cloud_file_mover = CallableCloudFileMover(
            cast('ListCloudDirectoryCallable', cloud_functions[0]),
            cast('EnsureCloudDirectoryCallable', cloud_functions[1]),
            cast('MoveCloudFileCallable', cloud_functions[2]),
        )
    else:
        cloud_file_mover = None
    return RuntimeAdapters(
        actor_catalog=CallableActorCatalog(
            cast('ActorCallable', list_video_ids),
            supports_progress=_accepts_keyword(list_video_ids, 'progress_callback'),
        ),
        magnet_provider=CallableMagnetProvider(cast('MagnetCallable', find_magnet)),
        brand_resolver=CallableBrandResolver(cast('BrandCallable', resolve_brand)),
        close_function=cast('CloseCallable', close),
        cloud_file_mover=cloud_file_mover,
    )


def _load_module_from_root(module_name: str, root: Path) -> ModuleType:
    search_path = [str(root)]
    module: ModuleType | None = None
    parts = module_name.split('.')
    if not parts or any(not part.isidentifier() for part in parts):
        msg = 'embyx runtime compatibility module name is invalid'
        raise ValueError(msg)

    for index in range(len(parts)):
        qualified_name = '.'.join(parts[: index + 1])
        existing = sys.modules.get(qualified_name)
        if existing is not None:
            _verify_module_origin(existing, root)
            module = existing
        else:
            spec = PathFinder.find_spec(qualified_name, search_path)
            if spec is None or spec.loader is None:
                msg = f'embyx runtime compatibility module {qualified_name} was not found under the configured root'
                raise ModuleNotFoundError(msg)
            origin = spec.origin
            if origin is None or not _path_is_within(Path(origin), root):
                msg = 'embyx runtime compatibility module resolved outside the configured runtime root'
                raise RuntimeError(msg)
            module = module_from_spec(spec)
            sys.modules[qualified_name] = module
            try:
                spec.loader.exec_module(module)
            except BaseException:
                sys.modules.pop(qualified_name, None)
                raise
        if index < len(parts) - 1:
            package_path = getattr(module, '__path__', None)
            if package_path is None:
                msg = f'embyx runtime compatibility parent {qualified_name} is not a package'
                raise TypeError(msg)
            search_path = list(package_path)
    if module is None:
        msg = 'embyx runtime compatibility module name is empty'
        raise ValueError(msg)
    return module


def _verify_module_origin(module: ModuleType, root: Path) -> None:
    module_file = getattr(module, '__file__', None)
    if module_file is None:
        msg = 'embyx runtime compatibility module must be file-backed'
        raise RuntimeError(msg)
    if not _path_is_within(Path(module_file), root):
        msg = 'embyx runtime compatibility module was loaded outside the configured runtime root'
        raise RuntimeError(msg)


def _path_is_within(path: Path, root: Path) -> bool:
    try:
        path.resolve(strict=True).relative_to(root)
    except (FileNotFoundError, ValueError):
        return False
    return True


def _require_callable(module: ModuleType, name: str) -> Callable[..., object]:
    function = getattr(module, name, None)
    if not callable(function):
        msg = f'embyx runtime compatibility module is missing callable {name}'
        raise TypeError(msg)
    return function


def _require_async_callable(module: ModuleType, name: str) -> Callable[..., object]:
    function = _require_callable(module, name)
    if not inspect.iscoroutinefunction(function):
        msg = f'embyx runtime compatibility callable {name} must be async'
        raise TypeError(msg)
    return function


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
