import inspect
import sys
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from importlib.machinery import PathFinder
from importlib.util import module_from_spec
from pathlib import Path
from types import ModuleType
from typing import cast

ActorCallable = Callable[[str], Awaitable[Iterable[str]]]
MagnetCallable = Callable[[str], Awaitable[str | None]]
BrandCallable = Callable[[str], str | None]
CloseCallable = Callable[[], Awaitable[None]]


@dataclass(frozen=True)
class CallableActorCatalog:
    function: ActorCallable

    async def list_video_ids(self, actor_id: str) -> Iterable[str]:
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
class RuntimeAdapters:
    actor_catalog: CallableActorCatalog
    magnet_provider: CallableMagnetProvider
    brand_resolver: CallableBrandResolver
    close_function: CloseCallable

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
    return RuntimeAdapters(
        actor_catalog=CallableActorCatalog(cast('ActorCallable', list_video_ids)),
        magnet_provider=CallableMagnetProvider(cast('MagnetCallable', find_magnet)),
        brand_resolver=CallableBrandResolver(cast('BrandCallable', resolve_brand)),
        close_function=cast('CloseCallable', close),
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
