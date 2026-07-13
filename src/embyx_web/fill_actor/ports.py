from collections.abc import Iterable
from typing import Protocol


class ActorCatalog(Protocol):
    """Source that lists video identifiers associated with an actor."""

    async def list_video_ids(self, actor_id: str) -> Iterable[str]: ...


class MagnetProvider(Protocol):
    """Source that resolves a preferred magnet for a video identifier."""

    async def find_magnet(self, video_id: str) -> str | None: ...


class BrandResolver(Protocol):
    """Resolver for the library brand directory of a video identifier."""

    def resolve_brand(self, video_id: str) -> str | None: ...
