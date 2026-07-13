from datetime import datetime
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True)


class VideoState(StrEnum):
    EXISTS = 'exists'
    ADDITIONAL_FOUND = 'additional_found'
    MAGNET_FOUND = 'magnet_found'
    MISSING = 'missing'
    INVALID_VIDEO_ID = 'invalid_video_id'
    SCAN_FAILED = 'scan_failed'


class MoveState(StrEnum):
    MOVED = 'moved'
    STALE = 'stale'
    CONFLICT = 'conflict'
    INVALID_PATH = 'invalid_path'
    FAILED = 'failed'


class ApplyState(StrEnum):
    SUCCEEDED = 'succeeded'
    PARTIAL_FAILED = 'partial_failed'
    FAILED = 'failed'


class ActorPlan(FrozenModel):
    actor_id: str
    scraped_count: int = Field(ge=0)
    video_ids: tuple[str, ...] = ()
    error_code: str | None = None


class MoveCandidate(FrozenModel):
    candidate_id: str
    video_id: str
    file_name: str
    source_label: str
    destination_conflict: bool


class VideoPlan(FrozenModel):
    video_id: str
    actor_ids: tuple[str, ...]
    state: VideoState
    existing_files: tuple[str, ...] = ()
    move_candidates: tuple[MoveCandidate, ...] = ()
    magnet: str | None = None
    warnings: tuple[str, ...] = ()


class FillActorPlan(FrozenModel):
    plan_id: str
    revision: str
    created_at: datetime
    expires_at: datetime
    actors: tuple[ActorPlan, ...]
    videos: tuple[VideoPlan, ...]


class MoveResult(FrozenModel):
    candidate_id: str
    video_id: str
    file_name: str
    state: MoveState
    error_code: str | None = None


class ApplyResult(FrozenModel):
    plan_id: str
    revision: str
    state: ApplyState
    results: tuple[MoveResult, ...]
