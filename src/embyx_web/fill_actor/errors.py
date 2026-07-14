class FillActorError(Exception):
    """Base class for errors that API adapters can map to stable responses."""

    code = 'fill_actor_error'


class InvalidActorIdError(FillActorError):
    code = 'invalid_actor_id'


class TooManyActorsError(FillActorError):
    code = 'too_many_actors'


class TooManyVideosError(FillActorError):
    code = 'too_many_videos'


class JobQueueFullError(FillActorError):
    code = 'job_queue_full'


class ApplyRequestConflictError(FillActorError):
    code = 'apply_request_conflict'


class ApplyJobNotCancellableError(FillActorError):
    code = 'apply_job_not_cancellable'


class UnknownApplyJobError(FillActorError):
    code = 'unknown_apply_job'


class MoveDisabledError(FillActorError):
    code = 'move_disabled'


class LegacyPlanError(FillActorError):
    code = 'legacy_plan_requires_rescan'


class UnknownPlanError(FillActorError):
    code = 'unknown_plan'


class ExpiredPlanError(FillActorError):
    code = 'expired_plan'


class RevisionMismatchError(FillActorError):
    code = 'revision_mismatch'


class UnknownCandidateError(FillActorError):
    code = 'unknown_candidate'
