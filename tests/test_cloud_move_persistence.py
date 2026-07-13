from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from embyx_web.fill_actor.cloud_moves import CloudFileMetadata
from embyx_web.fill_actor.models import FillActorPlan, MoveCandidate, MoveResult, MoveState, VideoPlan, VideoState
from embyx_web.fill_actor.persistence import (
    CandidateKind,
    CandidateRecord,
    CloudMoveOperationRecord,
    CloudMoveOperationState,
    FileFingerprint,
    PlanRecord,
)
from embyx_web.fill_actor.sqlite_repository import SQLiteFillActorRepository


def plan_record(
    plan_id: str,
    candidate_id: str,
    *,
    source_path: str = '/cloud/library/source-b/ABC/ABC-001.mp4',
) -> PlanRecord:
    now = datetime.now(UTC)
    public_candidate = MoveCandidate(
        candidate_id=candidate_id,
        video_id='ABC-001',
        file_name='ABC-001.mp4',
        source_label='additional-1',
        destination_conflict=False,
    )
    return PlanRecord(
        public=FillActorPlan(
            plan_id=plan_id,
            revision=f'revision-{plan_id}',
            created_at=now,
            expires_at=now + timedelta(hours=1),
            actors=(),
            videos=(
                VideoPlan(
                    video_id='ABC-001',
                    actor_ids=('actor',),
                    state=VideoState.ADDITIONAL_FOUND,
                    move_candidates=(public_candidate,),
                ),
            ),
        ),
        candidates=(
            CandidateRecord(
                candidate_id=candidate_id,
                video_id='ABC-001',
                source=Path(f'/mapping/{plan_id}/ABC-001.strm'),
                source_root=Path(f'/mapping/{plan_id}'),
                destination=Path('/mapping/actor/ABC/ABC-001.strm'),
                fingerprint=FileFingerprint(device=1, inode=2, size=3, mtime_ns=4, ctime_ns=5),
                kind=CandidateKind.CLOUD_STRM,
                mapping_sha256='a' * 64,
                cloud_source_path=source_path,
                cloud_destination_dir='/cloud/library/destination/ABC',
                cloud_file=CloudFileMetadata(
                    path=source_path,
                    file_id=f'file-{plan_id}',
                    name='ABC-001.mp4',
                    size=123,
                    write_time=456,
                    hashes=(('sha1', 'abcd'),),
                ),
            ),
        ),
    )


def operation(
    record: PlanRecord,
    state: CloudMoveOperationState,
    *,
    error_code: str | None = None,
) -> CloudMoveOperationRecord:
    candidate = record.candidates[0]
    assert candidate.cloud_source_path is not None
    assert candidate.cloud_destination_dir is not None
    return CloudMoveOperationRecord(
        plan_id=record.public.plan_id,
        candidate_id=candidate.candidate_id,
        attempt_id=f'attempt-{record.public.plan_id}',
        source_path=candidate.cloud_source_path,
        destination_dir=candidate.cloud_destination_dir,
        state=state,
        updated_at=datetime.now(UTC),
        error_code=error_code,
    )


def terminal_result(record: PlanRecord, state: MoveState, error_code: str | None = None) -> MoveResult:
    candidate = record.candidates[0]
    return MoveResult(
        candidate_id=candidate.candidate_id,
        video_id=candidate.video_id,
        file_name='ABC-001.mp4',
        state=state,
        error_code=error_code,
    )


@pytest.mark.asyncio
async def test_sqlite_round_trips_cloud_candidate_and_operation_state(tmp_path: Path) -> None:
    repository = SQLiteFillActorRepository(tmp_path / 'state.sqlite3')
    record = plan_record('plan-1', 'candidate-1')
    await repository.save_plan(record)

    saved = await repository.get_candidate('plan-1', 'candidate-1')

    assert saved == record.candidates[0]
    prepared = operation(record, CloudMoveOperationState.PREPARED)
    await repository.save_cloud_move_operation(prepared)
    await repository.save_cloud_move_operation(operation(record, CloudMoveOperationState.SUBMITTING))
    await repository.save_cloud_move_operation(
        operation(record, CloudMoveOperationState.UNKNOWN, error_code='cloud_move_interrupted')
    )
    unresolved = await repository.list_unresolved_cloud_moves()
    assert len(unresolved) == 1
    assert unresolved[0].state is CloudMoveOperationState.UNKNOWN
    assert await repository.delete_plan('plan-1') is False

    await repository.finalize_cloud_move(
        operation(record, CloudMoveOperationState.SUCCEEDED),
        terminal_result(record, MoveState.MOVED),
    )
    assert await repository.list_unresolved_cloud_moves() == ()
    assert await repository.get_move_result('plan-1', 'candidate-1') == terminal_result(record, MoveState.MOVED)
    assert await repository.delete_plan('plan-1') is True


@pytest.mark.asyncio
async def test_sqlite_terminal_operation_and_result_finalize_atomically(tmp_path: Path) -> None:
    repository = SQLiteFillActorRepository(tmp_path / 'state.sqlite3')
    record = plan_record('plan-1', 'candidate-1')
    await repository.save_plan(record)
    await repository.save_cloud_move_operation(operation(record, CloudMoveOperationState.PREPARED))
    await repository.save_cloud_move_operation(operation(record, CloudMoveOperationState.SUBMITTING))
    unknown = operation(record, CloudMoveOperationState.UNKNOWN, error_code='cloud_move_interrupted')
    await repository.save_cloud_move_operation(unknown)
    succeeded = operation(record, CloudMoveOperationState.SUCCEEDED)

    with pytest.raises(ValueError, match='finalized with their result'):
        await repository.save_cloud_move_operation(succeeded)
    with pytest.raises(ValueError, match='video id does not match candidate'):
        await repository.finalize_cloud_move(
            succeeded,
            terminal_result(record, MoveState.MOVED).model_copy(update={'video_id': 'OTHER-001'}),
        )

    assert await repository.get_cloud_move_operation('plan-1', 'candidate-1') == unknown
    assert await repository.get_move_result('plan-1', 'candidate-1') is None


@pytest.mark.asyncio
async def test_sqlite_prevents_two_unresolved_operations_for_one_cloud_source(tmp_path: Path) -> None:
    repository = SQLiteFillActorRepository(tmp_path / 'state.sqlite3')
    first = plan_record('plan-1', 'candidate-1')
    second = plan_record('plan-2', 'candidate-2')
    await repository.save_plan(first)
    await repository.save_plan(second)
    await repository.save_cloud_move_operation(operation(first, CloudMoveOperationState.PREPARED))

    with pytest.raises(ValueError, match='already has an unresolved operation'):
        await repository.save_cloud_move_operation(operation(second, CloudMoveOperationState.PREPARED))

    await repository.finalize_cloud_move(
        operation(first, CloudMoveOperationState.FAILED, error_code='cloud_move_failed'),
        terminal_result(first, MoveState.FAILED, 'cloud_move_failed'),
    )
    await repository.save_cloud_move_operation(operation(second, CloudMoveOperationState.PREPARED))
