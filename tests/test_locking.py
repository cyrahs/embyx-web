import asyncio
from pathlib import Path

import pytest

from embyx_web.locking import AsyncFileLock


@pytest.mark.asyncio
async def test_file_lock_serializes_independent_instances(tmp_path: Path) -> None:
    first = AsyncFileLock(tmp_path / 'mutation.lock')
    second = AsyncFileLock(tmp_path / 'mutation.lock')
    acquired = asyncio.Event()

    async def contender() -> None:
        async with second.acquire():
            acquired.set()

    async with first.acquire():
        task = asyncio.create_task(contender())
        await asyncio.sleep(0.02)
        assert not acquired.is_set()
    await asyncio.wait_for(task, timeout=1)
    assert acquired.is_set()


@pytest.mark.asyncio
async def test_cancelled_file_lock_waiter_stops_before_holder_releases(tmp_path: Path) -> None:
    first = AsyncFileLock(tmp_path / 'mutation.lock', retry_interval=0.005)
    second = AsyncFileLock(tmp_path / 'mutation.lock', retry_interval=0.005)

    async def contend() -> None:
        async with second.acquire():
            pytest.fail('cancelled waiter acquired the lock')

    async with first.acquire():
        waiter = asyncio.create_task(contend())
        await asyncio.sleep(0.02)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(waiter, timeout=0.2)

    async with asyncio.timeout(0.2), second.acquire():
        pass
