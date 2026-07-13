import asyncio
import fcntl
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import BinaryIO


class AsyncFileLock:
    """Cancellation-responsive advisory lock shared by all application processes."""

    def __init__(self, path: Path, *, retry_interval: float = 0.05) -> None:
        if retry_interval <= 0:
            msg = 'lock retry interval must be positive'
            raise ValueError(msg)
        self._path = path
        self._retry_interval = retry_interval

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator[None]:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        handle = self._path.open('a+b')
        acquired = False
        try:
            while True:
                try:
                    self._try_lock(handle)
                except BlockingIOError:
                    await asyncio.sleep(self._retry_interval)
                else:
                    acquired = True
                    break
            yield
        finally:
            try:
                if acquired:
                    self._unlock(handle)
            finally:
                handle.close()

    @staticmethod
    def _try_lock(handle: BinaryIO) -> None:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)

    @staticmethod
    def _unlock(handle: BinaryIO) -> None:
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
