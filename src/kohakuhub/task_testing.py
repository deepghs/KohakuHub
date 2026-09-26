"""Test helpers for background task handlers.

``run_with_interruptions`` checks the handler contract (see
docs/development/background-tasks.md): a handler interrupted at any point and
then run again must end in the same state as one uninterrupted run.
"""

import inspect
import json
from types import SimpleNamespace
from typing import Any, Awaitable, Callable

from kohakuhub.tasks import TaskContext, TaskHandler


class SimulatedCrash(BaseException):
    """A BaseException, so a handler's ``except Exception`` cannot swallow it,
    just as it cannot swallow a real SIGKILL."""


class RecordingContext(TaskContext):
    """An in-memory ``TaskContext`` whose calls are interruption points.

    Every ``progress``, ``stage`` and ``checkpoint`` call is two points: just
    before and just after its effect. ``crash_at=n`` raises
    ``SimulatedCrash`` at the n-th point.
    """

    def __init__(
        self,
        task_id: int = 1,
        *,
        kind: str = "test.task",
        attempt: int = 1,
        checkpoint_state: Any = None,
        crash_at: int | None = None,
    ):
        row = SimpleNamespace(
            id=task_id,
            kind=kind,
            attempts=attempt,
            checkpoint=None if checkpoint_state is None else json.dumps(checkpoint_state),
        )
        super().__init__(row, log_limit_bytes=10 * 1024 * 1024)
        self.crash_at = crash_at
        self.points = 0
        self.reports: list[tuple[int, int | None]] = []
        self.stages: list[str] = []

    def _point(self) -> None:
        self.points += 1
        if self.points == self.crash_at:
            raise SimulatedCrash(f"simulated crash at interruption point {self.points}")

    def progress(self, done: int, total: int | None = None) -> None:
        self._point()
        super().progress(done, total)
        self.reports.append((done, total))
        self._point()

    def stage(self, name: str) -> None:
        self._point()
        self.stages.append(name)
        self._point()

    def checkpoint(self, state: Any) -> None:
        self._point()
        # Round-trip like the real row does, so non-JSON state fails here.
        self.checkpoint_state = json.loads(json.dumps(state))
        self._point()

    def assert_owned(self) -> None:
        return None


async def run_with_interruptions(
    handler: TaskHandler,
    payload: dict[str, Any],
    *,
    reset: Callable[[], Awaitable[None] | None],
    snapshot: Callable[[], Awaitable[Any] | Any],
    task_id: int = 1,
) -> int:
    """Assert that ``handler`` converges however it is interrupted.

    ``reset`` restores the initial state and ``snapshot`` returns the final
    state to compare, including anything left under the task's
    ``scratch_prefix``. The handler runs once uninterrupted; then, for every
    interruption point, it runs from a fresh reset, crashes at that point,
    and runs again with the checkpoint the crashed run left behind. Every
    such pair must end in the uninterrupted run's state.

    Returns the number of interruption points exercised.
    """
    if len(inspect.signature(handler).parameters) < 2:
        raise TypeError("run_with_interruptions needs a handler(payload, ctx)")

    async def call(function):
        result = function()
        return await result if inspect.isawaitable(result) else result

    await call(reset)
    clean = RecordingContext(task_id)
    await handler(payload, clean)
    expected = await call(snapshot)
    if clean.points == 0:
        raise AssertionError(
            "The handler has no interruption points: report progress, stages or checkpoints"
        )

    for point in range(1, clean.points + 1):
        await call(reset)
        crashed = RecordingContext(task_id, crash_at=point)
        try:
            await handler(payload, crashed)
        except SimulatedCrash:
            pass
        else:
            raise AssertionError(
                f"Interruption point {point} was not reached again: the handler is not "
                "deterministic for the same starting state"
            )
        resumed = RecordingContext(task_id, attempt=2, checkpoint_state=crashed.checkpoint_state)
        await handler(payload, resumed)
        actual = await call(snapshot)
        if actual != expected:
            raise AssertionError(
                f"Interrupted at point {point} of {clean.points} and run again, the handler "
                f"ended in {actual!r} instead of {expected!r}"
            )
    return clean.points
