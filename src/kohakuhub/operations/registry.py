"""Code-owned operation and handler registry."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from .types import OperationRecord, StepRecord, StepResult
from .handlers import commit_postprocess_handler


Handler = Callable[[OperationRecord, StepRecord], Awaitable[StepResult]]


@dataclass(frozen=True)
class HandlerSpec:
    """Server-owned execution policy for one public operation kind."""

    kind: str
    version: str
    task_name: str
    queue: str
    priority: int
    handler: Handler
    lock_prefix: str | None = None

    @property
    def step_name(self) -> str:
        return self.kind


class OperationRegistry:
    """Immutable-after-start registry of handlers known by this build."""

    def __init__(self, specs: tuple[HandlerSpec, ...] = ()) -> None:
        self._specs: dict[str, HandlerSpec] = {}
        for spec in specs:
            self.register(spec)

    def register(self, spec: HandlerSpec) -> None:
        if spec.kind in self._specs:
            raise ValueError(f"duplicate operation handler: {spec.kind}")
        if not spec.kind or not spec.version or not spec.task_name:
            raise ValueError("operation handler identity is required")
        self._specs[spec.kind] = spec

    def get(self, kind: str) -> HandlerSpec:
        try:
            return self._specs[kind]
        except KeyError as exc:
            raise ValueError(f"unknown operation kind: {kind}") from exc

    def kinds(self) -> tuple[str, ...]:
        return tuple(sorted(self._specs))

    def resolve_step(
        self, operation_kind: str, step_name: str, step_version: str
    ) -> HandlerSpec:
        spec = self.get(operation_kind)
        if spec.step_name != step_name or spec.version != step_version:
            raise ValueError(
                f"unsupported step {step_name}@{step_version} for {operation_kind}"
            )
        return spec


async def _noop_handler(_operation: OperationRecord, _step: StepRecord) -> StepResult:
    """A harmless handler used to prove delivery and state transitions."""

    return StepResult.succeeded(progress_current=1, progress_total=1)


async def _chain_handler(_operation: OperationRecord, step: StepRecord) -> StepResult:
    """Two-quantum handler used by integration tests for successor delivery."""

    if step.input_json.get("continued"):
        return StepResult.succeeded(progress_current=2, progress_total=2)
    return StepResult.continue_with(
        input_json={"continued": True},
        progress_current=1,
        progress_total=2,
    )


DEFAULT_REGISTRY = OperationRegistry(
    (
        HandlerSpec(
            kind="maintenance.noop.v1",
            version="1",
            task_name="khub:operation:execute.v1",
            queue="control-v1",
            priority=100,
            handler=_noop_handler,
        ),
        HandlerSpec(
            kind="maintenance.chain.v1",
            version="1",
            task_name="khub:operation:execute.v1",
            queue="control-v1",
            priority=90,
            handler=_chain_handler,
        ),
        HandlerSpec(
            kind="commit.postprocess.v1",
            version="1",
            task_name="khub:operation:execute.v1",
            queue="sync-v1",
            priority=10,
            handler=commit_postprocess_handler,
            lock_prefix="commit-postprocess",
        ),
    )
)
