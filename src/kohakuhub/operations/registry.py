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
    cancel_while_running: bool = True
    # External handlers must cross a durable dispatch boundary before running.
    # Replay is opt-in for handlers whose individual effects are idempotent.
    external_side_effect: bool = False
    replay_safe_after_dispatch: bool = False
    fence_scope: str | None = None
    # Bulk and cleanup lanes can opt into one global delivery lock while
    # retaining their domain-level repository/resource fences.
    lock_scope: str = "resource"

    @property
    def step_name(self) -> str:
        return self.kind

    def delivery_lock(self, resource_key: str) -> str | None:
        if not self.lock_prefix:
            return None
        if self.lock_scope == "global":
            return self.lock_prefix
        return f"{self.lock_prefix}:{resource_key}"


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
        if spec.lock_scope not in {"resource", "global"}:
            raise ValueError("unsupported operation lock scope")
        if spec.lock_scope == "global" and not spec.lock_prefix:
            raise ValueError("global operation locks require a lock prefix")
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


async def _observation_handler(
    _operation: OperationRecord, _step: StepRecord
) -> StepResult:
    """Keep an unresolved external commit visible until reconciliation settles it.

    The control reconciliation task owns remote observation.  This handler is
    intentionally non-mutating; running it can never resend the LakeFS commit.
    """

    return StepResult(
        state="uncertain",
        progress_message="waiting for commit reconciliation",
        error_code="commit_observing",
        error_summary="the external commit result is still being observed",
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
            cancel_while_running=False,
            external_side_effect=True,
            replay_safe_after_dispatch=True,
        ),
        HandlerSpec(
            kind="commit.observe.v1",
            version="1",
            task_name="khub:operation:execute.v1",
            queue="control-v1",
            priority=115,
            handler=_observation_handler,
            lock_prefix="commit-observe",
        ),
    )
)
