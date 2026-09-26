"""Tests for the run_with_interruptions handler-contract helper."""

import pytest

from kohakuhub.task_testing import RecordingContext, SimulatedCrash, run_with_interruptions


class Store:
    """A tiny stand-in for durable state: items moved from ``source`` to ``target``."""

    def __init__(self):
        self.reset()

    def reset(self):
        self.source = [1, 2, 3, 4]
        self.target = []
        self.scratch = []

    def snapshot(self):
        return {"target": list(self.target), "scratch": list(self.scratch)}


async def test_a_checkpointed_handler_converges_after_every_interruption():
    store = Store()

    async def move(payload, ctx):
        start = (ctx.checkpoint_state or {}).get("next", 0)
        ctx.stage("moving")
        for index in range(start, len(store.source)):
            item = store.source[index]
            if item not in store.target:  # re-runnable: skip what already landed
                store.target.append(item)
            ctx.checkpoint({"next": index + 1})
            ctx.progress(index + 1, len(store.source))

    points = await run_with_interruptions(move, {}, reset=store.reset, snapshot=store.snapshot)

    assert points == 2 + 4 * 4  # a stage, then a checkpoint and a report per item


async def test_a_handler_that_repeats_work_on_rerun_is_caught():
    store = Store()

    async def append_blindly(payload, ctx):
        for index, item in enumerate(store.source):
            store.target.append(item)  # not re-runnable: a rerun appends again
            ctx.progress(index + 1, len(store.source))

    with pytest.raises(AssertionError, match="instead of"):
        await run_with_interruptions(append_blindly, {}, reset=store.reset, snapshot=store.snapshot)


async def test_a_handler_that_leaves_scratch_behind_is_caught():
    store = Store()

    async def leaky(payload, ctx):
        # Per-attempt names break the contract: the next attempt cannot find
        # what a crashed one left behind, so it only removes its own part.
        part = f"{ctx.scratch_prefix}attempt-{ctx.attempt}"
        store.scratch.append(part)
        ctx.progress(1, 2)
        store.target = list(store.source)
        ctx.progress(2, 2)
        store.scratch.remove(part)

    with pytest.raises(AssertionError, match="tmp/tasks/7/attempt-1"):
        await run_with_interruptions(
            leaky, {}, reset=store.reset, snapshot=store.snapshot, task_id=7
        )


async def test_async_reset_and_snapshot_are_awaited():
    store = Store()

    async def reset():
        store.reset()

    async def snapshot():
        return store.snapshot()

    async def copy(payload, ctx):
        store.target = list(store.source)
        ctx.progress(1, 1)

    assert await run_with_interruptions(copy, {}, reset=reset, snapshot=snapshot) == 2


async def test_handlers_without_a_context_or_interruption_points_are_rejected():
    async def no_context(payload):
        return None

    async def silent(payload, ctx):
        return None

    with pytest.raises(TypeError):
        await run_with_interruptions(no_context, {}, reset=lambda: None, snapshot=lambda: None)
    with pytest.raises(AssertionError, match="no interruption points"):
        await run_with_interruptions(silent, {}, reset=lambda: None, snapshot=lambda: None)


async def test_a_nondeterministic_handler_is_reported():
    runs = {"count": 0}

    async def shrinking(payload, ctx):
        runs["count"] += 1
        # The uninterrupted run reports twice, later runs only once.
        for step in range(2 if runs["count"] == 1 else 1):
            ctx.progress(step, 2)

    with pytest.raises(AssertionError, match="not deterministic"):
        await run_with_interruptions(shrinking, {}, reset=lambda: None, snapshot=lambda: 0)


def test_recording_context_crashes_at_the_requested_point_and_skips_the_fence():
    ctx = RecordingContext(3, kind="k", checkpoint_state={"a": 1}, crash_at=3)
    assert (ctx.task_id, ctx.kind, ctx.checkpoint_state) == (3, "k", {"a": 1})
    ctx.assert_owned()
    ctx.stage("one")  # points 1 and 2
    with pytest.raises(SimulatedCrash):
        ctx.checkpoint({"a": 2})  # point 3, before the effect
    assert ctx.checkpoint_state == {"a": 1}
    with pytest.raises(TypeError):
        RecordingContext().checkpoint({"not json": object()})
