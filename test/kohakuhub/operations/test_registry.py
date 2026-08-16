import pytest

from kohakuhub.operations.registry import DEFAULT_REGISTRY, HandlerSpec, OperationRegistry
from kohakuhub.operations.service import _canonical_payload


def test_default_registry_owns_execution_policy():
    spec = DEFAULT_REGISTRY.get("maintenance.noop.v1")
    assert spec.task_name == "khub:operation:execute.v1"
    assert spec.queue == "control-v1"
    assert spec.priority == 100


def test_unknown_operation_kind_is_rejected():
    with pytest.raises(ValueError, match="unknown operation kind"):
        DEFAULT_REGISTRY.get("user.supplied.callable")


def test_registry_rejects_duplicate_kind():
    async def handler(_operation, _step):
        raise AssertionError

    registry = OperationRegistry()
    spec = HandlerSpec(
        kind="test.v1",
        version="1",
        task_name="khub:operation:execute.v1",
        queue="control-v1",
        priority=0,
        handler=handler,
    )
    registry.register(spec)
    with pytest.raises(ValueError, match="duplicate"):
        registry.register(spec)


def test_operation_payload_rejects_credentials():
    with pytest.raises(ValueError, match="sensitive field"):
        _canonical_payload({"source_token": "must-not-be-persisted"})


def test_operation_payload_is_canonical():
    first, first_hash = _canonical_payload({"b": 2, "a": [1, 2]})
    second, second_hash = _canonical_payload({"a": [1, 2], "b": 2})
    assert first == second
    assert first_hash == second_hash
