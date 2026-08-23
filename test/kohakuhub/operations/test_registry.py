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


def test_global_delivery_lock_ignores_resource_key():
    async def handler(_operation, _step):
        raise AssertionError

    spec = HandlerSpec(
        kind="test.global.v1",
        version="1",
        task_name="khub:operation:execute.v1",
        queue="bulk-v1",
        priority=0,
        handler=handler,
        lock_prefix="khub:bulk:v1",
        lock_scope="global",
    )
    assert spec.delivery_lock("repository-a") == "khub:bulk:v1"
    assert spec.delivery_lock("repository-b") == "khub:bulk:v1"


def test_global_delivery_lock_requires_prefix():
    async def handler(_operation, _step):
        raise AssertionError

    with pytest.raises(ValueError, match="global operation locks"):
        OperationRegistry(
            (
                HandlerSpec(
                    kind="test.invalid-global.v1",
                    version="1",
                    task_name="khub:operation:execute.v1",
                    queue="bulk-v1",
                    priority=0,
                    handler=handler,
                    lock_scope="global",
                ),
            )
        )


@pytest.mark.parametrize(
    "payload",
    [
        {"source_token": "must-not-be-persisted"},
        {"metadata": {"headers": {"Authorization": "Bearer secret"}}},
        {"metadata": {"headers": {"x-api-key": "secret"}}},
        {
            "metadata": {
                "headers": [{"name": "Authorization", "value": "Bearer secret"}]
            }
        },
        {"metadata": {"headers": [["x-api-key", "secret"]]}},
        {"metadata": {"AWS_ACCESS_KEY_ID": "AKIAEXAMPLE"}},
        {"metadata": {"awsSecretAccessKey": "secret"}},
        {"metadata": {"X-Amz-Security-Token": "secret"}},
        {"metadata": {"x_amz_signature": "secret"}},
    ],
)
def test_operation_payload_rejects_sensitive_fields(payload):
    with pytest.raises(ValueError, match="sensitive field"):
        _canonical_payload(payload)


@pytest.mark.parametrize(
    "url",
    [
        "https://user:password@example.test/hook",
        "https://example.test/hook?X-Amz-Signature=secret",
        "https://example.test/hook?X-Amz-Credential=AKIA%2Fscope",
        "https://example.test/hook?X-Amz-Security-Token=secret",
        "https://example.test/hook?x-api-key=secret",
        "https://example.test/hook?access_token=secret",
    ],
)
def test_operation_payload_rejects_credential_bearing_urls(url):
    with pytest.raises(ValueError, match="credential-bearing URL"):
        _canonical_payload({"metadata": {"callback": url}})


def test_operation_payload_is_canonical():
    first, first_hash = _canonical_payload({"b": 2, "a": [1, 2]})
    second, second_hash = _canonical_payload({"a": [1, 2], "b": 2})
    assert first == second
    assert first_hash == second_hash
