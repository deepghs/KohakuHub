"""Unit coverage for API operation-runtime resource configuration."""

from types import SimpleNamespace

import pytest

import kohakuhub.operations.runtime as runtime_module


class _PoolContext:
    def __init__(self, connection):
        self.connection = connection

    async def __aenter__(self):
        return self.connection

    async def __aexit__(self, *_args):
        return False


class _Pool:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.connection_value = object()
        self.closed = False

    async def open(self, *, wait):
        assert wait is True

    def connection(self):
        return _PoolContext(self.connection_value)

    async def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_operation_runtime_uses_shared_api_pool_limit(monkeypatch):
    monkeypatch.setenv("KOHAKU_HUB_OPERATION_POOL_MAX", "9")
    pools = []
    settings_seen = []

    def pool_factory(**kwargs):
        pool = _Pool(**kwargs)
        pools.append(pool)
        return pool

    class Connector:
        pass

    connector = Connector()
    async def open_async(**_kwargs):
        return _AppContext()

    async def close_async():
        return None

    app = SimpleNamespace(open_async=open_async, close_async=close_async)

    def build_app(settings, *, connector, registry):
        assert connector is not None
        assert registry is not None
        settings_seen.append(settings)
        return app

    class Service:
        def __init__(self, *_args, **_kwargs):
            pass

    monkeypatch.setattr(runtime_module, "AsyncConnectionPool", pool_factory)
    monkeypatch.setattr(runtime_module, "PsycopgConnector", lambda: connector)
    monkeypatch.setattr(runtime_module, "build_worker_app", build_app)
    monkeypatch.setattr(runtime_module, "OperationService", Service)

    async def verify(_connection):
        return None

    monkeypatch.setattr(runtime_module, "verify_operation_schema", verify)

    runtime = await runtime_module.OperationRuntime.open(
        "postgresql://user:pass@localhost/db"
    )
    await runtime.close()

    assert pools[0].kwargs["max_size"] == 9
    assert settings_seen[0].pool_max_size == 9
    assert settings_seen[0].api_pool_max_size == 9
    assert pools[0].closed


@pytest.mark.asyncio
async def test_operation_runtime_closes_pool_when_startup_fails(monkeypatch):
    pools = []

    def pool_factory(**kwargs):
        pool = _Pool(**kwargs)
        pools.append(pool)
        return pool

    async def verify(_connection):
        raise RuntimeError("schema unavailable")

    monkeypatch.setattr(runtime_module, "AsyncConnectionPool", pool_factory)
    monkeypatch.setattr(runtime_module, "verify_operation_schema", verify)

    with pytest.raises(RuntimeError, match="schema unavailable"):
        await runtime_module.OperationRuntime.open(
            "postgresql://user:pass@localhost/db"
        )

    assert pools[0].closed


class _AppContext:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False
