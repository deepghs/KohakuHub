"""Pytest fixtures for KohakuHub backend tests."""

from __future__ import annotations

import httpx
import pytest
import pytest_asyncio

from test.kohakuhub.support.bootstrap import ADMIN_TOKEN, DEFAULT_PASSWORD
from test.kohakuhub.support.live_server import start_live_server, stop_live_server
from test.kohakuhub.support.service_bootstrap import apply_service_test_env
from test.kohakuhub.support.service_state import create_service_test_state

apply_service_test_env()

_INITIAL_BASELINE_READY = False
_LAST_BACKEND_MODULE = None
_MISSING = object()
_BACKEND_FIXTURE_NAMES = {
    "prepared_backend_test_state",
    "backend_test_state",
    "app",
    "client",
    "owner_client",
    "member_client",
    "visitor_client",
    "outsider_client",
    "admin_client",
    "live_server_url",
    "hf_api_token",
}


@pytest.fixture(scope="session")
def backend_test_state(pytestconfig):
    terminal_reporter = pytestconfig.pluginmanager.get_plugin("terminalreporter")

    def report_progress(message: str) -> None:
        if terminal_reporter is not None:
            terminal_reporter.write_line(f"[backend-test] {message}")

    return create_service_test_state(progress_callback=report_progress)


@pytest.fixture(scope="session")
async def prepared_backend_test_state(backend_test_state):
    global _INITIAL_BASELINE_READY
    await backend_test_state.prepare()
    _INITIAL_BASELINE_READY = True
    return backend_test_state


@pytest.fixture(scope="session")
def app(prepared_backend_test_state):
    return prepared_backend_test_state.modules.app


@pytest.fixture(autouse=True)
def _restore_backend_state_per_test(request):
    global _INITIAL_BASELINE_READY, _LAST_BACKEND_MODULE

    needs_backend = bool(_BACKEND_FIXTURE_NAMES.intersection(request.fixturenames))
    if not needs_backend:
        return

    backend_test_state = request.getfixturevalue("backend_test_state")
    request.getfixturevalue("prepared_backend_test_state")
    current_module = request.node.module.__name__

    # Drop the LakeFSRestClient singleton's pooled httpx client between
    # tests. ``httpx.AsyncClient`` binds its connection pool to the event
    # loop it was constructed in. Two loops are in play:
    #
    #   * ``restore_active_state()`` uses ``asyncio.run(...)`` internally —
    #     a short-lived loop for the clear+seed phase. FastAPI handlers
    #     driven during the seed lazily build the pool *inside* that loop,
    #     then the loop closes.
    #   * pytest-asyncio gives each test its own fresh loop.
    #
    # We null the singleton both *before* the restore (so the seed phase
    # doesn't inherit a pool tied to the previous test's closed loop) and
    # again *after* (so the test body doesn't inherit the seed phase's
    # pool tied to its now-closed asyncio.run loop). Without the second
    # reset, handlers raise ``Event loop is closed`` on the first LakeFS
    # call.
    from kohakuhub import lakefs_rest_client as _lakefs_rest
    _lakefs_rest._singleton_client = None

    # ``live_server_url`` runs the shared app through FastAPI lifespan.  Its
    # shutdown clears the production operation runtime by assigning ``None``;
    # the next ASGI-transport test must remove that stale marker so it uses
    # the intended test compatibility path instead of looking like a broken
    # PostgreSQL runtime.
    backend_test_state.modules.app.state._state.pop("operation_runtime", None)
    backend_test_state.modules.app.state._khub_test_compatibility = True

    if request.node.get_closest_marker("backend_per_test") is not None:
        backend_test_state.restore_active_state()
        _lakefs_rest._singleton_client = None
        _LAST_BACKEND_MODULE = current_module
        return

    if _LAST_BACKEND_MODULE != current_module:
        if _INITIAL_BASELINE_READY:
            _INITIAL_BASELINE_READY = False
        else:
            backend_test_state.restore_active_state()
            _lakefs_rest._singleton_client = None
        _LAST_BACKEND_MODULE = current_module


_DIRECT_ROUTE_UNIT_MODULES = frozenset(
    {
        "test_operations_unit.py",
        "test_crud_unit.py",
        "test_branches_unit.py",
    }
)


@pytest.fixture(autouse=True)
def _configure_direct_route_unit_compatibility(request):
    """Keep pure route/helper tests on their non-lifespan compatibility path.

    These modules call route helpers directly with fake LakeFS clients; they
    do not use the PostgreSQL-backed service fixtures.  The service bootstrap
    reloads backend modules, so use the gateway instance already imported by
    each unit module rather than assuming the current package-level instance.
    Real backend tests retain the PostgreSQL fence path, and the gateway
    security module is intentionally outside this compatibility fixture.
    """

    if request.node.fspath.basename in _DIRECT_ROUTE_UNIT_MODULES:
        route_module = next(
            (
                getattr(request.module, name, None)
                for name in ("commit_ops", "repo_crud", "branches_api")
                if getattr(request.module, name, None) is not None
            ),
            None,
        )
        gateway = getattr(route_module, "mutation_gateway", None)
        if gateway is None:
            yield
            return
        with gateway.test_compatibility():
            yield
        return
    yield


@pytest.fixture(autouse=True)
def _enable_direct_route_compatibility(request):
    """Allow direct ASGI tests to use the legacy, non-lifespan path.

    The application test state marks the ASGI app as compatibility-mode, but
    the mutation gateway deliberately uses a ContextVar so production config
    cannot enable the bypass.  The live-server tests run their uvicorn app in
    a separate thread and therefore do not inherit this context.  The
    gateway's own tests are excluded so their fail-closed assertions remain
    meaningful.
    """

    if request.node.fspath.basename == "test_mutation_gateway.py":
        yield
        return

    from kohakuhub import lakefs_mutation_gateway

    with lakefs_mutation_gateway.test_compatibility():
        yield


@pytest_asyncio.fixture
async def client(app):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
        follow_redirects=False,
    ) as async_client:
        yield async_client


@pytest_asyncio.fixture
async def owner_client(client):
    response = await client.post(
        "/api/auth/login",
        json={"username": "owner", "password": DEFAULT_PASSWORD},
    )
    response.raise_for_status()
    return client


@pytest_asyncio.fixture
async def member_client(app):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
        follow_redirects=False,
    ) as async_client:
        response = await async_client.post(
            "/api/auth/login",
            json={"username": "member", "password": DEFAULT_PASSWORD},
        )
        response.raise_for_status()
        yield async_client


@pytest_asyncio.fixture
async def visitor_client(app):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
        follow_redirects=False,
    ) as async_client:
        response = await async_client.post(
            "/api/auth/login",
            json={"username": "visitor", "password": DEFAULT_PASSWORD},
        )
        response.raise_for_status()
        yield async_client


@pytest_asyncio.fixture
async def outsider_client(app):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
        follow_redirects=False,
    ) as async_client:
        response = await async_client.post(
            "/api/auth/login",
            json={"username": "outsider", "password": DEFAULT_PASSWORD},
        )
        response.raise_for_status()
        yield async_client


@pytest_asyncio.fixture
async def admin_client(app):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
        follow_redirects=False,
        headers={"X-Admin-Token": ADMIN_TOKEN},
    ) as async_client:
        yield async_client


@pytest.fixture
def live_server_url(app, backend_test_state):
    cfg = backend_test_state.modules.config_module.cfg
    previous_base_url = cfg.app.base_url
    previous_internal_base_url = cfg.app.internal_base_url
    # The ASGI fixtures use an app-local compatibility marker because they do
    # not run FastAPI lifespan. A live uvicorn thread must not inherit it: its
    # lifespan opens the real operation runtime and should exercise fences.
    previous_compatibility = app.state._state.pop(
        "_khub_test_compatibility", _MISSING
    )
    try:
        handle = start_live_server(app)
    except BaseException:
        if previous_compatibility is not _MISSING:
            app.state._khub_test_compatibility = previous_compatibility
        raise
    cfg.app.base_url = handle.base_url
    cfg.app.internal_base_url = handle.base_url
    try:
        yield handle.base_url
    finally:
        cfg.app.base_url = previous_base_url
        cfg.app.internal_base_url = previous_internal_base_url
        stop_live_server(handle)
        if previous_compatibility is not _MISSING:
            app.state._khub_test_compatibility = previous_compatibility


@pytest_asyncio.fixture
async def hf_api_token(owner_client):
    response = await owner_client.post(
        "/api/auth/tokens/create",
        json={"name": "hf-api-compat"},
    )
    response.raise_for_status()
    return response.json()["token"]
