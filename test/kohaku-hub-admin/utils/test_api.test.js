import { beforeEach, describe, expect, it, vi } from "vitest";
import axios from "@/testing/axios";

const client = {
  get: vi.fn(),
  post: vi.fn(),
  put: vi.fn(),
  patch: vi.fn(),
  delete: vi.fn(),
};

describe("admin API client", () => {
  async function loadModule() {
    vi.resetModules();
    return import("@/utils/api");
  }

  beforeEach(() => {
    vi.restoreAllMocks();

    vi.spyOn(axios, "create").mockReturnValue(client);
    vi.spyOn(axios, "post").mockResolvedValue({ data: {} });
    vi.spyOn(axios, "delete").mockResolvedValue({ data: {} });

    client.get.mockResolvedValue({ data: {} });
    client.post.mockResolvedValue({ data: {} });
    client.put.mockResolvedValue({ data: {} });
    client.patch.mockResolvedValue({ data: {} });
    client.delete.mockResolvedValue({ data: {} });
  });

  it("routes admin endpoints through axios clients with the admin token header", async () => {
    const api = await loadModule();

    await api.listUsers("admin-token", {
      search: "mai",
      limit: 10,
      offset: 5,
      include_orgs: true,
    });
    await api.getUserInfo("admin-token", "mai_lin");
    await api.createUser("admin-token", { username: "new-user" });
    await api.deleteUser("admin-token", "mai_lin", true);
    await api.setEmailVerification("admin-token", "mai_lin", true);
    await api.updateUserQuota("admin-token", "mai_lin", 1000, 2000);

    await api.getQuota("admin-token", "aurora-labs", true);
    await api.setQuota(
      "admin-token",
      "aurora-labs",
      { private_quota_bytes: 1, public_quota_bytes: 2 },
      true,
    );
    await api.recalculateQuota("admin-token", "aurora-labs", true);
    await api.getQuotaOverview("admin-token");

    await api.getSystemStats("admin-token");
    await api.getDetailedStats("admin-token");
    await api.getTimeseriesStats("admin-token", 14);
    await api.getTopRepositories("admin-token", 5, "size");

    await api.getDependencyHealth("admin-token");
    await api.getDependencyHealth("admin-token", { timeoutSeconds: 1.5 });

    await api.listAdminSessions("admin-token");
    await api.listAdminSessions("admin-token", {
      user: "outsider",
      activeOnly: true,
      createdAfter: "2026-01-01T00:00:00Z",
      limit: 5,
      offset: 10,
    });
    await api.revokeAdminSession("admin-token", 42);
    await api.revokeAdminSessionsBulk("admin-token", { user: "outsider" });

    await api.listAdminTokens("admin-token", {
      user: "owner",
      unusedForDays: 30,
    });
    await api.revokeAdminToken("admin-token", 7);

    await api.listAdminSshKeys("admin-token", {
      user: "owner",
      unusedForDays: 90,
    });
    await api.revokeAdminSshKey("admin-token", 3);

    await api.listRepositories("admin-token", {
      search: "lineart",
      repo_type: "model",
      namespace: "mai_lin",
      limit: 5,
      offset: 10,
    });
    await api.getRepositoryDetails(
      "admin-token",
      "model",
      "mai_lin",
      "lineart-caption-base",
    );
    await api.getRepositoryFiles(
      "admin-token",
      "model",
      "mai_lin",
      "lineart-caption-base",
      "dev",
    );
    await api.getRepositoryStorageBreakdown(
      "admin-token",
      "model",
      "mai_lin",
      "lineart-caption-base",
    );
    await api.listCommits("admin-token", {
      repo_full_id: "mai_lin/lineart-caption-base",
      username: "mai_lin",
      limit: 20,
      offset: 40,
    });

    await api.listS3Buckets("admin-token");
    await api.listS3Objects("admin-token", "", {
      prefix: "models/",
      limit: 100,
    });
    await api.recalculateAllRepoStorage("admin-token", {
      repo_type: "dataset",
      namespace: "aurora-labs",
    });

    await api.createRegisterInvitation("admin-token", {
      role: "member",
      max_usage: 1,
      expires_days: 7,
    });
    await api.listInvitations("admin-token", {
      action: "register",
      limit: 10,
      offset: 20,
    });
    await api.deleteInvitation("admin-token", "invite-token");

    await api.globalSearch("admin-token", "mai", ["users", "repos"], 8);

    await api.listDatabaseTables("admin-token");
    await api.getDatabaseQueryTemplates("admin-token");
    await api.executeDatabaseQuery("admin-token", "select 1");

    await api.listFallbackSources("admin-token", {
      namespace: "mai_lin",
      enabled: true,
    });
    await api.getFallbackSource("admin-token", 3);
    await api.createFallbackSource("admin-token", {
      name: "Mirror",
      namespace: "",
      url: "https://hf.example",
      enabled: true,
      priority: 10,
      source_type: "huggingface",
    });
    await api.updateFallbackSource("admin-token", 3, {
      enabled: false,
    });
    await api.deleteFallbackSource("admin-token", 3);
    await api.getFallbackCacheStats("admin-token");
    await api.clearFallbackCache("admin-token");
    await api.invalidateFallbackRepoCache(
      "admin-token",
      "model",
      "mai_lin",
      "lineart-caption-base",
    );
    await api.invalidateFallbackUserCacheById("admin-token", 42);
    await api.invalidateFallbackUserCacheByUsername("admin-token", "mai_lin");
    await api.bulkReplaceFallbackSources("admin-token", [
      {
        namespace: "",
        url: "https://hf.example",
        token: null,
        priority: 10,
        name: "HF",
        source_type: "huggingface",
        enabled: true,
      },
    ]);
    await api.runFallbackChainSimulate("admin-token", {
      op: "info",
      repo_type: "model",
      namespace: "owner",
      name: "demo",
      revision: "main",
      sources: [
        {
          name: "HF", url: "https://hf.example",
          source_type: "huggingface", token: null, priority: 10,
        },
      ],
      as_username: "mai_lin",
      header_tokens: { "https://hf.example": "hf_xxx" },
    });

    await api.deleteRepositoryAdmin(
      "admin-token",
      "model",
      "mai_lin",
      "lineart-caption-base",
    );
    await api.moveRepositoryAdmin(
      "admin-token",
      "model",
      "mai_lin",
      "lineart-caption-base",
      "aurora-labs",
      "lineart-caption-pro",
    );
    await api.squashRepositoryAdmin(
      "admin-token",
      "model",
      "mai_lin",
      "lineart-caption-base",
    );

    await api.deleteS3Object("admin-token", "models/demo/file.bin");
    await api.prepareDeleteS3Prefix("admin-token", "models/demo/");
    await api.deleteS3Prefix("admin-token", "models/demo/", "confirm-123");

    expect(axios.create).toHaveBeenCalledWith({
      baseURL: "/admin/api",
      headers: {
        "X-Admin-Token": "admin-token",
      },
    });

    expect(client.get).toHaveBeenCalledWith("/users", {
      params: { search: "mai", limit: 10, offset: 5, include_orgs: true },
    });
    expect(client.patch).toHaveBeenCalledWith(
      "/users/mai_lin/email-verification",
      null,
      { params: { verified: true } },
    );
    expect(client.put).toHaveBeenCalledWith("/users/mai_lin/quota", {
      private_quota_bytes: 1000,
      public_quota_bytes: 2000,
    });
    expect(client.get).toHaveBeenCalledWith("/quota/aurora-labs", {
      params: { is_org: true },
    });
    expect(client.post).toHaveBeenCalledWith(
      "/quota/aurora-labs/recalculate",
      null,
      { params: { is_org: true } },
    );
    expect(client.get).toHaveBeenCalledWith("/stats/timeseries", {
      params: { days: 14 },
    });
    expect(client.get).toHaveBeenCalledWith("/stats/top-repos", {
      params: { limit: 5, by: "size" },
    });
    expect(client.get).toHaveBeenCalledWith("/health/dependencies", {
      params: {},
    });
    expect(client.get).toHaveBeenCalledWith("/health/dependencies", {
      params: { timeout_seconds: 1.5 },
    });
    expect(client.get).toHaveBeenCalledWith("/sessions", {
      params: { limit: 100, offset: 0 },
    });
    expect(client.get).toHaveBeenCalledWith("/sessions", {
      params: {
        limit: 5,
        offset: 10,
        user: "outsider",
        active_only: true,
        created_after: "2026-01-01T00:00:00Z",
      },
    });
    expect(client.delete).toHaveBeenCalledWith("/sessions/42");
    expect(client.post).toHaveBeenCalledWith("/sessions/revoke-bulk", {
      user: "outsider",
    });
    expect(client.get).toHaveBeenCalledWith("/tokens", {
      params: { limit: 100, offset: 0, user: "owner", unused_for_days: 30 },
    });
    expect(client.delete).toHaveBeenCalledWith("/tokens/7");
    expect(client.get).toHaveBeenCalledWith("/ssh-keys", {
      params: { limit: 100, offset: 0, user: "owner", unused_for_days: 90 },
    });
    expect(client.delete).toHaveBeenCalledWith("/ssh-keys/3");
    expect(client.get).toHaveBeenCalledWith("/repositories", {
      params: {
        search: "lineart",
        repo_type: "model",
        namespace: "mai_lin",
        limit: 5,
        offset: 10,
      },
    });
    expect(client.get).toHaveBeenCalledWith(
      "/repositories/model/mai_lin/lineart-caption-base/files",
      { params: { ref: "dev" } },
    );
    expect(client.get).toHaveBeenCalledWith("/storage/objects", {
      params: { prefix: "models/", limit: 100 },
    });
    expect(client.get).toHaveBeenCalledWith("/search", {
      params: { q: "mai", types: ["users", "repos"], limit: 8 },
    });
    expect(client.post).toHaveBeenCalledWith("/database/query", {
      sql: "select 1",
    });
    expect(client.get).toHaveBeenCalledWith("/fallback-sources", {
      params: { namespace: "mai_lin", enabled: true },
    });
    expect(client.delete).toHaveBeenCalledWith(
      "/fallback-sources/cache/repo/model/mai_lin/lineart-caption-base",
    );
    expect(client.delete).toHaveBeenCalledWith(
      "/fallback-sources/cache/user/42",
    );
    expect(client.delete).toHaveBeenCalledWith(
      "/fallback-sources/cache/username/mai_lin",
    );
    expect(client.put).toHaveBeenCalledWith(
      "/fallback/sources-bulk-replace",
      expect.objectContaining({
        sources: expect.any(Array),
      }),
    );
    expect(client.post).toHaveBeenCalledWith(
      "/fallback/test/simulate",
      expect.objectContaining({
        op: "info",
        as_username: "mai_lin",
        sources: expect.any(Array),
        header_tokens: { "https://hf.example": "hf_xxx" },
      }),
    );
    expect(client.delete).toHaveBeenCalledWith(
      "/storage/objects/models%2Fdemo%2Ffile.bin",
    );
    expect(client.post).toHaveBeenCalledWith(
      "/storage/prefix/prepare-delete",
      null,
      { params: { prefix: "models/demo/" } },
    );
    expect(client.delete).toHaveBeenCalledWith("/storage/prefix", {
      params: { prefix: "models/demo/", confirm_token: "confirm-123" },
    });
    expect(axios.delete).toHaveBeenCalledWith("/api/repos/delete", {
      headers: { "X-Admin-Token": "admin-token" },
      data: {
        type: "model",
        name: "lineart-caption-base",
        organization: "mai_lin",
      },
    });
    expect(axios.post).toHaveBeenCalledWith(
      "/api/repos/move",
      {
        fromRepo: "mai_lin/lineart-caption-base",
        toRepo: "aurora-labs/lineart-caption-pro",
        type: "model",
      },
      {
        headers: { "X-Admin-Token": "admin-token" },
      },
    );
    expect(axios.post).toHaveBeenCalledWith(
      "/api/repos/squash",
      {
        repo: "mai_lin/lineart-caption-base",
        type: "model",
      },
      {
        headers: { "X-Admin-Token": "admin-token" },
      },
    );
  });

  // ---------------------------------------------------------------------
  // Chain-tester helpers (#78 redesign):
  //   - decodeChainTraceHeader: tolerant base64-JSON decoder
  //   - buildProbeRequestTarget: URL + method per op
  //   - runFallbackProbe: real-request driver, parses X-Chain-Trace
  // ---------------------------------------------------------------------

  it("decodeChainTraceHeader decodes valid headers and tolerates broken input", async () => {
    const api = await loadModule();
    const hops = [
      { kind: "local", source_name: "local", decision: "LOCAL_HIT" },
      { kind: "fallback", source_name: "HF", decision: "BIND_AND_RESPOND" },
    ];
    const encoded = btoa(JSON.stringify({ version: 1, hops }));
    expect(api.decodeChainTraceHeader(encoded)).toEqual(hops);

    // Tolerant fallthroughs.
    expect(api.decodeChainTraceHeader(null)).toEqual([]);
    expect(api.decodeChainTraceHeader(undefined)).toEqual([]);
    expect(api.decodeChainTraceHeader("")).toEqual([]);
    expect(api.decodeChainTraceHeader("not-base64")).toEqual([]);
    // Valid base64, invalid JSON.
    expect(api.decodeChainTraceHeader(btoa("{not json"))).toEqual([]);
    // Valid JSON without hops.
    expect(
      api.decodeChainTraceHeader(btoa(JSON.stringify({ version: 1 }))),
    ).toEqual([]);
  });

  it("buildProbeRequestTarget assembles the URL + method for each op", async () => {
    const api = await loadModule();
    const base = { repo_type: "model", namespace: "owner", name: "demo" };

    expect(api.buildProbeRequestTarget({ op: "info", ...base })).toEqual({
      url: "/api/models/owner/demo",
      method: "get",
    });
    expect(
      api.buildProbeRequestTarget({ op: "tree", revision: "main", file_path: "", ...base }),
    ).toEqual({
      url: "/api/models/owner/demo/tree/main",
      method: "get",
    });
    expect(
      api.buildProbeRequestTarget({
        op: "tree",
        revision: "dev",
        file_path: "subdir/leaf",
        ...base,
      }),
    ).toEqual({
      url: "/api/models/owner/demo/tree/dev/subdir/leaf",
      method: "get",
    });
    expect(
      api.buildProbeRequestTarget({
        op: "resolve",
        revision: "main",
        file_path: "config.json",
        ...base,
      }),
    ).toEqual({
      url: "/models/owner/demo/resolve/main/config.json",
      method: "head",
    });
    expect(
      api.buildProbeRequestTarget({ op: "paths_info", revision: "main", ...base }),
    ).toEqual({
      url: "/api/models/owner/demo/paths-info/main",
      method: "post",
    });
    // Bad op throws.
    expect(() =>
      api.buildProbeRequestTarget({ op: "totally-bogus", ...base }),
    ).toThrow(/Unknown probe op/);
  });

  it("runFallbackProbe sends the real request, decodes X-Chain-Trace, and assembles a report", async () => {
    const api = await loadModule();

    const hops = [
      {
        kind: "local",
        source_name: "local",
        source_url: null,
        source_type: null,
        method: "GET",
        upstream_path: "/api/models/owner/demo",
        status_code: 200,
        x_error_code: null,
        x_error_message: null,
        decision: "LOCAL_HIT",
        duration_ms: 12,
        error: null,
      },
    ];
    const encoded = btoa(JSON.stringify({ version: 1, hops }));

    const requestSpy = vi.spyOn(axios, "request").mockResolvedValue({
      status: 200,
      headers: {
        "content-type": "application/json",
        "x-chain-trace": encoded,
      },
      data: { id: "owner/demo", private: false },
    });

    const report = await api.runFallbackProbe({
      op: "info",
      repo_type: "model",
      namespace: "owner",
      name: "demo",
      revision: "main",
      authorization: "Bearer khub_xxx|https://hf.example,hf_yyy",
    });

    expect(requestSpy).toHaveBeenCalledTimes(1);
    const call = requestSpy.mock.calls[0][0];
    expect(call.url).toBe("/api/models/owner/demo");
    expect(call.method).toBe("get");
    expect(call.headers.Authorization).toBe(
      "Bearer khub_xxx|https://hf.example,hf_yyy",
    );
    expect(call.validateStatus(404)).toBe(true);  // accepts all statuses

    expect(report.final_outcome).toBe("LOCAL_HIT");
    expect(report.bound_source).toEqual({ name: "local", url: null });
    expect(report.attempts).toHaveLength(1);
    expect(report.attempts[0].decision).toBe("LOCAL_HIT");
    expect(report.attempts[0].kind).toBe("local");
    expect(report.final_response.status_code).toBe(200);
    expect(report.final_response.body_preview).toContain('"id": "owner/demo"');
    // Curated headers — only the relevant set is preserved.
    expect(report.final_response.headers["content-type"]).toBe(
      "application/json",
    );
  });

  it("runFallbackProbe with no Authorization sends no Authorization header", async () => {
    const api = await loadModule();
    const requestSpy = vi.spyOn(axios, "request").mockResolvedValue({
      status: 200,
      headers: {},
      data: {},
    });
    await api.runFallbackProbe({
      op: "info",
      repo_type: "model",
      namespace: "owner",
      name: "demo",
    });
    const call = requestSpy.mock.calls[0][0];
    expect(call.headers.Authorization).toBeUndefined();
  });

  it("runFallbackProbe synthesizes a NETWORK_ERROR report when the request throws", async () => {
    const api = await loadModule();
    vi.spyOn(axios, "request").mockRejectedValue(new Error("connection refused"));

    const report = await api.runFallbackProbe({
      op: "info",
      repo_type: "model",
      namespace: "owner",
      name: "demo",
    });

    expect(report.final_outcome).toBe("ERROR");
    expect(report.bound_source).toBeNull();
    expect(report.attempts).toHaveLength(1);
    expect(report.attempts[0].decision).toBe("NETWORK_ERROR");
    expect(report.attempts[0].error).toBe("connection refused");
    expect(report.final_response).toBeNull();
  });

  it("runFallbackProbe missing X-Chain-Trace yields an empty timeline (CHAIN_EXHAUSTED)", async () => {
    const api = await loadModule();
    vi.spyOn(axios, "request").mockResolvedValue({
      status: 200,
      headers: { "content-type": "text/plain" },
      data: "hello",
    });
    const report = await api.runFallbackProbe({
      op: "info",
      repo_type: "model",
      namespace: "owner",
      name: "demo",
    });
    expect(report.attempts).toEqual([]);
    expect(report.final_outcome).toBe("CHAIN_EXHAUSTED");
    expect(report.final_response.status_code).toBe(200);
    expect(report.final_response.body_preview).toBe("hello");
  });

  it("runFallbackProbe paths_info packs paths into the request body", async () => {
    const api = await loadModule();
    const requestSpy = vi.spyOn(axios, "request").mockResolvedValue({
      status: 200,
      headers: {},
      data: [],
    });
    await api.runFallbackProbe({
      op: "paths_info",
      repo_type: "dataset",
      namespace: "owner",
      name: "demo",
      revision: "main",
      paths: ["README.md", "config.json"],
    });
    const call = requestSpy.mock.calls[0][0];
    expect(call.method).toBe("post");
    expect(call.url).toBe("/api/datasets/owner/demo/paths-info/main");
    expect(call.data).toEqual({ paths: ["README.md", "config.json"] });
  });

  // -------------------------------------------------------------------
  // Per-probe trace cookie pickup (#78 v3): on redirect-follow paths
  // the W3C Fetch spec strips X-Chain-Trace from JS visibility, so
  // ``runFallbackProbe`` falls back to a per-probe cookie set by the
  // backend. The cookie name is ``_khub_chain_trace_<probeId>`` where
  // probeId comes from the request's ``X-Khub-Probe-Id`` header.
  // -------------------------------------------------------------------

  // Helper: clear any leftover trace cookies between tests so cookie
  // jar from prior cases doesn't leak into the current one. The test
  // file's ``afterEach`` only resets axios mocks.
  function _clearAllTraceCookies() {
    document.cookie
      .split(";")
      .map((c) => c.trim())
      .forEach((cookie) => {
        const eq = cookie.indexOf("=");
        const name = eq > 0 ? cookie.slice(0, eq) : cookie;
        if (name.startsWith("_khub_chain_trace_")) {
          document.cookie = `${name}=; Max-Age=0; Path=/; SameSite=Lax`;
        }
      });
  }

  it("runFallbackProbe sends X-Khub-Probe-Id header on every call", async () => {
    const api = await loadModule();
    const requestSpy = vi.spyOn(axios, "request").mockResolvedValue({
      status: 200,
      headers: {},
      data: {},
    });
    await api.runFallbackProbe({
      op: "info", repo_type: "model", namespace: "owner", name: "demo",
    });
    const call = requestSpy.mock.calls[0][0];
    expect(call.headers["X-Khub-Probe-Id"]).toMatch(/.+/);
  });

  it("runFallbackProbe generates a fresh probe id per call", async () => {
    const api = await loadModule();
    const requestSpy = vi.spyOn(axios, "request").mockResolvedValue({
      status: 200,
      headers: {},
      data: {},
    });
    await api.runFallbackProbe({
      op: "info", repo_type: "model", namespace: "owner", name: "demo",
    });
    await api.runFallbackProbe({
      op: "info", repo_type: "model", namespace: "owner", name: "demo",
    });
    const id1 = requestSpy.mock.calls[0][0].headers["X-Khub-Probe-Id"];
    const id2 = requestSpy.mock.calls[1][0].headers["X-Khub-Probe-Id"];
    expect(id1).not.toBe(id2);
  });

  it("runFallbackProbe falls back to per-probe cookie when X-Chain-Trace header is missing", async () => {
    _clearAllTraceCookies();
    const api = await loadModule();
    // Capture the probe id the SPA generates so we can plant a
    // matching cookie before the axios mock returns.
    let capturedProbeId = null;
    vi.spyOn(axios, "request").mockImplementation(async (cfg) => {
      capturedProbeId = cfg.headers["X-Khub-Probe-Id"];
      // Plant the cookie now — simulates the redirect-follow case
      // where the backend Set-Cookie'd before the redirect, and the
      // browser carried it through to the post-redirect response.
      const hops = [
        { kind: "local", source_name: "local", decision: "LOCAL_MISS",
          status_code: 404, duration_ms: 1 },
        { kind: "fallback", source_name: "HF", decision: "BIND_AND_RESPOND",
          status_code: 200, duration_ms: 12 },
      ];
      const traceValue = btoa(JSON.stringify({ version: 1, hops }));
      document.cookie =
        `_khub_chain_trace_${capturedProbeId}=${traceValue}; ` +
        `Max-Age=300; Path=/; SameSite=Lax`;
      return { status: 200, headers: {}, data: {} };  // no X-Chain-Trace
    });

    const report = await api.runFallbackProbe({
      op: "info", repo_type: "model", namespace: "owner", name: "demo",
    });
    expect(report.attempts).toHaveLength(2);
    expect(report.attempts[0].decision).toBe("LOCAL_MISS");
    expect(report.attempts[1].decision).toBe("BIND_AND_RESPOND");
    expect(report.final_outcome).toBe("BIND_AND_RESPOND");
    // Cookie cleaned up after pickup.
    expect(document.cookie).not.toContain(
      `_khub_chain_trace_${capturedProbeId}=`,
    );
  });

  it("runFallbackProbe prefers X-Chain-Trace header over cookie when both present", async () => {
    _clearAllTraceCookies();
    const api = await loadModule();
    const headerHops = [
      { kind: "local", source_name: "local", decision: "LOCAL_HIT",
        status_code: 200, duration_ms: 7 },
    ];
    const headerEncoded = btoa(JSON.stringify({ version: 1, hops: headerHops }));
    let plantedProbeId = null;
    vi.spyOn(axios, "request").mockImplementation(async (cfg) => {
      plantedProbeId = cfg.headers["X-Khub-Probe-Id"];
      // Plant a *different* trace into the cookie so we can prove
      // the header took precedence.
      const decoyHops = [
        { kind: "local", source_name: "local", decision: "LOCAL_MISS",
          status_code: 404, duration_ms: 1 },
      ];
      const decoy = btoa(JSON.stringify({ version: 1, hops: decoyHops }));
      document.cookie =
        `_khub_chain_trace_${plantedProbeId}=${decoy}; ` +
        `Max-Age=300; Path=/; SameSite=Lax`;
      return {
        status: 200,
        headers: { "x-chain-trace": headerEncoded },
        data: {},
      };
    });
    const report = await api.runFallbackProbe({
      op: "info", repo_type: "model", namespace: "owner", name: "demo",
    });
    // Header (LOCAL_HIT) wins — not the cookie's LOCAL_MISS decoy.
    expect(report.attempts).toHaveLength(1);
    expect(report.attempts[0].decision).toBe("LOCAL_HIT");
    expect(report.final_outcome).toBe("LOCAL_HIT");
    // Cookie still cleaned up regardless.
    expect(document.cookie).not.toContain(
      `_khub_chain_trace_${plantedProbeId}=`,
    );
  });

  it("runFallbackProbe doesn't read other probes' cookies (concurrent isolation)", async () => {
    _clearAllTraceCookies();
    const api = await loadModule();
    // Plant a cookie under a *different* probe id (simulating a
    // concurrent in-flight probe from another tab/run).
    const otherHops = [{ kind: "local", source_name: "local",
      decision: "LOCAL_HIT", status_code: 200, duration_ms: 1 }];
    const otherEncoded = btoa(JSON.stringify({ version: 1, hops: otherHops }));
    document.cookie =
      `_khub_chain_trace_other-probe=${otherEncoded}; ` +
      `Max-Age=300; Path=/; SameSite=Lax`;

    vi.spyOn(axios, "request").mockResolvedValue({
      status: 200, headers: {}, data: {},
    });
    const report = await api.runFallbackProbe({
      op: "info", repo_type: "model", namespace: "owner", name: "demo",
    });
    // Empty timeline — own cookie wasn't planted, other-probe's
    // cookie must NOT be picked up because the name doesn't match.
    expect(report.attempts).toEqual([]);
    expect(report.final_outcome).toBe("CHAIN_EXHAUSTED");
    // The other probe's cookie is untouched (different probe id,
    // different cookie name).
    expect(document.cookie).toContain("_khub_chain_trace_other-probe=");
    // Clean up so this doesn't leak into other tests.
    _clearAllTraceCookies();
  });

  it("verifies admin tokens and formats quota sizes safely", async () => {
    const api = await loadModule();

    client.get
      .mockResolvedValueOnce({ data: {} })
      .mockRejectedValueOnce({ response: { status: 401 } })
      .mockRejectedValueOnce(new Error("backend-down"));

    await expect(api.verifyAdminToken("valid-token")).resolves.toBe(true);
    await expect(api.verifyAdminToken("invalid-token")).resolves.toBe(false);
    await expect(api.verifyAdminToken("broken-token")).rejects.toThrow(
      "backend-down",
    );

    expect(api.formatBytes(null)).toBe("Unlimited");
    expect(api.formatBytes(0)).toBe("0 Bytes");
    expect(api.formatBytes(1530, 1)).toBe("1.5 KB");
    expect(api.parseSize("10GB")).toBe(10_000_000_000);
    expect(api.parseSize("1.5 MB")).toBe(1_500_000);
    expect(api.parseSize("unlimited")).toBeNull();
    expect(api.parseSize("broken")).toBeNull();
  });
});
