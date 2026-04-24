import { describe, expect, it } from "vitest";

import {
  classifyError,
  classifyResponse,
  defaultCopyFor,
  ERROR_KIND,
} from "@/utils/http-errors";

// Convenience: build a Response-like object with Headers + optional
// JSON body, so every test below is one line of setup and one line of
// assertion. Using the real `Response` constructor ensures we're
// exercising the same `clone()` + `json()` paths that production
// code walks.
function mkResponse({
  status = 200,
  headers = {},
  body = null,
}) {
  const init = { status, headers: new Headers(headers) };
  if (body == null) return new Response(null, init);
  if (typeof body === "string") return new Response(body, init);
  init.headers.set("Content-Type", "application/json");
  return new Response(JSON.stringify(body), init);
}

describe("classifyResponse", () => {
  it("returns a GENERIC placeholder for a null / undefined response", async () => {
    const out = await classifyResponse(null);
    expect(out.kind).toBe(ERROR_KIND.GENERIC);
    expect(out.status).toBeNull();
    expect(out.errorCode).toBeNull();
    expect(out.sources).toBeNull();
  });

  it("reads X-Error-Code=GatedRepo on 401 → gated", async () => {
    const res = mkResponse({
      status: 401,
      headers: { "X-Error-Code": "GatedRepo", "X-Error-Message": "need auth" },
    });
    const out = await classifyResponse(res);
    expect(out.kind).toBe(ERROR_KIND.GATED);
    expect(out.status).toBe(401);
    expect(out.errorCode).toBe("GatedRepo");
    expect(out.detail).toBe("need auth");
  });

  it("treats a bare 401 without X-Error-Code as gated anyway", async () => {
    const res = mkResponse({ status: 401 });
    const out = await classifyResponse(res);
    expect(out.kind).toBe(ERROR_KIND.GATED);
  });

  it("classifies 403 → forbidden", async () => {
    const res = mkResponse({ status: 403 });
    const out = await classifyResponse(res);
    expect(out.kind).toBe(ERROR_KIND.FORBIDDEN);
  });

  it("classifies 404 / 410 / EntryNotFound / RepoNotFound as not-found", async () => {
    expect((await classifyResponse(mkResponse({ status: 404 }))).kind).toBe(
      ERROR_KIND.NOT_FOUND,
    );
    expect((await classifyResponse(mkResponse({ status: 410 }))).kind).toBe(
      ERROR_KIND.NOT_FOUND,
    );
    expect(
      (
        await classifyResponse(
          mkResponse({
            status: 502,
            headers: { "X-Error-Code": "EntryNotFound" },
          }),
        )
      ).kind,
    ).toBe(ERROR_KIND.NOT_FOUND);
    expect(
      (
        await classifyResponse(
          mkResponse({
            status: 500,
            headers: { "X-Error-Code": "RepoNotFound" },
          }),
        )
      ).kind,
    ).toBe(ERROR_KIND.NOT_FOUND);
    expect(
      (
        await classifyResponse(
          mkResponse({
            status: 500,
            headers: { "X-Error-Code": "RevisionNotFound" },
          }),
        )
      ).kind,
    ).toBe(ERROR_KIND.NOT_FOUND);
  });

  it("classifies 5xx → upstream-unavailable", async () => {
    for (const s of [500, 502, 503, 504]) {
      expect((await classifyResponse(mkResponse({ status: s }))).kind).toBe(
        ERROR_KIND.UPSTREAM_UNAVAILABLE,
      );
    }
  });

  it("picks up the aggregated {error, detail, sources[]} body shape", async () => {
    const res = mkResponse({
      status: 401,
      headers: { "X-Error-Code": "GatedRepo" },
      body: {
        error: "GatedRepo",
        detail: "Upstream source requires authentication",
        sources: [
          {
            name: "HuggingFace",
            status: 401,
            category: "auth",
            message: "Access restricted",
          },
        ],
      },
    });
    const out = await classifyResponse(res);
    expect(out.kind).toBe(ERROR_KIND.GATED);
    expect(out.detail).toBe("Upstream source requires authentication");
    expect(out.sources).toHaveLength(1);
    expect(out.sources[0].name).toBe("HuggingFace");
  });

  it("falls back to body.error when X-Error-Code is absent", async () => {
    const res = mkResponse({
      status: 404,
      body: { error: "RepoNotFound", detail: "..." },
    });
    const out = await classifyResponse(res);
    expect(out.errorCode).toBe("RepoNotFound");
    expect(out.kind).toBe(ERROR_KIND.NOT_FOUND);
  });

  it("ignores body.sources when it is not an array", async () => {
    const res = mkResponse({
      status: 500,
      body: { sources: "oops" },
    });
    const out = await classifyResponse(res);
    expect(out.sources).toBeNull();
  });

  it("tolerates non-JSON bodies without throwing", async () => {
    const res = mkResponse({
      status: 500,
      body: "<html>upstream exploded</html>",
    });
    const out = await classifyResponse(res);
    expect(out.kind).toBe(ERROR_KIND.UPSTREAM_UNAVAILABLE);
    expect(out.errorCode).toBeNull();
  });

  it("works on axios-shaped responses (plain object headers, data already parsed)", async () => {
    // Axios surfaces response.data already parsed and response.headers
    // as a plain object (not a Headers instance). classifyResponse
    // should accept either shape.
    const axiosLike = {
      status: 401,
      headers: { "x-error-code": "GatedRepo" },
      data: { sources: [{ name: "HF", status: 401, category: "auth" }] },
    };
    const out = await classifyResponse(axiosLike);
    expect(out.kind).toBe(ERROR_KIND.GATED);
    expect(out.sources).toHaveLength(1);
  });

  it("returns generic for a plain non-matching 418", async () => {
    const out = await classifyResponse(mkResponse({ status: 418 }));
    expect(out.kind).toBe(ERROR_KIND.GENERIC);
  });
});

describe("classifyError", () => {
  it("returns a GENERIC placeholder for null / undefined", () => {
    expect(classifyError(null).kind).toBe(ERROR_KIND.GENERIC);
    expect(classifyError(undefined).kind).toBe(ERROR_KIND.GENERIC);
  });

  it("classifies axios-style errors via err.response", () => {
    const err = {
      response: {
        status: 401,
        headers: { "X-Error-Code": "GatedRepo" },
        data: {
          detail: "need auth",
          sources: [{ name: "HF", status: 401, category: "auth" }],
        },
      },
    };
    const out = classifyError(err);
    expect(out.kind).toBe(ERROR_KIND.GATED);
    expect(out.detail).toBe("need auth");
    expect(out.sources).toHaveLength(1);
  });

  it("classifies axios-style headers accessed via .get()", () => {
    const err = {
      response: {
        status: 404,
        headers: {
          get: (n) =>
            n.toLowerCase() === "x-error-code" ? "EntryNotFound" : null,
        },
        data: null,
      },
    };
    expect(classifyError(err).kind).toBe(ERROR_KIND.NOT_FOUND);
  });

  it("classifies SafetensorsFetchError-style errors (inline status/errorCode/sources)", () => {
    const err = {
      name: "SafetensorsFetchError",
      message: "range read failed",
      status: 502,
      errorCode: null,
      sources: [{ name: "HF", status: null, category: "timeout" }],
      detail: "upstream unavailable",
    };
    const out = classifyError(err);
    expect(out.kind).toBe(ERROR_KIND.UPSTREAM_UNAVAILABLE);
    expect(out.status).toBe(502);
    expect(out.detail).toBe("upstream unavailable");
    expect(out.sources).toHaveLength(1);
  });

  it("flags TypeError / Failed to fetch as cors", () => {
    expect(classifyError(new TypeError("Failed to fetch")).kind).toBe(
      ERROR_KIND.CORS,
    );
    const nerr = new Error("NetworkError when attempting to fetch");
    nerr.name = "NetworkError"; // Firefox shape (not a TypeError)
    expect(classifyError(nerr).kind).toBe(ERROR_KIND.CORS);
  });

  it("returns generic for AbortError without flooding the UI", () => {
    const abort = new Error("aborted");
    abort.name = "AbortError";
    const out = classifyError(abort);
    expect(out.kind).toBe(ERROR_KIND.GENERIC);
    expect(out.detail).toBe("aborted");
  });

  it("falls back to .message detail for plain Errors", () => {
    const err = new Error("boom");
    expect(classifyError(err).detail).toBe("boom");
  });
});

describe("defaultCopyFor", () => {
  it("returns distinct copy for every declared kind", () => {
    const kinds = Object.values(ERROR_KIND);
    const titles = new Set(kinds.map((k) => defaultCopyFor(k).title));
    expect(titles.size).toBe(kinds.length);
    for (const k of kinds) {
      const c = defaultCopyFor(k);
      expect(typeof c.title).toBe("string");
      expect(c.title.length).toBeGreaterThan(0);
      expect(typeof c.hint).toBe("string");
      expect(c.hint.length).toBeGreaterThan(0);
    }
  });

  it("falls back to GENERIC copy for unknown kinds", () => {
    expect(defaultCopyFor("nonsense")).toEqual(
      defaultCopyFor(ERROR_KIND.GENERIC),
    );
    expect(defaultCopyFor(undefined)).toEqual(
      defaultCopyFor(ERROR_KIND.GENERIC),
    );
  });
});
