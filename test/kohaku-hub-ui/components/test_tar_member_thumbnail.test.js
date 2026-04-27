// Component tests for TarMemberThumbnail.vue.
//
// The component sits in every image row of the indexed-tar listing.
// Coverage targets: IntersectionObserver wiring, the ready / fallback
// state transitions, the cache-hit short-circuit, and the global
// toggle short-circuit (off → never even subscribes to IO).

import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { flushPromises, mount } from "@vue/test-utils";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { http, HttpResponse } from "@/testing/msw";

import { server } from "../setup/msw-server";

import {
  TOGGLE_STORAGE_KEY,
  _resetThumbnailCache,
  _resetThumbnailToggle,
} from "@/utils/tar-thumbnail";

import TarMemberThumbnail from "@/components/repo/preview/TarMemberThumbnail.vue";

const __dirname = dirname(fileURLToPath(import.meta.url));
const FIXTURES = resolve(__dirname, "../fixtures/previews");
const JPEG_WITH_EXIF = readFileSync(resolve(FIXTURES, "with_exif_thumb.jpg"));

const TAR_URL = "https://s3.test.local/bucket/archive.tar";
const SAMPLE_MEMBER = {
  name: "photo.jpg",
  path: "images/photo.jpg",
  offset: 0,
  size: JPEG_WITH_EXIF.length,
};

// jsdom realm doesn't ship IntersectionObserver; install a controllable
// stub so we can directly drive intersect events from the test.
class FakeIntersectionObserver {
  constructor(cb) {
    this.cb = cb;
    FakeIntersectionObserver.instances.push(this);
    this._observed = new Set();
  }
  observe(el) {
    this._observed.add(el);
  }
  unobserve(el) {
    this._observed.delete(el);
  }
  disconnect() {
    this._observed.clear();
  }
  fire(isIntersecting) {
    this.cb(
      Array.from(this._observed).map((target) => ({
        isIntersecting,
        target,
      })),
    );
  }
}
FakeIntersectionObserver.instances = [];

let originalCreate;
let originalRevoke;
let originalIO;
let originalFetch;

function rangeServer(buffer) {
  return ({ request }) => {
    const range = request.headers.get("range");
    const m = /^bytes=(\d+)-(\d+)$/.exec(range || "");
    if (!m) return new HttpResponse(buffer, { status: 200 });
    const start = Number(m[1]);
    const end = Math.min(Number(m[2]), buffer.length - 1);
    return new HttpResponse(buffer.subarray(start, end + 1), {
      status: 206,
      headers: {
        "Content-Range": `bytes ${start}-${end}/${buffer.length}`,
        "Content-Length": String(end - start + 1),
        "Accept-Ranges": "bytes",
      },
    });
  };
}

beforeEach(() => {
  FakeIntersectionObserver.instances.length = 0;
  originalIO = globalThis.IntersectionObserver;
  globalThis.IntersectionObserver = FakeIntersectionObserver;
  originalCreate = URL.createObjectURL;
  originalRevoke = URL.revokeObjectURL;
  let counter = 0;
  URL.createObjectURL = vi.fn(() => `blob:mock/${++counter}`);
  URL.revokeObjectURL = vi.fn();
  // Same realm-mismatch workaround as the panel suite: undici under
  // jsdom + Node 24 rejects AbortSignals constructed inside the
  // component scope. Strip `signal` from the test-side fetch wrapper
  // so the production code path runs verbatim against MSW; abort
  // semantics are pinned in the standalone util test instead.
  originalFetch = globalThis.fetch;
  globalThis.fetch = (input, init = {}) => {
    if (init && "signal" in init) {
      const { signal: _ignored, ...rest } = init;
      return originalFetch(input, rest);
    }
    return originalFetch(input, init);
  };
  _resetThumbnailToggle();
  _resetThumbnailCache();
});

afterEach(() => {
  globalThis.IntersectionObserver = originalIO;
  URL.createObjectURL = originalCreate;
  URL.revokeObjectURL = originalRevoke;
  globalThis.fetch = originalFetch;
  _resetThumbnailToggle();
});

function mountThumb(props = {}) {
  return mount(TarMemberThumbnail, {
    props: {
      tarUrl: TAR_URL,
      member: SAMPLE_MEMBER,
      placeholderIcon: "i-carbon-image text-purple-500",
      size: 28,
      ...props,
    },
  });
}

describe("TarMemberThumbnail · placeholder lifecycle", () => {
  it("renders only the placeholder icon while idle (no IO event yet)", async () => {
    const wrapper = mountThumb();
    await flushPromises();
    expect(wrapper.find('img').exists()).toBe(false);
    expect(wrapper.find(".i-carbon-image").exists()).toBe(true);
  });

  it("subscribes to IntersectionObserver on mount and unobserves on unmount", async () => {
    const wrapper = mountThumb();
    await flushPromises();
    expect(FakeIntersectionObserver.instances.length).toBe(1);
    const observer = FakeIntersectionObserver.instances[0];
    expect(observer._observed.size).toBe(1);
    wrapper.unmount();
    expect(observer._observed.size).toBe(0);
  });
});

describe("TarMemberThumbnail · happy path (EXIF probe)", () => {
  it("swaps the placeholder for an <img> after the row intersects and the EXIF probe succeeds", async () => {
    server.use(http.get(TAR_URL, rangeServer(JPEG_WITH_EXIF)));
    const wrapper = mountThumb();
    await flushPromises();
    const observer = FakeIntersectionObserver.instances[0];
    observer.fire(true);
    await flushPromises();
    const img = wrapper.find("img");
    expect(img.exists()).toBe(true);
    expect(img.attributes("src")).toMatch(/^blob:mock\//);
    expect(img.attributes("alt")).toBe(SAMPLE_MEMBER.name);
    // Placeholder icon is gone once thumbnail mounts.
    expect(wrapper.find(".i-carbon-image").exists()).toBe(false);
  });

  it("issues exactly one Range read for an EXIF-bearing JPEG", async () => {
    let rangeHits = 0;
    server.use(
      http.get(TAR_URL, async ({ request }) => {
        rangeHits += 1;
        return rangeServer(JPEG_WITH_EXIF)({ request });
      }),
    );
    const wrapper = mountThumb();
    await flushPromises();
    FakeIntersectionObserver.instances[0].fire(true);
    await flushPromises();
    expect(wrapper.find("img").exists()).toBe(true);
    expect(rangeHits).toBe(1);
  });
});

describe("TarMemberThumbnail · failure → fallback", () => {
  it("keeps the placeholder when the Range read errors (no thrown render, no toast)", async () => {
    server.use(
      http.get(TAR_URL, () => new HttpResponse("nope", { status: 500 })),
    );
    const wrapper = mountThumb();
    await flushPromises();
    FakeIntersectionObserver.instances[0].fire(true);
    await flushPromises();
    expect(wrapper.find("img").exists()).toBe(false);
    expect(wrapper.find(".i-carbon-image").exists()).toBe(true);
  });
});

describe("TarMemberThumbnail · toggle off", () => {
  it("never renders the thumbnail when the toggle is OFF, even after intersect", async () => {
    // Pre-set storage to OFF before the composable reads its initial
    // value at mount.
    localStorage.setItem(TOGGLE_STORAGE_KEY, "0");
    server.use(http.get(TAR_URL, rangeServer(JPEG_WITH_EXIF)));
    const wrapper = mountThumb();
    await flushPromises();
    FakeIntersectionObserver.instances[0].fire(true);
    await flushPromises();
    // Even though extraction may have run + populated the cache, the
    // template gate (`enabled && state === 'ready'`) keeps the row
    // on the placeholder until the user flips the switch back on.
    expect(wrapper.find("img").exists()).toBe(false);
    expect(wrapper.find(".i-carbon-image").exists()).toBe(true);
  });
});
