import { describe, expect, it, vi } from "vitest";
import { http, HttpResponse } from "@/testing/msw";

import { server } from "../setup/msw-server";

import {
  IndexedTarFetchError,
  IndexedTarFormatError,
  buildMemberRangeHeader,
  buildTreeFromIndex,
  classifyMember,
  compareTarHash,
  downloadBytesAs,
  extractMemberBytes,
  guessMimeType,
  hasIndexSibling,
  listDirectory,
  parseTarIndex,
} from "@/utils/indexed-tar";

const INDEX_URL = "https://s3.test.local/bucket/archive.json";
const TAR_URL = "https://s3.test.local/bucket/archive.tar";

const SAMPLE_INDEX = {
  filesize: 4096,
  hash: "0123456789abcdef0123456789abcdef01234567",
  hash_lfs:
    "abcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabca",
  files: {
    "README.md": { offset: 512, size: 64, sha256: "aa" },
    "images/cover.png": { offset: 1024, size: 256, sha256: "bb" },
    "images/photos/forest.jpg": { offset: 1536, size: 512, sha256: "cc" },
    "audio/bell.wav": { offset: 2048, size: 128 },
    "docs/guide.md": { offset: 2304, size: 32 },
  },
};

describe("parseTarIndex", () => {
  it("fetches and validates a well-formed index JSON", async () => {
    server.use(
      http.get(INDEX_URL, () => HttpResponse.json(SAMPLE_INDEX)),
    );
    const phases = [];
    const payload = await parseTarIndex(INDEX_URL, {
      onProgress: (p) => phases.push(p),
    });
    expect(payload.filesize).toBe(4096);
    expect(Object.keys(payload.files)).toHaveLength(5);
    expect(phases).toEqual(["fetch", "parsing", "done"]);
  });

  it("raises IndexedTarFetchError on a non-2xx response", async () => {
    server.use(
      http.get(INDEX_URL, () =>
        new HttpResponse("not found", { status: 404 }),
      ),
    );
    await expect(parseTarIndex(INDEX_URL)).rejects.toBeInstanceOf(
      IndexedTarFetchError,
    );
  });

  it("raises IndexedTarFormatError when the JSON has no files map", async () => {
    server.use(
      http.get(INDEX_URL, () =>
        HttpResponse.json({ filesize: 1, hash: "", hash_lfs: "" }),
      ),
    );
    await expect(parseTarIndex(INDEX_URL)).rejects.toBeInstanceOf(
      IndexedTarFormatError,
    );
  });

  it("raises IndexedTarFormatError when an entry is missing offset", async () => {
    server.use(
      http.get(INDEX_URL, () =>
        HttpResponse.json({
          filesize: 1,
          hash: "",
          hash_lfs: "",
          files: { broken: { size: 10 } },
        }),
      ),
    );
    await expect(parseTarIndex(INDEX_URL)).rejects.toBeInstanceOf(
      IndexedTarFormatError,
    );
  });
});

describe("buildTreeFromIndex + listDirectory", () => {
  it("groups files by directory prefix and aggregates size + count", () => {
    const tree = buildTreeFromIndex(SAMPLE_INDEX.files);
    expect(tree.fileCount).toBe(5);
    expect(tree.size).toBe(64 + 256 + 512 + 128 + 32);

    const root = listDirectory(tree, []);
    expect(root.folders.map((f) => f.name)).toEqual([
      "audio",
      "docs",
      "images",
    ]);
    expect(root.files.map((f) => f.name)).toEqual(["README.md"]);

    const images = listDirectory(tree, ["images"]);
    expect(images.folders.map((f) => f.name)).toEqual(["photos"]);
    expect(images.files.map((f) => f.name)).toEqual(["cover.png"]);

    const photos = listDirectory(tree, ["images", "photos"]);
    expect(photos.files.map((f) => f.name)).toEqual(["forest.jpg"]);
  });

  it("returns empty listings for an unknown segment", () => {
    const tree = buildTreeFromIndex(SAMPLE_INDEX.files);
    expect(listDirectory(tree, ["does-not-exist"]).folders).toEqual([]);
    expect(listDirectory(tree, ["does-not-exist"]).files).toEqual([]);
  });

  it("strips '.' and '..' segments instead of escaping the tree", () => {
    const tree = buildTreeFromIndex({
      "../escape.txt": { offset: 0, size: 4 },
      "./normal.txt": { offset: 4, size: 4 },
    });
    const root = listDirectory(tree, []);
    expect(root.files.map((f) => f.name).sort()).toEqual([
      "escape.txt",
      "normal.txt",
    ]);
  });
});

describe("buildMemberRangeHeader + extractMemberBytes", () => {
  it("emits the inclusive HTTP byte range hfutils.index uses", () => {
    expect(
      buildMemberRangeHeader({ offset: 100, size: 50 }),
    ).toBe("bytes=100-149");
  });

  it("throws on a zero-byte member so the caller can short-circuit", () => {
    expect(() =>
      buildMemberRangeHeader({ offset: 100, size: 0 }),
    ).toThrow();
  });

  it("Range-reads exactly size bytes from the tar URL", async () => {
    const fullTar = new Uint8Array(64);
    for (let i = 0; i < 64; i++) fullTar[i] = i;

    server.use(
      http.get(TAR_URL, async ({ request }) => {
        const range = request.headers.get("range");
        const match = /^bytes=(\d+)-(\d+)$/.exec(range || "");
        if (!match) return new HttpResponse("missing range", { status: 400 });
        const start = Number(match[1]);
        const end = Number(match[2]);
        const slice = fullTar.subarray(start, end + 1);
        return new HttpResponse(slice, {
          status: 206,
          headers: {
            "Content-Range": `bytes ${start}-${end}/${fullTar.length}`,
            "Content-Length": String(slice.length),
            "Accept-Ranges": "bytes",
          },
        });
      }),
    );

    const bytes = await extractMemberBytes(TAR_URL, { offset: 8, size: 16 });
    expect(bytes).toBeInstanceOf(Uint8Array);
    expect(bytes.length).toBe(16);
    expect(Array.from(bytes)).toEqual([
      8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
    ]);
  });

  it("returns an empty Uint8Array for size=0 without issuing a fetch", async () => {
    const fetchSpy = vi.spyOn(globalThis, "fetch");
    const bytes = await extractMemberBytes(TAR_URL, { offset: 0, size: 0 });
    expect(bytes.length).toBe(0);
    expect(fetchSpy).not.toHaveBeenCalled();
    fetchSpy.mockRestore();
  });
});

describe("compareTarHash", () => {
  it("matches when hash_lfs equals the tree-API sha256-shaped oid", () => {
    const oid = "abcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabcabca";
    expect(
      compareTarHash({ hash_lfs: oid }, { oid, size: 1, type: "file" }),
    ).toEqual({ kind: "match" });
  });

  it("flags a mismatch when sha256 differs", () => {
    const result = compareTarHash(
      { hash_lfs: "a".repeat(64), hash: "" },
      { oid: "b".repeat(64) },
    );
    expect(result.kind).toBe("mismatch");
    expect(result.expected).toBe("a".repeat(64));
    expect(result.actual).toBe("b".repeat(64));
  });

  it("returns 'unknown' when neither hash side has any payload", () => {
    expect(
      compareTarHash({ hash: "", hash_lfs: "" }, { oid: "anything" }),
    ).toEqual({ kind: "unknown" });
  });

  it("returns 'partial' when the index has hashes but the tree entry has none", () => {
    expect(
      compareTarHash({ hash_lfs: "a".repeat(64) }, { oid: "" }),
    ).toEqual({ kind: "partial" });
  });

  it("reads through to lfs.oid when the tar is LFS-stored", () => {
    const sha = "f".repeat(64);
    expect(
      compareTarHash({ hash_lfs: sha }, { oid: "ignored", lfs: { oid: sha } }),
    ).toEqual({ kind: "match" });
  });
});

describe("hasIndexSibling", () => {
  it("returns true when the same listing carries a basename.json sibling", () => {
    const siblings = [
      { type: "file", path: "archives/gallery/bundle.tar" },
      { type: "file", path: "archives/gallery/bundle.json" },
      { type: "file", path: "archives/gallery/README.md" },
    ];
    expect(
      hasIndexSibling("archives/gallery/bundle.tar", siblings),
    ).toBe(true);
  });

  it("returns false for a bare .tar with no sidecar", () => {
    const siblings = [
      { type: "file", path: "archives/gallery/bundle.tar" },
      { type: "file", path: "archives/gallery/README.md" },
    ];
    expect(
      hasIndexSibling("archives/gallery/bundle.tar", siblings),
    ).toBe(false);
  });

  it("ignores directories with the same basename", () => {
    const siblings = [
      { type: "file", path: "archives/foo.tar" },
      { type: "directory", path: "archives/foo.json" },
    ];
    expect(hasIndexSibling("archives/foo.tar", siblings)).toBe(false);
  });

  it("returns false for non-tar paths", () => {
    expect(hasIndexSibling("bundle.json", [])).toBe(false);
    expect(hasIndexSibling("bundle.zip", [])).toBe(false);
  });
});

describe("classifyMember + guessMimeType", () => {
  it("routes common extensions to the correct preview category", () => {
    expect(classifyMember("a.png")).toBe("image");
    expect(classifyMember("a.MP4")).toBe("video");
    expect(classifyMember("a.flac")).toBe("audio");
    expect(classifyMember("a.pdf")).toBe("pdf");
    expect(classifyMember("a.md")).toBe("markdown");
    expect(classifyMember("a.safetensors")).toBe("safetensors");
    expect(classifyMember("a.parquet")).toBe("parquet");
    expect(classifyMember("a.json")).toBe("text");
    expect(classifyMember("a.bin")).toBe("binary");
  });

  it("returns standard MIME types for blob construction", () => {
    expect(guessMimeType("a.png")).toBe("image/png");
    expect(guessMimeType("a.jpg")).toBe("image/jpeg");
    expect(guessMimeType("a.pdf")).toBe("application/pdf");
    expect(guessMimeType("a.unknown")).toBe("application/octet-stream");
  });
});

describe("downloadBytesAs", () => {
  it("constructs an object URL anchor whose download attribute matches the in-tar basename", () => {
    const blobs = [];
    const originalCreate = URL.createObjectURL;
    const originalRevoke = URL.revokeObjectURL;
    URL.createObjectURL = vi.fn((blob) => {
      blobs.push(blob);
      return "blob:test/123";
    });
    URL.revokeObjectURL = vi.fn();

    const created = [];
    const originalCreateElement = document.createElement.bind(document);
    const createSpy = vi.spyOn(document, "createElement").mockImplementation(
      (tag) => {
        const el = originalCreateElement(tag);
        if (tag === "a") {
          el.click = vi.fn();
          created.push(el);
        }
        return el;
      },
    );

    try {
      downloadBytesAs(new Uint8Array([1, 2, 3]), "members/photo.png", "image/png");
      expect(created).toHaveLength(1);
      const a = created[0];
      expect(a.download).toBe("members/photo.png");
      expect(a.href).toBe("blob:test/123");
      expect(a.click).toHaveBeenCalled();
      expect(blobs[0].type).toBe("image/png");
    } finally {
      createSpy.mockRestore();
      URL.createObjectURL = originalCreate;
      URL.revokeObjectURL = originalRevoke;
    }
  });
});
