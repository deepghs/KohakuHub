import { flushPromises, mount } from "@vue/test-utils";
import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { http, HttpResponse } from "@/testing/msw";
import { ElementPlusStubs, RouterLinkStub } from "../helpers/vue";
import {
  cloneFixture,
  jsonResponse,
  uiApiFixtures,
} from "../helpers/api-fixtures";
import { server } from "../setup/msw-server";

const mocks = vi.hoisted(() => ({
  router: {
    push: vi.fn(),
    back: vi.fn(),
  },
}));

vi.mock("vue-router/auto", () => ({
  useRouter: () => mocks.router,
}));

import RepoViewer from "@/components/repo/RepoViewer.vue";

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

describe("RepoViewer path handling", () => {
  const requests = {
    pathsInfo: [],
    tree: [],
  };

  function repoInfoFor(name) {
    return {
      ...cloneFixture(uiApiFixtures.repo.info),
      id: `open-media-lab/${name}`,
    };
  }

  function installBaseHandlers() {
    requests.pathsInfo.length = 0;
    requests.tree.length = 0;

    server.use(
      http.get("/api/users/open-media-lab/type", () =>
        jsonResponse({ type: "org" }),
      ),
      http.get("/api/datasets/open-media-lab/:name", ({ params }) =>
        jsonResponse(repoInfoFor(params.name)),
      ),
      http.get("/api/datasets/open-media-lab/:name/commits/main", () =>
        jsonResponse(cloneFixture(uiApiFixtures.repo.commitsHf.page1)),
      ),
    );
  }

  beforeEach(() => {
    vi.clearAllMocks();
    setActivePinia(createPinia());
    vi.spyOn(console, "error").mockImplementation(() => {});
    installBaseHandlers();
  });

  function mountViewer(props) {
    return mount(RepoViewer, {
      props: {
        repoType: "dataset",
        namespace: "open-media-lab",
        name: "hierarchy-crawl-fixtures",
        branch: "main",
        currentPath: "",
        tab: "files",
        ...props,
      },
      global: {
        stubs: {
          ...ElementPlusStubs,
          RouterLink: RouterLinkStub,
          MarkdownViewer: true,
          MetadataHeader: true,
          DetailedMetadataPanel: true,
          ReferencedDatasetsCard: true,
          SidebarRelationshipsCard: true,
          DatasetViewerTab: true,
        },
      },
    });
  }

  it("loads repo-root tree entries through the API client, merges expanded path info, and links commits", async () => {
    server.use(
      http.get(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main/catalog",
        ({ request }) => {
          const url = new URL(request.url);
          requests.tree.push({
            name: "hierarchy-crawl-fixtures",
            path: "/catalog",
            params: Object.fromEntries(url.searchParams.entries()),
          });
          return jsonResponse(cloneFixture(uiApiFixtures.repo.tree));
        },
      ),
      http.post(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/paths-info/main",
        async ({ request }) => {
          const form = new URLSearchParams(await request.clone().text());
          requests.pathsInfo.push({
            name: "hierarchy-crawl-fixtures",
            paths: form.getAll("paths"),
            expand: form.get("expand"),
          });
          return jsonResponse(cloneFixture(uiApiFixtures.repo.pathsInfo));
        },
      ),
    );

    const wrapper = mountViewer({ currentPath: "catalog" });

    await flushPromises();
    await flushPromises();

    expect(requests.tree).toEqual([
      {
        name: "hierarchy-crawl-fixtures",
        path: "/catalog",
        // The file list now requests one page at a time. The page-size
        // pref defaults to 50 (utils/repo-list-pagination.js); paginated
        // listing reduces the wire volume on large directories without
        // changing how the SPA reads back individual entries.
        params: { recursive: "false", limit: "50" },
      },
    ]);
    expect(requests.pathsInfo).toEqual([
      {
        name: "hierarchy-crawl-fixtures",
        paths: ["catalog/section-01"],
        expand: "true",
      },
    ]);

    const row = wrapper
      .findAll('[class*="cursor-pointer"]')
      .find((node) => node.text().includes("section-01"));
    expect(row).toBeTruthy();
    expect(wrapper.text()).toContain("Add section summary");

    const commitLink = wrapper
      .findAll('a[data-router-link="true"]')
      .find(
        (node) =>
          node.attributes("href") ===
          "/datasets/open-media-lab/hierarchy-crawl-fixtures/commit/commit-1",
      );
    expect(commitLink).toBeTruthy();

    // Directory rows render as a stretched <RouterLink> overlay so the
    // browser treats them as real anchors (right-click / middle-click /
    // Cmd-click all work). Assert the overlay href matches the tree
    // route for the directory.
    const rowLink = row.find('[data-testid="filelist-row-link"]');
    expect(rowLink.exists()).toBe(true);
    expect(rowLink.attributes("href")).toBe(
      "/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main/catalog/section-01",
    );
  });

  it("sorts directories before files and orders same-type paths alphabetically", async () => {
    server.use(
      http.get(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main/catalog",
        () =>
          jsonResponse([
            {
              type: "file",
              path: "catalog/z-last.txt",
              size: 4,
              lastModified: "2026-04-21T13:53:39.000000Z",
            },
            {
              type: "directory",
              path: "catalog/b-section",
              size: 0,
              lastModified: "2026-04-21T13:53:39.000000Z",
            },
            {
              type: "file",
              path: "catalog/a-first.txt",
              size: 2,
              lastModified: "2026-04-21T13:53:39.000000Z",
            },
            {
              type: "directory",
              path: "catalog/a-section",
              size: 0,
              lastModified: "2026-04-21T13:53:39.000000Z",
            },
          ]),
      ),
      http.post(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/paths-info/main",
        () => jsonResponse([]),
      ),
    );

    const wrapper = mountViewer({ currentPath: "catalog" });

    await flushPromises();
    await flushPromises();

    const rowNames = wrapper
      .findAll('[class*="cursor-pointer"] .font-medium.truncate')
      .map((node) => node.text());

    expect(rowNames).toEqual([
      "a-section",
      "b-section",
      "a-first.txt",
      "z-last.txt",
    ]);
  });

  it("keeps repo-root file navigation working when expanded path info fails", async () => {
    server.use(
      http.get(
        "/api/datasets/open-media-lab/table-scan-fixtures/tree/main/metadata",
        () =>
          jsonResponse([
            {
              type: "file",
              path: "metadata/features.json",
              size: 42,
              lastModified: "2026-04-21T13:53:39.000000Z",
            },
          ]),
      ),
      http.post(
        "/api/datasets/open-media-lab/table-scan-fixtures/paths-info/main",
        () => jsonResponse({ detail: "expand failed" }, { status: 500 }),
      ),
    );

    const wrapper = mountViewer({
      name: "table-scan-fixtures",
      currentPath: "metadata",
    });

    await flushPromises();
    await flushPromises();

    const row = wrapper
      .findAll('[class*="cursor-pointer"]')
      .find((node) => node.text().includes("features.json"));
    expect(row).toBeTruthy();

    // File rows point the stretched RouterLink at the blob route.
    const rowLink = row.find('[data-testid="filelist-row-link"]');
    expect(rowLink.exists()).toBe(true);
    expect(rowLink.attributes("href")).toBe(
      "/datasets/open-media-lab/table-scan-fixtures/blob/main/metadata/features.json",
    );
  });

  it("renders every file-list row as a real <a> anchor so right-click → Open in New Tab works", async () => {
    // Mix of nested subdirectories + files with and without lastCommit
    // to exercise the directory-route vs blob-route branch and confirm
    // the RouterLink overlay is present on every row regardless of
    // entry type or metadata shape.
    server.use(
      http.get(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main/catalog",
        () =>
          jsonResponse([
            {
              type: "directory",
              path: "catalog/sub-dir",
              size: 0,
              lastModified: "2026-04-21T13:53:39.000000Z",
            },
            {
              type: "file",
              path: "catalog/weights.safetensors",
              size: 1024,
              lastModified: "2026-04-21T13:53:39.000000Z",
            },
            {
              type: "file",
              path: "catalog/notes.md",
              size: 64,
              lastModified: "2026-04-21T13:53:39.000000Z",
              lastCommit: {
                id: "abc123",
                title: "Add notes",
                date: "2026-04-21T13:53:39.000000Z",
              },
            },
          ]),
      ),
      http.post(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/paths-info/main",
        () => jsonResponse([]),
      ),
    );

    const wrapper = mountViewer({ currentPath: "catalog" });
    await flushPromises();
    await flushPromises();

    const rowLinks = wrapper.findAll('[data-testid="filelist-row-link"]');
    const hrefs = rowLinks.map((n) => n.attributes("href")).sort();
    expect(hrefs).toEqual(
      [
        "/datasets/open-media-lab/hierarchy-crawl-fixtures/blob/main/catalog/notes.md",
        "/datasets/open-media-lab/hierarchy-crawl-fixtures/blob/main/catalog/weights.safetensors",
        "/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main/catalog/sub-dir",
      ].sort(),
    );

    // Every overlay is an <a> element — that is what makes the native
    // browser context menu offer "Open link in new tab".
    for (const link of rowLinks) {
      expect(link.element.tagName).toBe("A");
      expect(link.attributes("aria-label")).toMatch(/^Open /);
    }

    // The preview button and commit-row RouterLink must NOT trigger a
    // navigation of the stretched row link — their clicks are
    // swallowed by @click.stop and their z-index keeps them on top.
    const previewBtn = wrapper
      .findAll("button")
      .find((b) => (b.attributes("aria-label") || "").startsWith("Preview"));
    if (previewBtn) {
      await previewBtn.trigger("click");
      // click on the preview button should NOT have navigated via router.push
      expect(mocks.router.push).not.toHaveBeenCalledWith(
        expect.stringMatching(/\/blob\/|\/tree\//),
      );
    }
  });

  it("skips expanded path loading for empty trees and clears the tree when loading fails", async () => {
    let requestCount = 0;
    server.use(
      http.get(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main/:path",
        ({ params }) => {
        requestCount += 1;
        if (requestCount === 1) {
          return jsonResponse([]);
        }
        return jsonResponse({ detail: "tree failed" }, { status: 500 });
        },
      ),
    );

    const emptyWrapper = mountViewer({ currentPath: "catalog" });

    await flushPromises();
    await flushPromises();

    expect(emptyWrapper.text()).toContain("No files found");
    expect(requests.pathsInfo).toEqual([]);

    const failedWrapper = mountViewer({ currentPath: "catalog-next" });

    await flushPromises();
    await flushPromises();

    expect(failedWrapper.findAll('[class*="cursor-pointer"]')).toHaveLength(0);
    expect(requests.pathsInfo).toEqual([]);
  });

  it("ignores stale tree responses after the current path changes", async () => {
    const firstTree = deferred();
    const secondTree = deferred();

    server.use(
      http.get(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main/:path",
        async ({ params }) => {
        if (params.path === "catalog") {
          const payload = await firstTree.promise;
          return jsonResponse(payload);
        }
        const payload = await secondTree.promise;
        return jsonResponse(payload);
        },
      ),
      http.post(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/paths-info/main",
        async ({ request }) => {
          const form = new URLSearchParams(await request.clone().text());
          requests.pathsInfo.push({
            name: "hierarchy-crawl-fixtures",
            paths: form.getAll("paths"),
            expand: form.get("expand"),
          });
          return jsonResponse([
            { type: "file", path: "catalog-next/new.txt", size: 1 },
          ]);
        },
      ),
    );

    const wrapper = mountViewer({ currentPath: "catalog" });

    await flushPromises();
    await wrapper.setProps({ currentPath: "catalog-next" });

    secondTree.resolve([
      {
        type: "file",
        path: "catalog-next/new.txt",
        size: 1,
        lastModified: "2026-04-21T13:53:39.000000Z",
      },
    ]);
    await flushPromises();
    await flushPromises();

    firstTree.resolve([
      {
        type: "file",
        path: "catalog/old.txt",
        size: 1,
        lastModified: "2026-04-21T13:53:39.000000Z",
      },
    ]);
    await flushPromises();
    await flushPromises();

    expect(wrapper.text()).toContain("new.txt");
    expect(wrapper.text()).not.toContain("old.txt");
    expect(requests.pathsInfo).toEqual([
      {
        name: "hierarchy-crawl-fixtures",
        paths: ["catalog-next/new.txt"],
        expand: "true",
      },
    ]);
  });

  it("ignores stale expanded path info responses after a newer request wins", async () => {
    const firstPathsInfo = deferred();

    server.use(
      http.get(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main/:path",
        ({ params }) => {
        if (params.path === "catalog") {
          return jsonResponse([
            {
              type: "file",
              path: "catalog/old.txt",
              size: 1,
              lastModified: "2026-04-21T13:53:39.000000Z",
            },
          ]);
        }
        return jsonResponse([
          {
            type: "file",
            path: "catalog-next/new.txt",
            size: 1,
            lastModified: "2026-04-21T13:53:39.000000Z",
          },
        ]);
        },
      ),
      http.post(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/paths-info/main",
        async ({ request }) => {
          const form = new URLSearchParams(await request.clone().text());
          const paths = form.getAll("paths");
          if (paths[0] === "catalog/old.txt") {
            const payload = await firstPathsInfo.promise;
            return jsonResponse(payload);
          }
          return jsonResponse([
            {
              type: "file",
              path: "catalog-next/new.txt",
              size: 3,
              lastCommit: {
                id: "commit-2",
                title: "Ship new tree row",
                date: "2026-04-21T13:53:39.000000Z",
              },
            },
          ]);
        },
      ),
    );

    const wrapper = mountViewer({ currentPath: "catalog" });

    await flushPromises();
    await flushPromises();

    await wrapper.setProps({ currentPath: "catalog-next" });
    await flushPromises();
    await flushPromises();

    firstPathsInfo.resolve([
      {
        type: "file",
        path: "catalog/old.txt",
        size: 99,
        lastCommit: {
          id: "commit-1",
          title: "Old tree row",
          date: "2026-04-21T13:53:39.000000Z",
        },
      },
    ]);
    await flushPromises();
    await flushPromises();

    expect(wrapper.text()).toContain("new.txt");
    expect(wrapper.text()).toContain("Ship new tree row");
    expect(wrapper.text()).not.toContain("Old tree row");
  });

  it("renders an ErrorState panel when the root tree fetch fails (gated fallback)", async () => {
    // Backend replies with the shape build_aggregate_failure_response
    // emits on an all-auth aggregate: 401 + X-Error-Code=GatedRepo +
    // sources[] in the JSON body. RepoViewer should classify via the
    // axios interceptor and render ErrorState where the file list
    // would otherwise sit — NOT silently show an empty grid.
    server.use(
      http.get(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main",
        () =>
          jsonResponse(
            {
              error: "GatedRepo",
              detail:
                "Upstream source requires authentication - likely a gated repository.",
              sources: [
                {
                  name: "HuggingFace",
                  url: "https://huggingface.co",
                  status: 401,
                  category: "auth",
                  message: "Access restricted",
                },
              ],
            },
            {
              status: 401,
              headers: {
                "X-Error-Code": "GatedRepo",
                "X-Error-Message":
                  "Upstream source requires authentication - likely a gated repository.",
              },
            },
          ),
      ),
    );

    const wrapper = mountViewer();
    await flushPromises();
    await flushPromises();

    // Shared ErrorState's default copy for `gated`.
    expect(wrapper.text()).toContain("Authentication required");
    // Diagnostic disclosure from the sources[] body. The per-row
    // cell content lives inside the ElTable stub's scoped-slot
    // rendering, which isn't exercised at this mount's stubs; the
    // count is enough to prove we plumbed sources through.
    expect(wrapper.text()).toContain("Fallback sources tried (1)");
    // The misleading "No files" / empty-tree copy must NOT be shown
    // once we have a classified error.
    expect(wrapper.text()).not.toContain("No files");

    wrapper.unmount();
  });

  it("paginates the file list: defaults to 50/page, advances via the cursor in Link rel=next, and walks back via First/Prev", async () => {
    // Two-page directory. Backend hands a `Link: rel=next` header with
    // a cursor on page 1; the SPA must request the same path with that
    // cursor for page 2 and stop following on the empty-cursor response.
    let page1Calls = 0;
    let page2Calls = 0;
    server.use(
      http.get(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main/catalog",
        ({ request }) => {
          const url = new URL(request.url);
          const cursor = url.searchParams.get("cursor");
          const limit = url.searchParams.get("limit");
          expect(limit).toBe("50");
          if (!cursor) {
            page1Calls += 1;
            return jsonResponse(
              [
                {
                  type: "file",
                  path: "catalog/a-first.txt",
                  size: 1,
                  lastModified: "2026-04-21T13:53:39.000000Z",
                },
              ],
              {
                headers: {
                  Link: '<https://hub.test/api/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main/catalog?recursive=false&limit=50&cursor=cursor-2>; rel="next"',
                },
              },
            );
          }
          expect(cursor).toBe("cursor-2");
          page2Calls += 1;
          return jsonResponse([
            {
              type: "file",
              path: "catalog/z-last.txt",
              size: 2,
              lastModified: "2026-04-21T13:53:39.000000Z",
            },
          ]);
        },
      ),
      http.post(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/paths-info/main",
        () => jsonResponse([]),
      ),
    );

    const wrapper = mountViewer({ currentPath: "catalog" });
    await flushPromises();
    await flushPromises();
    await flushPromises();

    expect(page1Calls).toBeGreaterThanOrEqual(1);
    expect(wrapper.find('[data-testid="file-list-pager"]').exists()).toBe(true);
    expect(wrapper.text()).toContain("a-first.txt");
    expect(wrapper.text()).not.toContain("z-last.txt");
    expect(wrapper.text()).toContain("Page 1");

    // Background discovery walked forward and confirmed page 2 is the
    // tail; the pager's Next + page-2 buttons are now usable.
    const nextBtn = wrapper.find('[data-el-pagination-next="true"]');
    expect(nextBtn.exists()).toBe(true);
    await nextBtn.trigger("click");
    await flushPromises();
    await flushPromises();

    expect(page2Calls).toBeGreaterThanOrEqual(1);
    expect(wrapper.text()).toContain("z-last.txt");
    expect(wrapper.text()).not.toContain("a-first.txt");
    expect(wrapper.text()).toContain("Page 2");
    // Page 2 was the tail (no Link rel=next) and discovery confirmed
    // it — Next disables itself.
    expect(
      wrapper
        .find('[data-el-pagination-next="true"]')
        .attributes("disabled"),
    ).toBeDefined();

    // Prev re-issues the cursor-less first-page request (backend can't
    // seek backwards, so we always re-fetch).
    await wrapper.find('[data-el-pagination-prev="true"]').trigger("click");
    await flushPromises();
    await flushPromises();
    expect(wrapper.text()).toContain("a-first.txt");
    expect(wrapper.text()).toContain("Page 1");

    wrapper.unmount();
  });

  it("changing the page-size selector resets to page 1, persists the choice in localStorage, and re-fetches with the new limit", async () => {
    const observed = [];
    server.use(
      http.get(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main/catalog",
        ({ request }) => {
          const url = new URL(request.url);
          const cursor = url.searchParams.get("cursor");
          observed.push({
            limit: url.searchParams.get("limit"),
            cursor,
          });
          if (!cursor) {
            return jsonResponse(
              [
                {
                  type: "file",
                  path: "catalog/a-first.txt",
                  size: 1,
                  lastModified: "2026-04-21T13:53:39.000000Z",
                },
              ],
              {
                headers: {
                  Link: '<https://hub.test/api?cursor=cursor-2>; rel="next"',
                },
              },
            );
          }
          // Tail page — no Link header so background discovery
          // terminates instead of looping the same cursor forever.
          return jsonResponse([
            {
              type: "file",
              path: "catalog/z-last.txt",
              size: 1,
              lastModified: "2026-04-21T13:53:39.000000Z",
            },
          ]);
        },
      ),
      http.post(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/paths-info/main",
        () => jsonResponse([]),
      ),
    );

    const wrapper = mountViewer({ currentPath: "catalog" });
    await flushPromises();
    await flushPromises();

    // Default first-page request uses limit=50, no cursor.
    expect(observed[0]).toEqual({ limit: "50", cursor: null });

    const select = wrapper.find('[data-testid="file-list-page-size"]');
    expect(select.exists()).toBe(true);

    // ElSelect renders as a stub here — drive the change handler
    // directly (the el-select stub doesn't render real <option>s in
    // jsdom). Mirrors how page-size widgets are exercised elsewhere
    // in the suite.
    const componentVm = wrapper.vm;
    componentVm.changeFileListPageSize(100);
    await flushPromises();
    await flushPromises();

    // New foreground fetch lands with the new limit AND no cursor —
    // switching size resets pagination because the previously-discovered
    // cursors were minted at the old page size and don't address the
    // same slice anymore. (The very last call is the discovery walk's
    // page-2 probe; we want the first call AFTER the size change.)
    const postChange = observed.find(
      (entry, idx) =>
        idx > 0 && entry.limit === "100" && entry.cursor === null,
    );
    expect(postChange).toBeDefined();
    expect(localStorage.getItem("kohaku-repo-file-list-page-size")).toBe(
      "100",
    );

    wrapper.unmount();
  });

  it("First / Last / page-N / jumper navigate a multi-page directory through discovered cursors", async () => {
    // Three-page directory; the SPA's discovery walks forward in the
    // background after the initial page-1 fetch so the pager can
    // surface a real total + numbered page buttons + a Last button.
    let calls = 0;
    server.use(
      http.get(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main/catalog",
        ({ request }) => {
          calls += 1;
          const url = new URL(request.url);
          const cursor = url.searchParams.get("cursor");
          if (!cursor) {
            return jsonResponse(
              [
                {
                  type: "file",
                  path: "catalog/a.txt",
                  size: 1,
                  lastModified: "2026-04-21T13:53:39.000000Z",
                },
              ],
              {
                headers: {
                  Link: '<https://hub.test/api?cursor=cursor-2>; rel="next"',
                },
              },
            );
          }
          if (cursor === "cursor-2") {
            return jsonResponse(
              [
                {
                  type: "file",
                  path: "catalog/m.txt",
                  size: 1,
                  lastModified: "2026-04-21T13:53:39.000000Z",
                },
              ],
              {
                headers: {
                  Link: '<https://hub.test/api?cursor=cursor-3>; rel="next"',
                },
              },
            );
          }
          return jsonResponse([
            {
              type: "file",
              path: "catalog/z.txt",
              size: 1,
              lastModified: "2026-04-21T13:53:39.000000Z",
            },
          ]);
        },
      ),
      http.post(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/paths-info/main",
        () => jsonResponse([]),
      ),
    );

    const wrapper = mountViewer({ currentPath: "catalog" });
    // Page 1 fetch + discovery walk + paths-info — flush enough times
    // for all of them to settle before assertions read the pager.
    await flushPromises();
    await flushPromises();
    await flushPromises();
    await flushPromises();

    // Discovery has confirmed all three pages.
    const pagerEl = wrapper.find('[data-el-pagination="true"]');
    expect(pagerEl.attributes("data-page-count")).toBe("3");

    // Numbered button → page 3 (== Last button equivalent).
    await wrapper.find('[data-el-pagination-page="3"]').trigger("click");
    await flushPromises();
    await flushPromises();
    expect(wrapper.text()).toContain("z.txt");
    expect(wrapper.text()).toContain("Page 3");
    expect(
      wrapper
        .find('[data-testid="file-list-page-last"]')
        .attributes("disabled"),
    ).toBeDefined();

    // First button rewinds without intermediate hops.
    await wrapper.find('[data-testid="file-list-page-first"]').trigger("click");
    await flushPromises();
    await flushPromises();
    expect(wrapper.text()).toContain("a.txt");
    expect(wrapper.text()).toContain("Page 1");

    // Jumper input → direct goto-page-N. Driving the stub's <input>
    // mirrors what the real ElPagination jumper does: a Number()
    // change emits `current-change` with the entered page.
    await wrapper
      .find('[data-el-pagination-jumper="true"]')
      .setValue("2");
    await flushPromises();
    await flushPromises();
    expect(wrapper.text()).toContain("m.txt");
    expect(wrapper.text()).toContain("Page 2");

    // Last button at this point goes to page 3 (end is discovered).
    await wrapper.find('[data-testid="file-list-page-last"]').trigger("click");
    await flushPromises();
    await flushPromises();
    expect(wrapper.text()).toContain("z.txt");
    expect(wrapper.text()).toContain("Page 3");

    wrapper.unmount();
  });

  it("jumper input that targets a not-yet-discovered page extends discovery before navigating", async () => {
    // Three-page directory; we only flush enough for page 1 to land,
    // then drive the jumper to page 3. goToFileListPage should walk
    // forward synchronously (`extendFileListDiscoveryTo`) and then
    // navigate — i.e. the user is not blocked on background discovery
    // having finished first.
    const calls = [];
    server.use(
      http.get(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main/catalog",
        ({ request }) => {
          const url = new URL(request.url);
          const cursor = url.searchParams.get("cursor");
          calls.push(cursor);
          if (!cursor) {
            return jsonResponse(
              [
                {
                  type: "file",
                  path: "catalog/a.txt",
                  size: 1,
                  lastModified: "2026-04-21T13:53:39.000000Z",
                },
              ],
              {
                headers: {
                  Link: '<https://hub.test/api?cursor=cursor-2>; rel="next"',
                },
              },
            );
          }
          if (cursor === "cursor-2") {
            return jsonResponse(
              [
                {
                  type: "file",
                  path: "catalog/m.txt",
                  size: 1,
                  lastModified: "2026-04-21T13:53:39.000000Z",
                },
              ],
              {
                headers: {
                  Link: '<https://hub.test/api?cursor=cursor-3>; rel="next"',
                },
              },
            );
          }
          return jsonResponse([
            {
              type: "file",
              path: "catalog/z.txt",
              size: 1,
              lastModified: "2026-04-21T13:53:39.000000Z",
            },
          ]);
        },
      ),
      http.post(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/paths-info/main",
        () => jsonResponse([]),
      ),
    );

    const wrapper = mountViewer({ currentPath: "catalog" });
    await flushPromises();
    await flushPromises();
    await flushPromises();
    await flushPromises();

    await wrapper
      .find('[data-el-pagination-jumper="true"]')
      .setValue("3");
    await flushPromises();
    await flushPromises();
    await flushPromises();

    expect(wrapper.text()).toContain("z.txt");
    expect(wrapper.text()).toContain("Page 3");

    wrapper.unmount();
  });

  it("Last button kicks off a synchronous discovery walk when the background pass has not finished yet", async () => {
    // Background discovery is gated on a never-resolved promise so it
    // never reaches the end on its own. Last must run its own walk,
    // which uses the same fetch path — so we open a second gate the
    // foreground walk can release. Two pages; the second page is the
    // tail (no Link header) once unblocked.
    let unblockBackground;
    const backgroundGate = new Promise((resolve) => {
      unblockBackground = resolve;
    });
    let backgroundCalls = 0;

    server.use(
      http.get(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main/catalog",
        async ({ request }) => {
          const url = new URL(request.url);
          const cursor = url.searchParams.get("cursor");
          if (!cursor) {
            return jsonResponse(
              [
                {
                  type: "file",
                  path: "catalog/a.txt",
                  size: 1,
                  lastModified: "2026-04-21T13:53:39.000000Z",
                },
              ],
              {
                headers: {
                  Link: '<https://hub.test/api?cursor=cursor-2>; rel="next"',
                },
              },
            );
          }
          backgroundCalls += 1;
          if (backgroundCalls === 1) {
            // First (background) hop hangs to keep endDiscovered=false.
            await backgroundGate;
          }
          // Tail page (no Link) on whatever call happens to win.
          return jsonResponse([
            {
              type: "file",
              path: "catalog/z.txt",
              size: 1,
              lastModified: "2026-04-21T13:53:39.000000Z",
            },
          ]);
        },
      ),
      http.post(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/paths-info/main",
        () => jsonResponse([]),
      ),
    );

    const wrapper = mountViewer({ currentPath: "catalog" });
    await flushPromises();
    await flushPromises();

    // Last should still be disabled — endDiscovered hasn't been
    // observed yet.
    expect(
      wrapper
        .find('[data-testid="file-list-page-last"]')
        .attributes("disabled"),
    ).toBeDefined();

    // Drive the goToLastFileListPage handler directly (the rendered
    // button is `disabled` while endDiscovered=false, so a real
    // .click() is dropped at the DOM layer; the user-visible affordance
    // arrives via the keyboard shortcut / jumper, which both call the
    // same function). Release the background gate first so the
    // runId-superseded loop unblocks and exits.
    unblockBackground();
    await wrapper.vm.goToLastFileListPage();
    await flushPromises();
    await flushPromises();
    await flushPromises();

    expect(wrapper.text()).toContain("z.txt");
    expect(wrapper.text()).toContain("Page 2");

    wrapper.unmount();
  });

  it("background discovery silently swallows a transient fetch error so the foreground listing still renders", async () => {
    // Discovery's error handler must NOT surface anything to the
    // user — page 1 still loads, the pager shows what it knows.
    let bgCalls = 0;
    server.use(
      http.get(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main/catalog",
        ({ request }) => {
          const url = new URL(request.url);
          const cursor = url.searchParams.get("cursor");
          if (!cursor) {
            return jsonResponse(
              [
                {
                  type: "file",
                  path: "catalog/a.txt",
                  size: 1,
                  lastModified: "2026-04-21T13:53:39.000000Z",
                },
              ],
              {
                headers: {
                  Link: '<https://hub.test/api?cursor=cursor-2>; rel="next"',
                },
              },
            );
          }
          bgCalls += 1;
          return jsonResponse({ detail: "discovery probe failed" }, { status: 503 });
        },
      ),
      http.post(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/paths-info/main",
        () => jsonResponse([]),
      ),
    );

    const wrapper = mountViewer({ currentPath: "catalog" });
    await flushPromises();
    await flushPromises();
    await flushPromises();

    expect(bgCalls).toBeGreaterThanOrEqual(1);
    // Page 1 is rendered; no error panel.
    expect(wrapper.text()).toContain("a.txt");
    expect(wrapper.text()).not.toContain("Authentication required");

    wrapper.unmount();
  });

  it("Last button stays disabled and the 'discovering more…' hint shows while the cursor walk is still in flight", async () => {
    // We block the discovery walk on its second hop by handing it a
    // Promise we never resolve, so the SPA stays in the "page 1
    // loaded; end not confirmed yet" state for the assertion. Page 1
    // resolves immediately so the foreground load completes; only
    // the cursor=cursor-2 call hangs.
    let releaseDiscovery;
    const discoveryGate = new Promise((resolve) => {
      releaseDiscovery = resolve;
    });

    server.use(
      http.get(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main/catalog",
        async ({ request }) => {
          const url = new URL(request.url);
          const cursor = url.searchParams.get("cursor");
          if (!cursor) {
            return jsonResponse(
              [
                {
                  type: "file",
                  path: "catalog/a.txt",
                  size: 1,
                  lastModified: "2026-04-21T13:53:39.000000Z",
                },
              ],
              {
                headers: {
                  Link: '<https://hub.test/api?cursor=cursor-2>; rel="next"',
                },
              },
            );
          }
          await discoveryGate;
          return jsonResponse([]);
        },
      ),
      http.post(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/paths-info/main",
        () => jsonResponse([]),
      ),
    );

    const wrapper = mountViewer({ currentPath: "catalog" });
    await flushPromises();
    await flushPromises();

    expect(
      wrapper
        .find('[data-testid="file-list-page-last"]')
        .attributes("disabled"),
    ).toBeDefined();
    expect(
      wrapper.find('[data-testid="file-list-discovery-hint"]').exists(),
    ).toBe(true);

    // Release the discovery hop and tear down so the test cleans up
    // without leaking the dangling fetch into the next case.
    releaseDiscovery();
    await flushPromises();
    wrapper.unmount();
  });

  it("paginated empty-search placeholder reads 'No files match \"…\" on this page', not the bare 'No files found'", async () => {
    server.use(
      http.get(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main",
        () =>
          jsonResponse([
            {
              type: "file",
              path: "alpha.txt",
              size: 1,
              lastModified: "2026-04-21T13:53:39.000000Z",
            },
            {
              type: "file",
              path: "beta.txt",
              size: 1,
              lastModified: "2026-04-21T13:53:39.000000Z",
            },
          ]),
      ),
      http.post(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/paths-info/main",
        () => jsonResponse([]),
      ),
    );

    const wrapper = mountViewer();
    await flushPromises();
    await flushPromises();

    // Search for something not on this page (since paginated search
    // only filters the current slice). The empty-state copy must
    // distinguish "current page" from "whole repo" so the user is
    // not misled into thinking the file does not exist at all.
    wrapper.vm.fileSearchQuery = "zeta";
    await flushPromises();
    expect(wrapper.text()).toContain('No files match "zeta" on this page');

    wrapper.unmount();
  });

  it("Card tab finds the README via paths-info when it is not on the current page", async () => {
    // The repo's root listing intentionally omits README.md (simulating
    // a paginated tree where README falls off this page); paths-info
    // must surface it and the Card tab still renders the markdown.
    let pathsInfoCalls = 0;
    server.use(
      http.get(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main",
        () =>
          jsonResponse([
            {
              type: "file",
              path: "alpha.json",
              size: 5,
              lastModified: "2026-04-21T13:53:39.000000Z",
            },
          ]),
      ),
      http.post(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/paths-info/main",
        async ({ request }) => {
          pathsInfoCalls += 1;
          const form = new URLSearchParams(await request.clone().text());
          const paths = form.getAll("paths");
          if (paths.includes("README.md")) {
            return jsonResponse([
              {
                type: "file",
                path: "README.md",
                size: 64,
                lastCommit: {
                  id: "c1",
                  title: "Add readme",
                  date: "2026-04-21T13:53:39.000000Z",
                },
              },
            ]);
          }
          return jsonResponse([]);
        },
      ),
      http.get(
        "/datasets/open-media-lab/hierarchy-crawl-fixtures/resolve/main/README.md",
        () =>
          new HttpResponse(
            "---\ntitle: Recovered\n---\n\n# Off-Page README\n",
            {
              status: 200,
              headers: { "Content-Type": "text/markdown" },
            },
          ),
      ),
    );

    const wrapper = mountViewer({ tab: "card" });
    await flushPromises();
    await flushPromises();
    await flushPromises();

    expect(pathsInfoCalls).toBeGreaterThan(0);
    // MarkdownViewer is stubbed in this test mount, so the rendered
    // markdown body is not in wrapper.text(). We assert the recovery
    // path negatively: the empty placeholder is gone, which only
    // happens when readmeContent was populated by the probe + fetch
    // chain. (The structurally-identical test in
    // FilePreviewDialog.spec covers the actual markdown render.)
    expect(wrapper.text()).not.toContain("No README.md found");

    wrapper.unmount();
  });

  it("README paths-info probe failure degrades gracefully to the empty-readme state", async () => {
    server.use(
      http.get(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main",
        () => jsonResponse([]),
      ),
      http.post(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/paths-info/main",
        () =>
          jsonResponse(
            { detail: "paths-info upstream offline" },
            { status: 502 },
          ),
      ),
    );

    const wrapper = mountViewer({ tab: "card" });
    await flushPromises();
    await flushPromises();
    await flushPromises();

    // Soft-fallback: the probe failure does not throw; the Card tab
    // ends up with no README and renders the regular empty copy.
    expect(wrapper.text()).not.toContain("Authentication required");
    expect(wrapper.text()).not.toContain("undefined");

    wrapper.unmount();
  });

  it("indexed-tar sibling icon: lights up when the .json sibling exists on a different page and stays dark when HEAD says it does not", async () => {
    const headProbes = [];
    server.use(
      http.get(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main",
        () =>
          jsonResponse([
            {
              type: "file",
              path: "with-sibling.tar",
              size: 100,
              lastModified: "2026-04-21T13:53:39.000000Z",
            },
            {
              type: "file",
              path: "no-sibling.tar",
              size: 100,
              lastModified: "2026-04-21T13:53:39.000000Z",
            },
          ]),
      ),
      http.post(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/paths-info/main",
        () => jsonResponse([]),
      ),
      // HEAD probes mimic the backend: 200 for the existing sidecar,
      // 404 for the bare tar's missing sidecar.
      http.head(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/resolve/main/with-sibling.json",
        () => {
          headProbes.push("with-sibling.json");
          return new HttpResponse(null, { status: 200 });
        },
      ),
      http.head(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/resolve/main/no-sibling.json",
        () => {
          headProbes.push("no-sibling.json");
          return new HttpResponse(null, { status: 404 });
        },
      ),
    );

    const wrapper = mountViewer({ currentPath: "" });
    await flushPromises();
    await flushPromises();
    await flushPromises();

    // Both probes should have fired exactly once (one per .tar row
    // whose sibling is not in the loaded page).
    expect(headProbes.sort()).toEqual([
      "no-sibling.json",
      "with-sibling.json",
    ]);

    // The confirmed tar gets the indexed-tar icon (Carbon's archive),
    // the unconfirmed one stays bare.
    const previewButtons = wrapper.findAll(
      "button[aria-label^='Preview metadata for'], button[aria-label^='Preview metadata for ']",
    );
    // ElButton stubs render as <button>; iterate all of them and
    // collect the ones the icon predicate says are previewable.
    const allButtons = wrapper.findAll("button");
    const tarPreviewButtons = allButtons.filter((b) =>
      (b.attributes("aria-label") || "").startsWith("Preview metadata for"),
    );
    const titles = tarPreviewButtons.map((b) => b.attributes("title") || "");
    // One previewable .tar row only — the confirmed one.
    expect(tarPreviewButtons).toHaveLength(1);
    expect(titles[0]).toContain("Browse indexed tar contents");
    // unused alias kept to silence the linter when the future patch
    // adds a second selector — drop on the next touch.
    void previewButtons;

    wrapper.unmount();
  });

  it("indexed-tar sibling icon short-circuits when the .json is in the loaded page (no HEAD probe issued)", async () => {
    const headSpy = vi.fn(() => new HttpResponse(null, { status: 500 }));
    server.use(
      http.get(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main",
        () =>
          jsonResponse([
            {
              type: "file",
              path: "bundle.tar",
              size: 100,
              lastModified: "2026-04-21T13:53:39.000000Z",
            },
            {
              type: "file",
              path: "bundle.json",
              size: 50,
              lastModified: "2026-04-21T13:53:39.000000Z",
            },
          ]),
      ),
      http.post(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/paths-info/main",
        () => jsonResponse([]),
      ),
      http.head(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/resolve/main/bundle.json",
        headSpy,
      ),
    );

    const wrapper = mountViewer({ currentPath: "" });
    await flushPromises();
    await flushPromises();
    await flushPromises();

    // The icon should already be lit from the loaded-listing fast
    // path — and the probe must NOT fire, otherwise the cheap path is
    // doing wasted work on every paginated render.
    expect(headSpy).not.toHaveBeenCalled();
    const previewBtn = wrapper
      .findAll("button")
      .find((b) =>
        (b.attributes("aria-label") || "").startsWith("Preview metadata for bundle.tar"),
      );
    expect(previewBtn).toBeTruthy();
    expect(previewBtn.attributes("title")).toContain(
      "Browse indexed tar contents",
    );

    wrapper.unmount();
  });

  it("renders an ErrorState for a README that fails to resolve, not the 'No README.md found' placeholder", async () => {
    // Tree returns one entry — a README — so loadReadme() will try to
    // fetch it. That fetch returns a classified 401/GatedRepo body;
    // RepoViewer should show the ErrorState in the README slot
    // instead of the misleading "No README.md found" empty copy.
    server.use(
      http.get(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/tree/main",
        () =>
          jsonResponse([
            {
              type: "file",
              path: "README.md",
              size: 120,
              lastCommit: {
                id: "c1",
                title: "Add readme",
                date: "2026-04-21T13:53:39.000000Z",
              },
            },
          ]),
      ),
      http.post(
        "/api/datasets/open-media-lab/hierarchy-crawl-fixtures/paths-info/main",
        () => jsonResponse([]),
      ),
      // The README resolve itself fails with the aggregate-gated body.
      http.get(
        "/datasets/open-media-lab/hierarchy-crawl-fixtures/resolve/main/README.md",
        () =>
          jsonResponse(
            {
              error: "GatedRepo",
              detail:
                "Upstream source requires authentication - likely a gated repository.",
              sources: [
                {
                  name: "HF",
                  url: "https://hf",
                  status: 401,
                  category: "auth",
                  message: "Access restricted",
                },
              ],
            },
            {
              status: 401,
              headers: {
                "X-Error-Code": "GatedRepo",
                "X-Error-Message":
                  "Upstream source requires authentication - likely a gated repository.",
              },
            },
          ),
      ),
    );

    const wrapper = mountViewer({ tab: "card" });
    await flushPromises();
    await flushPromises();
    await flushPromises();

    const text = wrapper.text();
    expect(text).toContain("Authentication required");
    expect(text).not.toContain("No README.md found");

    wrapper.unmount();
  });
});
