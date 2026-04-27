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

    expect(page1Calls).toBe(1);
    expect(wrapper.find('[data-testid="file-list-pager"]').exists()).toBe(true);
    expect(wrapper.text()).toContain("a-first.txt");
    expect(wrapper.text()).not.toContain("z-last.txt");
    expect(wrapper.text()).toContain("Page 1");

    const nextBtn = wrapper.find('[data-testid="file-list-page-next"]');
    expect(nextBtn.exists()).toBe(true);
    await nextBtn.trigger("click");
    await flushPromises();
    await flushPromises();

    expect(page2Calls).toBe(1);
    expect(wrapper.text()).toContain("z-last.txt");
    expect(wrapper.text()).not.toContain("a-first.txt");
    expect(wrapper.text()).toContain("Page 2");
    // Page 2 was the tail (no Link rel=next), so Next disables itself.
    expect(
      wrapper.find('[data-testid="file-list-page-next"]').attributes("disabled"),
    ).toBeDefined();

    // Prev returns to page 1 reusing the stored stack (no extra fetch
    // beyond the page-1 reload — backend can't seek backward, so the UI
    // re-issues the cursor-less first-page request).
    await wrapper.find('[data-testid="file-list-page-prev"]').trigger("click");
    await flushPromises();
    await flushPromises();
    expect(page1Calls).toBe(2);
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
          observed.push({
            limit: url.searchParams.get("limit"),
            cursor: url.searchParams.get("cursor"),
          });
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

    // New fetch lands with the new limit AND no cursor — switching size
    // resets the stack because the previous cursors were minted at the
    // old page size and don't address the same slice anymore.
    const last = observed[observed.length - 1];
    expect(last).toEqual({ limit: "100", cursor: null });
    expect(localStorage.getItem("kohaku-repo-file-list-page-size")).toBe(
      "100",
    );

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
