import { defineComponent, h } from "vue";
import { flushPromises, mount } from "@vue/test-utils";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { ElementPlusStubs } from "../helpers/vue";

const mocks = vi.hoisted(() => ({
  router: { push: vi.fn() },
  adminStore: { token: "admin-token", logout: vi.fn() },
  api: {
    listRepositories: vi.fn(),
    getRepositoryDetails: vi.fn(),
    getRepositoryStorageBreakdown: vi.fn(),
    recalculateAllRepoStorage: vi.fn(),
    listCommits: vi.fn(),
    deleteRepositoryAdmin: vi.fn(),
    moveRepositoryAdmin: vi.fn(),
    squashRepositoryAdmin: vi.fn(),
    getSiteConfig: vi.fn(),
  },
}));

vi.mock("vue-router", () => ({
  useRouter: () => mocks.router,
}));

vi.mock("@/stores/admin", () => ({
  useAdminStore: () => mocks.adminStore,
}));

vi.mock("@/utils/api", () => ({
  ...mocks.api,
  formatBytes: (value) => String(value ?? 0),
}));

vi.mock("@/components/AdminLayout.vue", () => ({
  default: defineComponent({
    setup(_, { slots }) {
      return () => h("main", slots.default ? slots.default() : []);
    },
  }),
}));

vi.mock("@/components/FileTree.vue", () => ({
  default: defineComponent({
    setup() {
      return () => h("div");
    },
  }),
}));

import RepositoriesPage from "@/pages/repositories.vue";

function mountPage() {
  return mount(RepositoriesPage, {
    global: { stubs: ElementPlusStubs },
  });
}

function enabledConfig(squash) {
  return {
    capabilities: {
      repository_operations: { revert: false, reset: false, squash },
    },
  };
}

describe("admin repository operation capability consumer", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.adminStore.token = "admin-token";
    mocks.api.listRepositories.mockResolvedValue({
      repositories: [
        {
          id: 1,
          repo_type: "model",
          namespace: "owner",
          name: "demo",
          full_id: "owner/demo",
          owner_username: "owner",
          used_bytes: 0,
          created_at: "2026-01-01T00:00:00Z",
        },
      ],
      total: 1,
    });
    mocks.api.getRepositoryDetails.mockResolvedValue({
      repo_type: "model",
      namespace: "owner",
      name: "demo",
      full_id: "owner/demo",
    });
    mocks.api.getSiteConfig.mockResolvedValue(enabledConfig(false));
  });

  async function openRepositoryDetails() {
    const wrapper = mountPage();
    await flushPromises();
    await wrapper
      .findAll("button")
      .find((button) => button.text() === "View Details")
      .trigger("click");
    await flushPromises();
    return wrapper;
  }

  it("hides squash when capability loading fails", async () => {
    mocks.api.getSiteConfig.mockRejectedValueOnce(new Error("offline"));

    const wrapper = await openRepositoryDetails();

    expect(wrapper.text()).not.toContain("Squash Repository");
    expect(mocks.api.squashRepositoryAdmin).not.toHaveBeenCalled();
  });

  it("shows squash only for an explicit true capability", async () => {
    mocks.api.getSiteConfig.mockResolvedValueOnce(enabledConfig(true));

    const wrapper = await openRepositoryDetails();

    expect(wrapper.text()).toContain("Squash Repository");
  });
});
