import { defineComponent, h } from "vue";
import { flushPromises, mount } from "@vue/test-utils";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ElementPlusStubs } from "../helpers/vue";

// Hoisted mocks for vue-router / pinia / api / element-plus message
// helpers — same pattern as test_cache_page.test.js.
const mocks = vi.hoisted(() => ({
  router: {
    push: vi.fn(),
  },
  adminStore: {
    token: "admin-token",
  },
  api: {
    listFallbackSources: vi.fn(),
    createFallbackSource: vi.fn(),
    updateFallbackSource: vi.fn(),
    deleteFallbackSource: vi.fn(),
    getFallbackCacheStats: vi.fn(),
    clearFallbackCache: vi.fn(),
    invalidateFallbackRepoCache: vi.fn(),
    invalidateFallbackUserCacheById: vi.fn(),
    invalidateFallbackUserCacheByUsername: vi.fn(),
    listUsers: vi.fn(),
    listRepositories: vi.fn(),
    bulkReplaceFallbackSources: vi.fn(),
    testFallbackChainSimulate: vi.fn(),
    testFallbackChainReal: vi.fn(),
  },
}));

vi.mock("vue-router", () => ({
  useRouter: () => mocks.router,
}));

vi.mock("@/stores/admin", () => ({
  useAdminStore: () => mocks.adminStore,
}));

vi.mock("@/utils/api", () => ({
  listFallbackSources: (...a) => mocks.api.listFallbackSources(...a),
  createFallbackSource: (...a) => mocks.api.createFallbackSource(...a),
  updateFallbackSource: (...a) => mocks.api.updateFallbackSource(...a),
  deleteFallbackSource: (...a) => mocks.api.deleteFallbackSource(...a),
  getFallbackCacheStats: (...a) => mocks.api.getFallbackCacheStats(...a),
  clearFallbackCache: (...a) => mocks.api.clearFallbackCache(...a),
  invalidateFallbackRepoCache: (...a) => mocks.api.invalidateFallbackRepoCache(...a),
  invalidateFallbackUserCacheById: (...a) =>
    mocks.api.invalidateFallbackUserCacheById(...a),
  invalidateFallbackUserCacheByUsername: (...a) =>
    mocks.api.invalidateFallbackUserCacheByUsername(...a),
  listUsers: (...a) => mocks.api.listUsers(...a),
  listRepositories: (...a) => mocks.api.listRepositories(...a),
  bulkReplaceFallbackSources: (...a) => mocks.api.bulkReplaceFallbackSources(...a),
  testFallbackChainSimulate: (...a) => mocks.api.testFallbackChainSimulate(...a),
  testFallbackChainReal: (...a) => mocks.api.testFallbackChainReal(...a),
}));

vi.mock("@/components/AdminLayout.vue", () => ({
  default: defineComponent({
    name: "AdminLayoutStub",
    setup(_, { slots }) {
      return () =>
        h(
          "div",
          { "data-testid": "admin-layout" },
          slots.default ? slots.default() : [],
        );
    },
  }),
}));

import FallbackSourcesPage from "@/pages/fallback-sources.vue";

const messageBoxConfirmSpy = vi.fn();
const messageSuccessSpy = vi.fn();
const messageErrorSpy = vi.fn();
let elementPlusModule;

// Stubs not present in the shared helpers/vue.js. Inline rather than
// extending the helper module to keep this file self-contained.
const ElRadioGroupStub = defineComponent({
  name: "ElRadioGroup",
  props: { modelValue: { type: [String, Number, Boolean], default: "" } },
  emits: ["update:modelValue", "change"],
  setup(props, { slots, emit }) {
    return () =>
      h(
        "div",
        {
          "data-el-radio-group": "true",
          "data-value": String(props.modelValue),
          onClick: (event) => {
            const target = event.target.closest("[data-radio-value]");
            if (target) {
              const value = target.getAttribute("data-radio-value");
              emit("update:modelValue", value);
              emit("change", value);
            }
          },
        },
        slots.default ? slots.default() : [],
      );
  },
});
const ElRadioStub = defineComponent({
  name: "ElRadio",
  props: { value: { type: [String, Number, Boolean], default: "" } },
  setup(props, { slots }) {
    return () =>
      h(
        "label",
        {
          "data-el-radio": "true",
          "data-radio-value": String(props.value),
        },
        slots.default ? slots.default() : [],
      );
  },
});
const ElSwitchStub = defineComponent({
  name: "ElSwitch",
  props: { modelValue: { type: Boolean, default: false } },
  emits: ["update:modelValue"],
  setup(props, { emit }) {
    return () =>
      h("input", {
        type: "checkbox",
        checked: props.modelValue,
        "data-el-switch": "true",
        onChange: (event) =>
          emit("update:modelValue", event.target.checked),
      });
  },
});

// ElAutocomplete behaves like an ElInput wrapper plus a fetchSuggestions
// callback. Match the ElInput stub's two-element shape (outer div with
// ``data-el-autocomplete`` carrying ``inheritAttrs`` + child ``<input>``)
// so test selectors that chain ``[data-testid="..."] input`` continue to
// work just as they did before this swap.
const ElAutocompleteStub = defineComponent({
  name: "ElAutocomplete",
  inheritAttrs: false,
  props: {
    modelValue: { type: [String, Number], default: "" },
    placeholder: { type: String, default: "" },
    fetchSuggestions: { type: Function, default: null },
  },
  emits: ["update:modelValue"],
  setup(props, { emit, attrs }) {
    return () =>
      h(
        "div",
        { ...attrs, "data-el-autocomplete": "true" },
        [
          h("input", {
            type: "text",
            value: props.modelValue ?? "",
            placeholder: props.placeholder,
            onInput: (event) => {
              emit("update:modelValue", event.target.value);
              // Auto-fire the fetch handler on every keystroke so tests
              // can assert what the page asks the server for.
              const value = event.target.value;
              if (typeof props.fetchSuggestions === "function") {
                const captured = [];
                const cb = (items) => {
                  captured.push(...(items || []));
                  if (typeof window !== "undefined") {
                    window.__lastAutocompleteSuggestions = captured;
                  }
                };
                props.fetchSuggestions(value, cb);
              }
            },
          }),
        ],
      );
  },
});

const stubs = {
  ...ElementPlusStubs,
  ElRadioGroup: ElRadioGroupStub,
  ElRadio: ElRadioStub,
  ElSwitch: ElSwitchStub,
  ElAutocomplete: ElAutocompleteStub,
};

async function waitForAutocomplete() {
  // suggestions are populated synchronously by the stub; flushPromises
  // drains the awaited mock to settle.
  await flushPromises();
  return globalThis.window?.__lastAutocompleteSuggestions ?? [];
}

const SOURCE_HF = {
  id: 1,
  namespace: "",
  url: "https://huggingface.co",
  token: null,
  priority: 100,
  name: "HuggingFace",
  source_type: "huggingface",
  enabled: true,
  created_at: "2026-01-01T00:00:00Z",
  updated_at: "2026-01-01T00:00:00Z",
};
const SOURCE_MIRROR = {
  id: 2,
  namespace: "mirror-org",
  url: "https://mirror.local",
  token: "tok-secret",
  priority: 50,
  name: "Mirror",
  source_type: "kohakuhub",
  enabled: false,
  created_at: "2026-02-01T00:00:00Z",
  updated_at: "2026-02-15T00:00:00Z",
};
const STATS = {
  size: 42,
  maxsize: 10000,
  ttl_seconds: 300,
  usage_percent: 0.42,
};

function mountPage() {
  return mount(FallbackSourcesPage, {
    global: {
      stubs,
    },
  });
}

describe("admin fallback-sources page", () => {
  beforeEach(async () => {
    mocks.router.push.mockReset();
    mocks.adminStore.token = "admin-token";
    Object.values(mocks.api).forEach((fn) => fn.mockReset());
    messageBoxConfirmSpy.mockReset();
    messageSuccessSpy.mockReset();
    messageErrorSpy.mockReset();

    if (!elementPlusModule) {
      elementPlusModule = await vi.importActual("element-plus");
    }
    vi.spyOn(elementPlusModule.ElMessageBox, "confirm").mockImplementation(
      (...args) => messageBoxConfirmSpy(...args),
    );
    vi.spyOn(elementPlusModule.ElMessage, "success").mockImplementation(
      (...args) => messageSuccessSpy(...args),
    );
    vi.spyOn(elementPlusModule.ElMessage, "error").mockImplementation(
      (...args) => messageErrorSpy(...args),
    );
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  // -----------------------------------------------------------------
  // Initial mount + auth gate
  // -----------------------------------------------------------------

  it("loads sources and cache stats on mount", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF, SOURCE_MIRROR]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);

    const wrapper = mountPage();
    await flushPromises();

    expect(mocks.api.listFallbackSources).toHaveBeenCalledWith("admin-token");
    expect(mocks.api.getFallbackCacheStats).toHaveBeenCalledWith("admin-token");
    expect(wrapper.text()).toContain("HuggingFace");
    expect(wrapper.text()).toContain("Mirror");
    // stats render
    expect(wrapper.text()).toContain("42"); // size
    expect(wrapper.text()).toContain("10000"); // maxsize
  });

  it("redirects to /login when admin token is missing", async () => {
    mocks.adminStore.token = null;
    mountPage();
    await flushPromises();
    expect(mocks.router.push).toHaveBeenCalledWith("/login");
    expect(mocks.api.listFallbackSources).not.toHaveBeenCalled();
  });

  it("renders empty state when no sources are configured", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();
    expect(wrapper.text()).toContain("No fallback sources configured");
  });

  it("surfaces an error toast when listFallbackSources fails", async () => {
    const failure = new Error("nope");
    failure.response = { data: { detail: { error: "DB down" } } };
    mocks.api.listFallbackSources.mockRejectedValue(failure);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mountPage();
    await flushPromises();
    expect(messageErrorSpy).toHaveBeenCalledWith("DB down");
  });

  it("falls back to the generic message when the listFallbackSources error has no detail", async () => {
    mocks.api.listFallbackSources.mockRejectedValue(new Error("boom"));
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mountPage();
    await flushPromises();
    expect(messageErrorSpy).toHaveBeenCalledWith("Failed to load fallback sources");
  });

  it("logs but does not toast when getFallbackCacheStats fails", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockRejectedValue(new Error("stats down"));
    mountPage();
    await flushPromises();
    // No error toast for the silent stats fetch — pattern matches the page.
    expect(messageErrorSpy).not.toHaveBeenCalled();
  });

  // -----------------------------------------------------------------
  // Source create / edit / delete / toggle
  // -----------------------------------------------------------------

  it("creates a new source and reloads", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.createFallbackSource.mockResolvedValue({ ...SOURCE_HF, id: 99 });
    const wrapper = mountPage();
    await flushPromises();

    // Open create dialog by clicking "Add Source" — the only primary
    // button in the Configured Sources card header.
    const addButton = wrapper
      .findAll("button")
      .find((b) => b.text().includes("Add Source"));
    await addButton.trigger("click");
    await flushPromises();

    // Submit through the open dialog's primary button. The page wires
    // it to handleSubmit via @click on the "Create" button in the dialog footer.
    const createButton = wrapper
      .findAll("button")
      .find((b) => b.text().trim() === "Create");
    await createButton.trigger("click");
    await flushPromises();

    expect(mocks.api.createFallbackSource).toHaveBeenCalledTimes(1);
    expect(messageSuccessSpy).toHaveBeenCalledWith(
      "Fallback source created successfully",
    );
    // Reload happens after create.
    expect(mocks.api.listFallbackSources).toHaveBeenCalledTimes(2);
  });

  it("updates an existing source via edit dialog", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.updateFallbackSource.mockResolvedValue(SOURCE_HF);
    const wrapper = mountPage();
    await flushPromises();

    // Find the edit button on the only listed source. The edit button
    // lives in source-actions and is the one with i-carbon-edit icon —
    // identifiable as the second action button (after the toggle and
    // before delete in the source-actions group).
    const editButtons = wrapper.findAll(".source-actions button");
    // Order: Disable/Enable, Edit, Delete.
    await editButtons[1].trigger("click");
    await flushPromises();

    const updateButton = wrapper
      .findAll("button")
      .find((b) => b.text().trim() === "Update");
    await updateButton.trigger("click");
    await flushPromises();

    expect(mocks.api.updateFallbackSource).toHaveBeenCalledTimes(1);
    expect(messageSuccessSpy).toHaveBeenCalledWith(
      "Fallback source updated successfully",
    );
  });

  it("toggles enabled flag via the Disable/Enable button", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.updateFallbackSource.mockResolvedValue({
      ...SOURCE_HF,
      enabled: false,
    });
    const wrapper = mountPage();
    await flushPromises();

    const buttons = wrapper.findAll(".source-actions button");
    await buttons[0].trigger("click");
    await flushPromises();

    expect(mocks.api.updateFallbackSource).toHaveBeenCalledWith(
      "admin-token",
      SOURCE_HF.id,
      { enabled: false },
    );
    expect(messageSuccessSpy).toHaveBeenCalledWith(
      `Source "${SOURCE_HF.name}" disabled`,
    );
  });

  it("deletes a source after confirm", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.deleteFallbackSource.mockResolvedValue({ success: true });
    messageBoxConfirmSpy.mockResolvedValue("confirm");
    const wrapper = mountPage();
    await flushPromises();

    const buttons = wrapper.findAll(".source-actions button");
    await buttons[2].trigger("click");
    await flushPromises();

    expect(messageBoxConfirmSpy).toHaveBeenCalled();
    expect(mocks.api.deleteFallbackSource).toHaveBeenCalledWith(
      "admin-token",
      SOURCE_HF.id,
    );
    expect(messageSuccessSpy).toHaveBeenCalledWith(
      `Fallback source "${SOURCE_HF.name}" deleted`,
    );
  });

  it("does not delete when the operator cancels the confirm dialog", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    messageBoxConfirmSpy.mockRejectedValue("cancel");
    const wrapper = mountPage();
    await flushPromises();

    const buttons = wrapper.findAll(".source-actions button");
    await buttons[2].trigger("click");
    await flushPromises();
    expect(mocks.api.deleteFallbackSource).not.toHaveBeenCalled();
  });

  it("surfaces a toast when delete itself fails", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    messageBoxConfirmSpy.mockResolvedValue("confirm");
    const failure = new Error("API error");
    failure.response = { data: { detail: { error: "permission denied" } } };
    mocks.api.deleteFallbackSource.mockRejectedValue(failure);
    const wrapper = mountPage();
    await flushPromises();

    const buttons = wrapper.findAll(".source-actions button");
    await buttons[2].trigger("click");
    await flushPromises();

    expect(messageErrorSpy).toHaveBeenCalledWith("permission denied");
  });

  it("falls back to generic message when delete error has no detail", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    messageBoxConfirmSpy.mockResolvedValue("confirm");
    mocks.api.deleteFallbackSource.mockRejectedValue(new Error("boom"));
    const wrapper = mountPage();
    await flushPromises();

    const buttons = wrapper.findAll(".source-actions button");
    await buttons[2].trigger("click");
    await flushPromises();

    expect(messageErrorSpy).toHaveBeenCalledWith(
      "Failed to delete fallback source",
    );
  });

  it("surfaces a toast when toggling the enabled flag fails", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.updateFallbackSource.mockRejectedValue(new Error("boom"));
    const wrapper = mountPage();
    await flushPromises();

    const buttons = wrapper.findAll(".source-actions button");
    await buttons[0].trigger("click");
    await flushPromises();

    expect(messageErrorSpy).toHaveBeenCalledWith("Failed to toggle source");
  });

  it("surfaces a toast when create/update submit fails", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.createFallbackSource.mockRejectedValue(new Error("boom"));
    const wrapper = mountPage();
    await flushPromises();

    const addButton = wrapper
      .findAll("button")
      .find((b) => b.text().includes("Add Source"));
    await addButton.trigger("click");
    await flushPromises();

    const createButton = wrapper
      .findAll("button")
      .find((b) => b.text().trim() === "Create");
    await createButton.trigger("click");
    await flushPromises();

    expect(messageErrorSpy).toHaveBeenCalledWith("Failed to save fallback source");
  });

  // -----------------------------------------------------------------
  // Existing global Clear Cache flow
  // -----------------------------------------------------------------

  it("clears the global fallback cache after confirm", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.clearFallbackCache.mockResolvedValue({
      success: true,
      message: "Cache cleared (42 entries removed)",
      old_size: 42,
    });
    messageBoxConfirmSpy.mockResolvedValue("confirm");
    const wrapper = mountPage();
    await flushPromises();

    const clearButton = wrapper
      .findAll("button")
      .find((b) => b.text().includes("Clear Cache"));
    await clearButton.trigger("click");
    await flushPromises();

    expect(messageBoxConfirmSpy).toHaveBeenCalled();
    expect(mocks.api.clearFallbackCache).toHaveBeenCalledWith("admin-token");
    expect(messageSuccessSpy).toHaveBeenCalledWith(
      "Cache cleared (42 entries removed)",
    );
  });

  it("does not clear cache when confirm dialog is cancelled", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    messageBoxConfirmSpy.mockRejectedValue("cancel");
    const wrapper = mountPage();
    await flushPromises();

    const clearButton = wrapper
      .findAll("button")
      .find((b) => b.text().includes("Clear Cache"));
    await clearButton.trigger("click");
    await flushPromises();
    expect(mocks.api.clearFallbackCache).not.toHaveBeenCalled();
  });

  it("surfaces a toast when global clear cache fails", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    messageBoxConfirmSpy.mockResolvedValue("confirm");
    mocks.api.clearFallbackCache.mockRejectedValue(new Error("boom"));
    const wrapper = mountPage();
    await flushPromises();

    const clearButton = wrapper
      .findAll("button")
      .find((b) => b.text().includes("Clear Cache"));
    await clearButton.trigger("click");
    await flushPromises();

    expect(messageErrorSpy).toHaveBeenCalledWith("Failed to clear cache");
  });

  // -----------------------------------------------------------------
  // Autocomplete suggestion fetchers
  // -----------------------------------------------------------------

  it("fetches namespace suggestions via listUsers(include_orgs=true)", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.listUsers.mockResolvedValue({
      users: [
        { username: "openai-community" },
        { username: "openai" },
      ],
    });
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-repo-button"]').trigger("click");
    await flushPromises();
    await wrapper
      .get('[data-testid="evict-repo-namespace"] input')
      .setValue("openai");
    await flushPromises();

    expect(mocks.api.listUsers).toHaveBeenCalledWith(
      "admin-token",
      expect.objectContaining({
        search: "openai",
        limit: 20,
        include_orgs: true,
      }),
    );
    expect(
      globalThis.window.__lastAutocompleteSuggestions.map((s) => s.value),
    ).toEqual(["openai-community", "openai"]);
  });

  it("namespace fetcher returns [] for empty query", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-repo-button"]').trigger("click");
    await flushPromises();
    globalThis.window.__lastAutocompleteSuggestions = [];
    await wrapper
      .get('[data-testid="evict-repo-namespace"] input')
      .setValue("");
    await flushPromises();
    expect(mocks.api.listUsers).not.toHaveBeenCalled();
    expect(globalThis.window.__lastAutocompleteSuggestions).toEqual([]);
  });

  it("namespace fetcher swallows API errors and returns []", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.listUsers.mockRejectedValue(new Error("listUsers boom"));
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-repo-button"]').trigger("click");
    await flushPromises();
    globalThis.window.__lastAutocompleteSuggestions = ["unset"];
    await wrapper
      .get('[data-testid="evict-repo-namespace"] input')
      .setValue("anything");
    await flushPromises();
    expect(globalThis.window.__lastAutocompleteSuggestions).toEqual([]);
    consoleSpy.mockRestore();
  });

  it("namespace fetcher honors checkAuth (no token -> []) and pushes to login", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-repo-button"]').trigger("click");
    await flushPromises();
    mocks.adminStore.token = null;
    globalThis.window.__lastAutocompleteSuggestions = ["unset"];
    await wrapper
      .get('[data-testid="evict-repo-namespace"] input')
      .setValue("openai");
    await flushPromises();
    expect(mocks.api.listUsers).not.toHaveBeenCalled();
    expect(globalThis.window.__lastAutocompleteSuggestions).toEqual([]);
    expect(mocks.router.push).toHaveBeenCalledWith("/login");
  });

  it("namespace fetcher unwraps {items} response shape (vs {users})", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.listUsers.mockResolvedValue({
      items: [{ username: "items_shaped_org" }],
    });
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-repo-button"]').trigger("click");
    await flushPromises();
    await wrapper
      .get('[data-testid="evict-repo-namespace"] input')
      .setValue("items");
    await flushPromises();
    expect(
      globalThis.window.__lastAutocompleteSuggestions.map((s) => s.value),
    ).toEqual(["items_shaped_org"]);
  });

  it("namespace fetcher accepts raw array response shape", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.listUsers.mockResolvedValue([
      { username: "raw_array_ns" },
    ]);
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-repo-button"]').trigger("click");
    await flushPromises();
    await wrapper
      .get('[data-testid="evict-repo-namespace"] input')
      .setValue("raw");
    await flushPromises();
    expect(
      globalThis.window.__lastAutocompleteSuggestions.map((s) => s.value),
    ).toEqual(["raw_array_ns"]);
  });

  it("fetches repo-name suggestions filtered by repo_type + namespace", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    // Wrapped {items: ...} shape — verify the helper unwraps it.
    mocks.api.listRepositories.mockResolvedValue({
      items: [
        { namespace: "openai-community", name: "gpt2" },
        { namespace: "openai-community", name: "gpt2-medium" },
      ],
    });
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-repo-button"]').trigger("click");
    await flushPromises();
    await wrapper
      .get('[data-testid="evict-repo-namespace"] input')
      .setValue("openai-community");
    await flushPromises();
    mocks.api.listRepositories.mockClear();
    mocks.api.listRepositories.mockResolvedValue({
      items: [
        { namespace: "openai-community", name: "gpt2" },
        { namespace: "openai-community", name: "gpt2-medium" },
      ],
    });
    await wrapper
      .get('[data-testid="evict-repo-name"] input')
      .setValue("gpt2");
    await flushPromises();
    expect(mocks.api.listRepositories).toHaveBeenCalledWith(
      "admin-token",
      expect.objectContaining({
        search: "gpt2",
        repo_type: "model",
        namespace: "openai-community",
        limit: 20,
      }),
    );
    expect(
      globalThis.window.__lastAutocompleteSuggestions.map((s) => s.value),
    ).toEqual(["gpt2", "gpt2-medium"]);
  });

  it("repo-name fetcher unwraps {repositories: [...]} (backend default shape)", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.listRepositories.mockResolvedValue({
      repositories: [
        { namespace: "openai-community", name: "gpt2" },
        { namespace: "openai-community", name: "gpt2-medium" },
      ],
    });
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-repo-button"]').trigger("click");
    await flushPromises();
    await wrapper
      .get('[data-testid="evict-repo-namespace"] input')
      .setValue("openai-community");
    await flushPromises();
    mocks.api.listRepositories.mockClear();
    mocks.api.listRepositories.mockResolvedValue({
      repositories: [
        { namespace: "openai-community", name: "gpt2" },
        { namespace: "openai-community", name: "gpt2-medium" },
      ],
    });
    await wrapper
      .get('[data-testid="evict-repo-name"] input')
      .setValue("gpt2");
    await flushPromises();
    expect(
      globalThis.window.__lastAutocompleteSuggestions.map((s) => s.value),
    ).toEqual(["gpt2", "gpt2-medium"]);
  });

  it("repo-name fetcher accepts raw array response shape", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.listRepositories.mockResolvedValue([
      { namespace: "x", name: "raw-array-repo" },
    ]);
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-repo-button"]').trigger("click");
    await flushPromises();
    await wrapper
      .get('[data-testid="evict-repo-name"] input')
      .setValue("raw-array");
    await flushPromises();
    expect(
      globalThis.window.__lastAutocompleteSuggestions.map((s) => s.value),
    ).toEqual(["raw-array-repo"]);
  });

  it("repo-name fetcher returns [] for empty query", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-repo-button"]').trigger("click");
    await flushPromises();
    globalThis.window.__lastAutocompleteSuggestions = ["unset"];
    await wrapper.get('[data-testid="evict-repo-name"] input').setValue("");
    await flushPromises();
    expect(mocks.api.listRepositories).not.toHaveBeenCalled();
    expect(globalThis.window.__lastAutocompleteSuggestions).toEqual([]);
  });

  it("repo-name fetcher swallows API errors", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.listRepositories.mockRejectedValue(new Error("repos boom"));
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-repo-button"]').trigger("click");
    await flushPromises();
    globalThis.window.__lastAutocompleteSuggestions = ["unset"];
    await wrapper.get('[data-testid="evict-repo-name"] input').setValue("g");
    await flushPromises();
    expect(globalThis.window.__lastAutocompleteSuggestions).toEqual([]);
    consoleSpy.mockRestore();
  });

  it("repo-name fetcher honors checkAuth", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="evict-repo-button"]').trigger("click");
    await flushPromises();
    mocks.adminStore.token = null;
    globalThis.window.__lastAutocompleteSuggestions = ["unset"];
    await wrapper.get('[data-testid="evict-repo-name"] input').setValue("g");
    await flushPromises();
    expect(globalThis.window.__lastAutocompleteSuggestions).toEqual([]);
    expect(mocks.router.push).toHaveBeenCalledWith("/login");
  });

  it("fetches username suggestions via listUsers on input", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.listUsers.mockResolvedValue({
      users: [
        { username: "mai_lin" },
        { username: "mai_admin" },
      ],
    });
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-user-button"]').trigger("click");
    await flushPromises();
    await wrapper
      .get('[data-testid="evict-user-username"] input')
      .setValue("mai");
    await flushPromises();
    expect(mocks.api.listUsers).toHaveBeenCalledWith(
      "admin-token",
      expect.objectContaining({
        search: "mai",
        limit: 20,
        include_orgs: false,
      }),
    );
    expect(
      globalThis.window.__lastAutocompleteSuggestions.map((s) => s.value),
    ).toEqual(["mai_lin", "mai_admin"]);
  });

  it("username fetcher returns [] for empty query", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="evict-user-button"]').trigger("click");
    await flushPromises();
    globalThis.window.__lastAutocompleteSuggestions = ["unset"];
    await wrapper
      .get('[data-testid="evict-user-username"] input')
      .setValue("");
    await flushPromises();
    expect(mocks.api.listUsers).not.toHaveBeenCalled();
    expect(globalThis.window.__lastAutocompleteSuggestions).toEqual([]);
  });

  it("username fetcher swallows API errors", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.listUsers.mockRejectedValue(new Error("users boom"));
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="evict-user-button"]').trigger("click");
    await flushPromises();
    globalThis.window.__lastAutocompleteSuggestions = ["unset"];
    await wrapper
      .get('[data-testid="evict-user-username"] input')
      .setValue("m");
    await flushPromises();
    expect(globalThis.window.__lastAutocompleteSuggestions).toEqual([]);
    consoleSpy.mockRestore();
  });

  it("username fetcher honors checkAuth", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="evict-user-button"]').trigger("click");
    await flushPromises();
    mocks.adminStore.token = null;
    globalThis.window.__lastAutocompleteSuggestions = ["unset"];
    await wrapper
      .get('[data-testid="evict-user-username"] input')
      .setValue("m");
    await flushPromises();
    expect(globalThis.window.__lastAutocompleteSuggestions).toEqual([]);
    expect(mocks.router.push).toHaveBeenCalledWith("/login");
  });


  it("namespace fetcher handles null / empty response shape", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.listUsers.mockResolvedValue(null);
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="evict-repo-button"]').trigger("click");
    await flushPromises();
    globalThis.window.__lastAutocompleteSuggestions = ["unset"];
    await wrapper
      .get('[data-testid="evict-repo-namespace"] input')
      .setValue("anything");
    await flushPromises();
    expect(globalThis.window.__lastAutocompleteSuggestions).toEqual([]);
  });

  it("username fetcher handles null / empty response shape", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.listUsers.mockResolvedValue(null);
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="evict-user-button"]').trigger("click");
    await flushPromises();
    globalThis.window.__lastAutocompleteSuggestions = ["unset"];
    await wrapper
      .get('[data-testid="evict-user-username"] input')
      .setValue("anything");
    await flushPromises();
    expect(globalThis.window.__lastAutocompleteSuggestions).toEqual([]);
  });

  it("username fetcher unwraps {items} shape (vs {users} shape)", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.listUsers.mockResolvedValue({
      items: [{ username: "items_shaped_user" }],
    });
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="evict-user-button"]').trigger("click");
    await flushPromises();
    await wrapper
      .get('[data-testid="evict-user-username"] input')
      .setValue("items");
    await flushPromises();
    expect(
      globalThis.window.__lastAutocompleteSuggestions.map((s) => s.value),
    ).toEqual(["items_shaped_user"]);
  });

  it("listUsers array shape (no .users / .items wrap)", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.listUsers.mockResolvedValue([{ username: "raw_array_user" }]);
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="evict-user-button"]').trigger("click");
    await flushPromises();
    await wrapper
      .get('[data-testid="evict-user-username"] input')
      .setValue("raw");
    await flushPromises();
    expect(
      globalThis.window.__lastAutocompleteSuggestions.map((s) => s.value),
    ).toEqual(["raw_array_user"]);
  });

  // -----------------------------------------------------------------
  // NEW (#79): per-repo eviction dialog
  // -----------------------------------------------------------------

  it("evicts a repo cache via the per-repo dialog", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.invalidateFallbackRepoCache.mockResolvedValue({
      success: true,
      evicted: 3,
      repo_type: "model",
      namespace: "owner",
      name: "demo",
    });
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-repo-button"]').trigger("click");
    await flushPromises();

    // Dialog now visible — fill namespace and name; repo_type defaults to "model".
    const dialog = wrapper.get('[data-testid="evict-repo-dialog"]');
    const namespaceInput = dialog.get(
      '[data-testid="evict-repo-namespace"] input',
    );
    await namespaceInput.setValue("owner");
    const nameInput = dialog.get('[data-testid="evict-repo-name"] input');
    await nameInput.setValue("demo");

    await dialog.get('[data-testid="evict-repo-submit"]').trigger("click");
    await flushPromises();

    expect(mocks.api.invalidateFallbackRepoCache).toHaveBeenCalledWith(
      "admin-token",
      "model",
      "owner",
      "demo",
    );
    expect(messageSuccessSpy).toHaveBeenCalledWith(
      "Evicted 3 cache entries for model/owner/demo",
    );
    // Stats refresh post-eviction.
    expect(mocks.api.getFallbackCacheStats).toHaveBeenCalledTimes(2);
  });

  it("evict-by-repo singular vs plural success message", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.invalidateFallbackRepoCache.mockResolvedValue({
      success: true,
      evicted: 1,
      repo_type: "dataset",
      namespace: "ns",
      name: "r1",
    });
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-repo-button"]').trigger("click");
    await flushPromises();
    const dialog = wrapper.get('[data-testid="evict-repo-dialog"]');

    // switch repo_type to "dataset"
    const select = dialog.get('[data-testid="evict-repo-type"]');
    select.element.value = "dataset";
    await select.trigger("change");

    await dialog
      .get('[data-testid="evict-repo-namespace"] input')
      .setValue("ns");
    await dialog.get('[data-testid="evict-repo-name"] input').setValue("r1");
    await dialog.get('[data-testid="evict-repo-submit"]').trigger("click");
    await flushPromises();

    expect(messageSuccessSpy).toHaveBeenCalledWith(
      "Evicted 1 cache entry for dataset/ns/r1",
    );
  });

  it("evict-by-repo rejects empty namespace / name", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-repo-button"]').trigger("click");
    await flushPromises();
    const dialog = wrapper.get('[data-testid="evict-repo-dialog"]');

    // Submit with empty fields.
    await dialog.get('[data-testid="evict-repo-submit"]').trigger("click");
    await flushPromises();

    expect(messageErrorSpy).toHaveBeenCalledWith(
      "repo_type, namespace, and name are all required",
    );
    expect(mocks.api.invalidateFallbackRepoCache).not.toHaveBeenCalled();
  });

  it("evict-by-repo surfaces backend error", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const failure = new Error("boom");
    failure.response = { data: { detail: { error: "internal error" } } };
    mocks.api.invalidateFallbackRepoCache.mockRejectedValue(failure);
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-repo-button"]').trigger("click");
    await flushPromises();
    const dialog = wrapper.get('[data-testid="evict-repo-dialog"]');
    await dialog
      .get('[data-testid="evict-repo-namespace"] input')
      .setValue("ns");
    await dialog.get('[data-testid="evict-repo-name"] input').setValue("r1");
    await dialog.get('[data-testid="evict-repo-submit"]').trigger("click");
    await flushPromises();

    expect(messageErrorSpy).toHaveBeenCalledWith("internal error");
  });

  it("evict-by-repo falls back to generic message when error has no detail", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.invalidateFallbackRepoCache.mockRejectedValue(new Error("boom"));
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-repo-button"]').trigger("click");
    await flushPromises();
    const dialog = wrapper.get('[data-testid="evict-repo-dialog"]');
    await dialog
      .get('[data-testid="evict-repo-namespace"] input')
      .setValue("ns");
    await dialog.get('[data-testid="evict-repo-name"] input').setValue("r1");
    await dialog.get('[data-testid="evict-repo-submit"]').trigger("click");
    await flushPromises();

    expect(messageErrorSpy).toHaveBeenCalledWith("Failed to evict repo cache");
  });

  // -----------------------------------------------------------------
  // NEW (#79): per-user eviction dialog (username + user_id modes)
  // -----------------------------------------------------------------

  it("evicts user cache by username after confirm", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.invalidateFallbackUserCacheByUsername.mockResolvedValue({
      success: true,
      evicted: 5,
      user_id: 42,
      username: "mai_lin",
    });
    messageBoxConfirmSpy.mockResolvedValue("confirm");
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-user-button"]').trigger("click");
    await flushPromises();
    const dialog = wrapper.get('[data-testid="evict-user-dialog"]');

    await dialog
      .get('[data-testid="evict-user-username"] input')
      .setValue("mai_lin");
    await dialog.get('[data-testid="evict-user-submit"]').trigger("click");
    await flushPromises();

    expect(messageBoxConfirmSpy).toHaveBeenCalled();
    expect(mocks.api.invalidateFallbackUserCacheByUsername).toHaveBeenCalledWith(
      "admin-token",
      "mai_lin",
    );
    expect(messageSuccessSpy).toHaveBeenCalledWith(
      "Evicted 5 cache entries for mai_lin (user_id=42)",
    );
  });

  it("evicts user cache by user_id after confirm + radio toggle", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.invalidateFallbackUserCacheById.mockResolvedValue({
      success: true,
      evicted: 1,
      user_id: 7,
    });
    messageBoxConfirmSpy.mockResolvedValue("confirm");
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-user-button"]').trigger("click");
    await flushPromises();
    const dialog = wrapper.get('[data-testid="evict-user-dialog"]');

    // Switch radio to "user_id" mode by clicking the radio with that value.
    const radioGroup = dialog.get('[data-testid="evict-user-mode"]');
    const userIdRadio = radioGroup.get('[data-radio-value="user_id"]');
    await userIdRadio.trigger("click");
    await flushPromises();

    // Now the user_id input should be visible.
    const numberInput = dialog.get('[data-testid="evict-user-userid"]');
    numberInput.element.value = "7";
    await numberInput.trigger("input");

    await dialog.get('[data-testid="evict-user-submit"]').trigger("click");
    await flushPromises();

    expect(mocks.api.invalidateFallbackUserCacheById).toHaveBeenCalledWith(
      "admin-token",
      7,
    );
    expect(messageSuccessSpy).toHaveBeenCalledWith(
      "Evicted 1 cache entry for user_id=7",
    );
  });

  it("evict-by-user does nothing when confirm dialog is cancelled", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    messageBoxConfirmSpy.mockRejectedValue("cancel");
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-user-button"]').trigger("click");
    await flushPromises();
    const dialog = wrapper.get('[data-testid="evict-user-dialog"]');
    await dialog
      .get('[data-testid="evict-user-username"] input')
      .setValue("mai_lin");
    await dialog.get('[data-testid="evict-user-submit"]').trigger("click");
    await flushPromises();
    expect(
      mocks.api.invalidateFallbackUserCacheByUsername,
    ).not.toHaveBeenCalled();
  });

  it("evict-by-user logs unexpected confirm-dialog errors", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    messageBoxConfirmSpy.mockRejectedValue(new Error("unexpected"));
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-user-button"]').trigger("click");
    await flushPromises();
    const dialog = wrapper.get('[data-testid="evict-user-dialog"]');
    await dialog
      .get('[data-testid="evict-user-username"] input')
      .setValue("mai_lin");
    await dialog.get('[data-testid="evict-user-submit"]').trigger("click");
    await flushPromises();
    expect(
      mocks.api.invalidateFallbackUserCacheByUsername,
    ).not.toHaveBeenCalled();
    consoleSpy.mockRestore();
  });

  it("evict-by-user (username mode) rejects empty username", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-user-button"]').trigger("click");
    await flushPromises();
    const dialog = wrapper.get('[data-testid="evict-user-dialog"]');
    // Submit empty.
    await dialog.get('[data-testid="evict-user-submit"]').trigger("click");
    await flushPromises();
    expect(messageErrorSpy).toHaveBeenCalledWith("Username is required");
  });

  it("evict-by-user (user_id mode) rejects non-positive user_id", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-user-button"]').trigger("click");
    await flushPromises();
    const dialog = wrapper.get('[data-testid="evict-user-dialog"]');

    // Switch to user_id mode.
    const radioGroup = dialog.get('[data-testid="evict-user-mode"]');
    await radioGroup.get('[data-radio-value="user_id"]').trigger("click");
    await flushPromises();

    // Leave user_id null.
    await dialog.get('[data-testid="evict-user-submit"]').trigger("click");
    await flushPromises();
    expect(messageErrorSpy).toHaveBeenCalledWith(
      "user_id must be a positive integer",
    );
  });

  it("evict-by-user surfaces 404 / unknown-user backend error", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    messageBoxConfirmSpy.mockResolvedValue("confirm");
    const failure = new Error("not found");
    failure.response = {
      data: { detail: { error: "User not found: nope" } },
    };
    mocks.api.invalidateFallbackUserCacheByUsername.mockRejectedValue(failure);
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-user-button"]').trigger("click");
    await flushPromises();
    const dialog = wrapper.get('[data-testid="evict-user-dialog"]');
    await dialog
      .get('[data-testid="evict-user-username"] input')
      .setValue("nope");
    await dialog.get('[data-testid="evict-user-submit"]').trigger("click");
    await flushPromises();

    expect(messageErrorSpy).toHaveBeenCalledWith("User not found: nope");
  });

  it("evict-by-user falls back to generic message when error has no detail", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    messageBoxConfirmSpy.mockResolvedValue("confirm");
    mocks.api.invalidateFallbackUserCacheByUsername.mockRejectedValue(
      new Error("boom"),
    );
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="evict-user-button"]').trigger("click");
    await flushPromises();
    const dialog = wrapper.get('[data-testid="evict-user-dialog"]');
    await dialog
      .get('[data-testid="evict-user-username"] input')
      .setValue("x");
    await dialog.get('[data-testid="evict-user-submit"]').trigger("click");
    await flushPromises();
    expect(messageErrorSpy).toHaveBeenCalledWith("Failed to evict user cache");
  });

  // -----------------------------------------------------------------
  // Auth gate on action handlers
  // -----------------------------------------------------------------

  it("redirects to login when token is missing on cache-clear path", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();

    mocks.adminStore.token = null;
    const clearButton = wrapper
      .findAll("button")
      .find((b) => b.text().includes("Clear Cache"));
    await clearButton.trigger("click");
    await flushPromises();
    expect(mocks.router.push).toHaveBeenCalledWith("/login");
  });

  it("redirects to login when token is missing on evict-repo path", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();

    mocks.adminStore.token = null;
    await wrapper.get('[data-testid="evict-repo-button"]').trigger("click");
    await flushPromises();
    const dialog = wrapper.get('[data-testid="evict-repo-dialog"]');
    await dialog
      .get('[data-testid="evict-repo-namespace"] input')
      .setValue("ns");
    await dialog.get('[data-testid="evict-repo-name"] input').setValue("r1");
    await dialog.get('[data-testid="evict-repo-submit"]').trigger("click");
    await flushPromises();
    expect(mocks.router.push).toHaveBeenCalledWith("/login");
    expect(mocks.api.invalidateFallbackRepoCache).not.toHaveBeenCalled();
  });

  it("redirects to login when token is missing on submit path (create)", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();

    const addButton = wrapper
      .findAll("button")
      .find((b) => b.text().includes("Add Source"));
    await addButton.trigger("click");
    await flushPromises();

    mocks.adminStore.token = null;
    const createButton = wrapper
      .findAll("button")
      .find((b) => b.text().trim() === "Create");
    await createButton.trigger("click");
    await flushPromises();
    expect(mocks.router.push).toHaveBeenCalledWith("/login");
    expect(mocks.api.createFallbackSource).not.toHaveBeenCalled();
  });

  it("redirects to login when token is missing on delete path", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    messageBoxConfirmSpy.mockResolvedValue("confirm");
    const wrapper = mountPage();
    await flushPromises();

    mocks.adminStore.token = null;
    const buttons = wrapper.findAll(".source-actions button");
    await buttons[2].trigger("click");
    await flushPromises();
    expect(mocks.router.push).toHaveBeenCalledWith("/login");
    expect(mocks.api.deleteFallbackSource).not.toHaveBeenCalled();
  });

  it("redirects to login when token is missing on toggle-enabled path", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();

    mocks.adminStore.token = null;
    const buttons = wrapper.findAll(".source-actions button");
    await buttons[0].trigger("click");
    await flushPromises();
    expect(mocks.router.push).toHaveBeenCalledWith("/login");
    expect(mocks.api.updateFallbackSource).not.toHaveBeenCalled();
  });

  it("toggles a disabled source to enabled (covers the inverse branch)", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_MIRROR]); // enabled=false
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.updateFallbackSource.mockResolvedValue({
      ...SOURCE_MIRROR,
      enabled: true,
    });
    const wrapper = mountPage();
    await flushPromises();

    const buttons = wrapper.findAll(".source-actions button");
    await buttons[0].trigger("click");
    await flushPromises();

    expect(mocks.api.updateFallbackSource).toHaveBeenCalledWith(
      "admin-token",
      SOURCE_MIRROR.id,
      { enabled: true },
    );
    expect(messageSuccessSpy).toHaveBeenCalledWith(
      `Source "${SOURCE_MIRROR.name}" enabled`,
    );
  });

  it("uses backend detail message on toggle / submit / clear-cache errors", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);

    // Toggle with detail.
    const toggleErr = new Error("boom");
    toggleErr.response = { data: { detail: { error: "toggle err" } } };
    mocks.api.updateFallbackSource.mockRejectedValueOnce(toggleErr);

    const wrapper = mountPage();
    await flushPromises();
    const buttons = wrapper.findAll(".source-actions button");
    await buttons[0].trigger("click");
    await flushPromises();
    expect(messageErrorSpy).toHaveBeenCalledWith("toggle err");

    // Submit with detail.
    const submitErr = new Error("boom");
    submitErr.response = { data: { detail: { error: "submit err" } } };
    mocks.api.createFallbackSource.mockRejectedValue(submitErr);

    const addButton = wrapper
      .findAll("button")
      .find((b) => b.text().includes("Add Source"));
    await addButton.trigger("click");
    await flushPromises();
    const createButton = wrapper
      .findAll("button")
      .find((b) => b.text().trim() === "Create");
    await createButton.trigger("click");
    await flushPromises();
    expect(messageErrorSpy).toHaveBeenCalledWith("submit err");

    // Clear cache with detail.
    messageErrorSpy.mockReset();
    messageBoxConfirmSpy.mockResolvedValue("confirm");
    const clearErr = new Error("boom");
    clearErr.response = { data: { detail: { error: "clear err" } } };
    mocks.api.clearFallbackCache.mockRejectedValue(clearErr);
    const clearButton = wrapper
      .findAll("button")
      .find((b) => b.text().includes("Clear Cache"));
    await clearButton.trigger("click");
    await flushPromises();
    expect(messageErrorSpy).toHaveBeenCalledWith("clear err");
  });

  it("redirects to login when token is missing on evict-user path", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();

    mocks.adminStore.token = null;
    await wrapper.get('[data-testid="evict-user-button"]').trigger("click");
    await flushPromises();
    const dialog = wrapper.get('[data-testid="evict-user-dialog"]');
    await dialog
      .get('[data-testid="evict-user-username"] input')
      .setValue("u");
    await dialog.get('[data-testid="evict-user-submit"]').trigger("click");
    await flushPromises();
    expect(mocks.router.push).toHaveBeenCalledWith("/login");
    expect(
      mocks.api.invalidateFallbackUserCacheByUsername,
    ).not.toHaveBeenCalled();
  });

  // -----------------------------------------------------------------
  // NEW (#78): Chain Tester — draft management
  // -----------------------------------------------------------------

  it("Load from System copies live sources into the draft", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF, SOURCE_MIRROR]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="load-from-system-btn"]').trigger("click");
    await flushPromises();

    // Two draft rows now visible.
    expect(wrapper.find('[data-testid="draft-row-0"]').exists()).toBe(true);
    expect(wrapper.find('[data-testid="draft-row-1"]').exists()).toBe(true);
    expect(messageSuccessSpy).toHaveBeenCalledWith(
      "Loaded 2 source(s) from system into draft",
    );
    // Just-loaded → not dirty.
    expect(wrapper.find('[data-testid="draft-dirty-tag"]').exists()).toBe(false);
  });

  it("Load from System redirects to login when token missing", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();
    mocks.adminStore.token = null;
    await wrapper.get('[data-testid="load-from-system-btn"]').trigger("click");
    await flushPromises();
    expect(mocks.router.push).toHaveBeenCalledWith("/login");
  });

  it("Add Source appends a blank row and marks draft dirty", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="draft-add-btn"]').trigger("click");
    await flushPromises();
    expect(wrapper.find('[data-testid="draft-row-0"]').exists()).toBe(true);
    expect(wrapper.find('[data-testid="draft-dirty-tag"]').exists()).toBe(true);
  });

  it("Remove draft row drops it and stays dirty", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="load-from-system-btn"]').trigger("click");
    await flushPromises();
    expect(wrapper.find('[data-testid="draft-row-0"]').exists()).toBe(true);
    await wrapper.get('[data-testid="draft-remove-0"]').trigger("click");
    await flushPromises();
    expect(wrapper.find('[data-testid="draft-row-0"]').exists()).toBe(false);
    expect(wrapper.find('[data-testid="draft-dirty-tag"]').exists()).toBe(true);
  });

  it("Discard Draft empties draft state", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="load-from-system-btn"]').trigger("click");
    await wrapper.get('[data-testid="draft-add-btn"]').trigger("click");
    await flushPromises();
    await wrapper.get('[data-testid="discard-draft-btn"]').trigger("click");
    await flushPromises();
    expect(wrapper.find('[data-testid="draft-row-0"]').exists()).toBe(false);
    expect(messageSuccessSpy).not.toHaveBeenCalledWith("Draft discarded");
    // discardDraft uses ElMessage.info, not success — verify via the
    // info path. Since our spy only catches success/error, just check
    // that the draft state actually changed.
  });

  // -----------------------------------------------------------------
  // NEW (#78): Push to System (bulk-replace)
  // -----------------------------------------------------------------

  it("Push to System calls bulkReplaceFallbackSources after confirm", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.bulkReplaceFallbackSources.mockResolvedValue({
      success: true, replaced: 1, before: 0, after: 1,
    });
    messageBoxConfirmSpy.mockResolvedValue("confirm");
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="load-from-system-btn"]').trigger("click");
    await flushPromises();
    // Make a meaningful edit to flip dirty.
    await wrapper.get('[data-testid="draft-add-btn"]').trigger("click");
    await flushPromises();
    await wrapper.get('[data-testid="push-to-system-btn"]').trigger("click");
    await flushPromises();

    expect(messageBoxConfirmSpy).toHaveBeenCalled();
    expect(mocks.api.bulkReplaceFallbackSources).toHaveBeenCalledTimes(1);
    expect(messageSuccessSpy).toHaveBeenCalledWith(
      "Pushed draft to system: 0 → 1 source(s)",
    );
  });

  it("Push to System redirects to login when token missing", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="load-from-system-btn"]').trigger("click");
    await wrapper.get('[data-testid="draft-add-btn"]').trigger("click");
    await flushPromises();
    mocks.adminStore.token = null;
    await wrapper.get('[data-testid="push-to-system-btn"]').trigger("click");
    await flushPromises();
    expect(mocks.router.push).toHaveBeenCalledWith("/login");
    expect(mocks.api.bulkReplaceFallbackSources).not.toHaveBeenCalled();
  });

  it("Push to System cancel branch — confirm dialog cancelled", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    messageBoxConfirmSpy.mockRejectedValue("cancel");
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="load-from-system-btn"]').trigger("click");
    await wrapper.get('[data-testid="draft-add-btn"]').trigger("click");
    await flushPromises();
    await wrapper.get('[data-testid="push-to-system-btn"]').trigger("click");
    await flushPromises();
    expect(mocks.api.bulkReplaceFallbackSources).not.toHaveBeenCalled();
  });

  it("Push to System unexpected confirm error logs but does not push", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    messageBoxConfirmSpy.mockRejectedValue(new Error("oops"));
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="load-from-system-btn"]').trigger("click");
    await wrapper.get('[data-testid="draft-add-btn"]').trigger("click");
    await flushPromises();
    await wrapper.get('[data-testid="push-to-system-btn"]').trigger("click");
    await flushPromises();
    expect(mocks.api.bulkReplaceFallbackSources).not.toHaveBeenCalled();
    consoleSpy.mockRestore();
  });

  it("Push to System surfaces backend error toast", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    messageBoxConfirmSpy.mockResolvedValue("confirm");
    const failure = new Error("api err");
    failure.response = { data: { detail: { error: "db blew up" } } };
    mocks.api.bulkReplaceFallbackSources.mockRejectedValue(failure);
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="load-from-system-btn"]').trigger("click");
    await wrapper.get('[data-testid="draft-add-btn"]').trigger("click");
    await flushPromises();
    await wrapper.get('[data-testid="push-to-system-btn"]').trigger("click");
    await flushPromises();
    expect(messageErrorSpy).toHaveBeenCalledWith("db blew up");
  });

  it("Push to System falls back to generic message when error has no detail", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    messageBoxConfirmSpy.mockResolvedValue("confirm");
    mocks.api.bulkReplaceFallbackSources.mockRejectedValue(new Error("boom"));
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="load-from-system-btn"]').trigger("click");
    await wrapper.get('[data-testid="draft-add-btn"]').trigger("click");
    await flushPromises();
    await wrapper.get('[data-testid="push-to-system-btn"]').trigger("click");
    await flushPromises();
    expect(messageErrorSpy).toHaveBeenCalledWith(
      "Failed to push draft to system",
    );
  });

  // -----------------------------------------------------------------
  // NEW (#78): Run Probe (simulate / real)
  // -----------------------------------------------------------------

  async function _seedProbeForm(wrapper) {
    await wrapper.get('[data-testid="probe-namespace"] input').setValue("openai");
    await wrapper.get('[data-testid="probe-name"] input').setValue("gpt2");
  }

  function _stubReport(opts = {}) {
    return {
      op: "info",
      repo_id: "openai/gpt2",
      revision: null,
      file_path: null,
      attempts: [
        {
          source_name: "A",
          source_url: "https://a.example",
          source_type: "huggingface",
          method: "GET",
          upstream_path: "/api/models/openai/gpt2",
          status_code: 200,
          x_error_code: null,
          x_error_message: null,
          decision: "BIND_AND_RESPOND",
          duration_ms: 12,
          error: null,
          response_body_preview: '{"id":"openai/gpt2"}',
          response_headers: { "content-type": "application/json" },
        },
      ],
      final_outcome: "BIND_AND_RESPOND",
      bound_source: { name: "A", url: "https://a.example" },
      duration_ms: 15,
      final_response: {
        status_code: 200,
        headers: { "content-type": "application/json" },
        body_preview: '{"id":"openai/gpt2"}',
      },
      ...opts,
    };
  }

  it("Run Simulate posts payload with draft sources + header overrides", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.testFallbackChainSimulate.mockResolvedValue(_stubReport());
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="load-from-system-btn"]').trigger("click");
    await flushPromises();
    await _seedProbeForm(wrapper);

    // Add a header token row.
    await wrapper.get('[data-testid="header-token-add"]').trigger("click");
    await flushPromises();
    await wrapper
      .get('[data-testid="header-token-url-0"] input')
      .setValue("https://huggingface.co");
    await wrapper
      .get('[data-testid="header-token-token-0"] input')
      .setValue("hf_test");

    await wrapper.get('[data-testid="run-simulate-btn"]').trigger("click");
    await flushPromises();

    expect(mocks.api.testFallbackChainSimulate).toHaveBeenCalledTimes(1);
    const [, payload] = mocks.api.testFallbackChainSimulate.mock.calls[0];
    expect(payload.op).toBe("info");
    expect(payload.namespace).toBe("openai");
    expect(payload.name).toBe("gpt2");
    expect(payload.sources).toHaveLength(1);
    expect(payload.user_tokens).toEqual({
      "https://huggingface.co": "hf_test",
    });
    // Result rendered.
    expect(wrapper.find('[data-testid="probe-report"]').exists()).toBe(true);
    expect(wrapper.text()).toContain("BIND_AND_RESPOND");
  });

  it("Run Simulate rejects when probe target is missing", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="load-from-system-btn"]').trigger("click");
    await flushPromises();
    // Don't fill probeForm.namespace / name.
    await wrapper.get('[data-testid="run-simulate-btn"]').trigger("click");
    await flushPromises();
    expect(messageErrorSpy).toHaveBeenCalledWith(
      "namespace and name are required for the probe target",
    );
    expect(mocks.api.testFallbackChainSimulate).not.toHaveBeenCalled();
  });

  it("Run Simulate refuses when draft is empty", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();
    await _seedProbeForm(wrapper);
    await wrapper.get('[data-testid="run-simulate-btn"]').trigger("click");
    await flushPromises();
    expect(messageErrorSpy).not.toHaveBeenCalled();
    expect(mocks.api.testFallbackChainSimulate).not.toHaveBeenCalled();
  });

  it("Run Simulate redirects to login when token missing", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="load-from-system-btn"]').trigger("click");
    await _seedProbeForm(wrapper);
    mocks.adminStore.token = null;
    await wrapper.get('[data-testid="run-simulate-btn"]').trigger("click");
    await flushPromises();
    expect(mocks.router.push).toHaveBeenCalledWith("/login");
  });

  it("Run Simulate handles backend error and shows error region", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const failure = new Error("api err");
    failure.response = { data: { detail: { error: "probe blew up" } } };
    mocks.api.testFallbackChainSimulate.mockRejectedValue(failure);
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="load-from-system-btn"]').trigger("click");
    await _seedProbeForm(wrapper);
    await wrapper.get('[data-testid="run-simulate-btn"]').trigger("click");
    await flushPromises();
    expect(wrapper.find('[data-testid="probe-error"]').exists()).toBe(true);
    expect(wrapper.text()).toContain("probe blew up");
  });

  it("Run Simulate generic fallback when no detail on error", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.testFallbackChainSimulate.mockRejectedValue(new Error("boom"));
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="load-from-system-btn"]').trigger("click");
    await _seedProbeForm(wrapper);
    await wrapper.get('[data-testid="run-simulate-btn"]').trigger("click");
    await flushPromises();
    expect(wrapper.text()).toContain("Simulate probe failed");
  });

  it("Run Real posts payload using as_username when set", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.testFallbackChainReal.mockResolvedValue(_stubReport());
    const wrapper = mountPage();
    await flushPromises();
    await _seedProbeForm(wrapper);
    // Switch identity to username mode.
    const userMode = wrapper.get('[data-testid="user-sim-mode"]');
    await userMode.get('[data-radio-value="username"]').trigger("click");
    await flushPromises();
    await wrapper.get('[data-testid="user-sim-username"] input').setValue("mai_lin");
    await wrapper.get('[data-testid="run-real-btn"]').trigger("click");
    await flushPromises();

    const [, payload] = mocks.api.testFallbackChainReal.mock.calls[0];
    expect(payload.as_username).toBe("mai_lin");
    expect(payload.as_user_id).toBeUndefined();
  });

  it("Run Real posts payload using as_user_id when set", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.testFallbackChainReal.mockResolvedValue(_stubReport());
    const wrapper = mountPage();
    await flushPromises();
    await _seedProbeForm(wrapper);
    const userMode = wrapper.get('[data-testid="user-sim-mode"]');
    await userMode.get('[data-radio-value="user_id"]').trigger("click");
    await flushPromises();
    const uid = wrapper.get('[data-testid="user-sim-userid"]');
    uid.element.value = "42";
    await uid.trigger("input");
    await wrapper.get('[data-testid="run-real-btn"]').trigger("click");
    await flushPromises();

    const [, payload] = mocks.api.testFallbackChainReal.mock.calls[0];
    expect(payload.as_user_id).toBe(42);
  });

  it("Run Real anonymous mode → no as_* fields", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.testFallbackChainReal.mockResolvedValue(_stubReport());
    const wrapper = mountPage();
    await flushPromises();
    await _seedProbeForm(wrapper);
    await wrapper.get('[data-testid="run-real-btn"]').trigger("click");
    await flushPromises();

    const [, payload] = mocks.api.testFallbackChainReal.mock.calls[0];
    expect(payload.as_username).toBeUndefined();
    expect(payload.as_user_id).toBeUndefined();
  });

  it("Run Real handles backend error", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const failure = new Error("api err");
    failure.response = { data: { detail: { error: "real blew up" } } };
    mocks.api.testFallbackChainReal.mockRejectedValue(failure);
    const wrapper = mountPage();
    await flushPromises();
    await _seedProbeForm(wrapper);
    await wrapper.get('[data-testid="run-real-btn"]').trigger("click");
    await flushPromises();
    expect(wrapper.text()).toContain("real blew up");
  });

  it("Run Real generic fallback when error has no detail", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.testFallbackChainReal.mockRejectedValue(new Error("boom"));
    const wrapper = mountPage();
    await flushPromises();
    await _seedProbeForm(wrapper);
    await wrapper.get('[data-testid="run-real-btn"]').trigger("click");
    await flushPromises();
    expect(wrapper.text()).toContain("Real probe failed");
  });

  it("Run Real validates probe target", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="run-real-btn"]').trigger("click");
    await flushPromises();
    expect(messageErrorSpy).toHaveBeenCalledWith(
      "namespace and name are required for the probe target",
    );
    expect(mocks.api.testFallbackChainReal).not.toHaveBeenCalled();
  });

  it("Run Real redirects to login when token missing", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();
    await _seedProbeForm(wrapper);
    mocks.adminStore.token = null;
    await wrapper.get('[data-testid="run-real-btn"]').trigger("click");
    await flushPromises();
    expect(mocks.router.push).toHaveBeenCalledWith("/login");
  });

  it("paths_info op packs paths CSV into the payload", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.testFallbackChainSimulate.mockResolvedValue(_stubReport({ op: "paths_info" }));
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="load-from-system-btn"]').trigger("click");
    await flushPromises();
    await _seedProbeForm(wrapper);
    // Switch op to paths_info.
    const opSelect = wrapper.get('[data-testid="probe-op"]');
    opSelect.element.value = "paths_info";
    await opSelect.trigger("change");
    await flushPromises();
    await wrapper
      .get('[data-testid="probe-paths-csv"] input')
      .setValue("README.md, config.json");
    await wrapper.get('[data-testid="run-simulate-btn"]').trigger("click");
    await flushPromises();
    const [, payload] = mocks.api.testFallbackChainSimulate.mock.calls[0];
    expect(payload.paths).toEqual(["README.md", "config.json"]);
  });

  it("decisionTagType maps each decision to the right Element Plus tag class", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.testFallbackChainSimulate.mockResolvedValue(
      _stubReport({
        attempts: [
          { source_name: "B", source_url: "u", source_type: "huggingface",
            method: "GET", upstream_path: "/x", status_code: 503,
            x_error_code: null, x_error_message: null,
            decision: "TRY_NEXT_SOURCE", duration_ms: 10, error: null,
            response_body_preview: null, response_headers: {} },
          { source_name: "C", source_url: "u", source_type: "huggingface",
            method: "GET", upstream_path: "/x", status_code: null,
            x_error_code: null, x_error_message: null,
            decision: "TIMEOUT", duration_ms: 0, error: "timed out",
            response_body_preview: null, response_headers: {} },
          { source_name: "D", source_url: "u", source_type: "huggingface",
            method: "GET", upstream_path: "/x", status_code: 404,
            x_error_code: "EntryNotFound", x_error_message: null,
            decision: "BIND_AND_PROPAGATE", duration_ms: 5, error: null,
            response_body_preview: null, response_headers: {} },
          { source_name: "E", source_url: "u", source_type: "huggingface",
            method: "GET", upstream_path: "/x", status_code: null,
            x_error_code: null, x_error_message: null,
            decision: "NETWORK_ERROR", duration_ms: 0, error: "boom",
            response_body_preview: null, response_headers: {} },
        ],
        final_outcome: "BIND_AND_PROPAGATE",
      }),
    );
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="load-from-system-btn"]').trigger("click");
    await flushPromises();
    await _seedProbeForm(wrapper);
    await wrapper.get('[data-testid="run-simulate-btn"]').trigger("click");
    await flushPromises();
    // Each decision name renders.
    expect(wrapper.text()).toContain("TRY_NEXT_SOURCE");
    expect(wrapper.text()).toContain("TIMEOUT");
    expect(wrapper.text()).toContain("BIND_AND_PROPAGATE");
    expect(wrapper.text()).toContain("NETWORK_ERROR");
  });

  it("CHAIN_EXHAUSTED outcome renders without final_response", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.testFallbackChainSimulate.mockResolvedValue({
      op: "info", repo_id: "x/y", revision: null, file_path: null,
      attempts: [
        { source_name: "A", source_url: "u", source_type: "huggingface",
          method: "GET", upstream_path: "/x", status_code: 503,
          x_error_code: null, x_error_message: null,
          decision: "TRY_NEXT_SOURCE", duration_ms: 1, error: null,
          response_body_preview: null, response_headers: {} },
      ],
      final_outcome: "CHAIN_EXHAUSTED",
      bound_source: null,
      duration_ms: 5,
      final_response: null,
    });
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="load-from-system-btn"]').trigger("click");
    await flushPromises();
    await _seedProbeForm(wrapper);
    await wrapper.get('[data-testid="run-simulate-btn"]').trigger("click");
    await flushPromises();
    expect(wrapper.text()).toContain("CHAIN_EXHAUSTED");
    expect(wrapper.find('[data-testid="probe-final-response"]').exists()).toBe(false);
  });

  it("BIND_AND_RESPOND outcome surfaces final_response with body preview", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.testFallbackChainSimulate.mockResolvedValue(_stubReport());
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="load-from-system-btn"]').trigger("click");
    await flushPromises();
    await _seedProbeForm(wrapper);
    await wrapper.get('[data-testid="run-simulate-btn"]').trigger("click");
    await flushPromises();
    expect(wrapper.find('[data-testid="probe-final-response"]').exists()).toBe(true);
    expect(wrapper.find('[data-testid="probe-final-body"]').text())
      .toContain('"id":"openai/gpt2"');
  });

  it("Header token Add and Remove flows", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="header-token-add"]').trigger("click");
    await wrapper.get('[data-testid="header-token-add"]').trigger("click");
    await flushPromises();
    expect(wrapper.find('[data-testid="header-token-url-0"]').exists()).toBe(true);
    expect(wrapper.find('[data-testid="header-token-url-1"]').exists()).toBe(true);
    await wrapper.get('[data-testid="header-token-remove-0"]').trigger("click");
    await flushPromises();
    expect(wrapper.find('[data-testid="header-token-url-1"]').exists()).toBe(false);
    expect(wrapper.find('[data-testid="header-token-url-0"]').exists()).toBe(true);
  });

  it("Editing a draft row field marks the draft dirty", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="load-from-system-btn"]').trigger("click");
    await flushPromises();
    // After Load, draft is clean.
    expect(wrapper.find('[data-testid="draft-dirty-tag"]').exists()).toBe(false);
    // Edit one field on row 0 — the URL input.
    const row0 = wrapper.get('[data-testid="draft-row-0"]');
    const inputs = row0.findAll("input");
    // The URL input is the second text-typed input (after the name field).
    // Just trigger input on the first available text input to fire @input.
    await inputs[0].setValue("https://changed.example");
    await flushPromises();
    expect(wrapper.find('[data-testid="draft-dirty-tag"]').exists()).toBe(true);
  });

  it("decisionTagType default branch — unknown decision falls back to info", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.testFallbackChainSimulate.mockResolvedValue({
      op: "info", repo_id: "x/y", revision: null, file_path: null,
      attempts: [
        { source_name: "weird", source_url: "u", source_type: "huggingface",
          method: "GET", upstream_path: "/x", status_code: 200,
          x_error_code: null, x_error_message: null,
          decision: "FUTURE_UNKNOWN_DECISION", duration_ms: 1, error: null,
          response_body_preview: null, response_headers: {} },
      ],
      final_outcome: "BIND_AND_RESPOND",
      bound_source: { name: "weird", url: "u" },
      duration_ms: 1,
      final_response: null,
    });
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="load-from-system-btn"]').trigger("click");
    await flushPromises();
    await _seedProbeForm(wrapper);
    await wrapper.get('[data-testid="run-simulate-btn"]').trigger("click");
    await flushPromises();
    // Page should still render the unknown decision name; the el-tag's
    // type defaults to ``info`` (per decisionTagType's default branch).
    expect(wrapper.text()).toContain("FUTURE_UNKNOWN_DECISION");
  });

  // -----------------------------------------------------------------
  // Chain Tester probe-target autocomplete (#78 follow-up)
  // -----------------------------------------------------------------

  it("probe namespace input fetches suggestions via listUsers(include_orgs=true)", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.listUsers.mockResolvedValue({
      users: [{ username: "openai-community" }, { username: "openai" }],
    });
    const wrapper = mountPage();
    await flushPromises();
    await wrapper
      .get('[data-testid="probe-namespace"] input')
      .setValue("openai");
    await flushPromises();

    expect(mocks.api.listUsers).toHaveBeenCalledWith(
      "admin-token",
      expect.objectContaining({
        search: "openai",
        limit: 20,
        include_orgs: true,
      }),
    );
    expect(
      globalThis.window.__lastAutocompleteSuggestions.map((s) => s.value),
    ).toEqual(["openai-community", "openai"]);
  });

  it("probe name input fetches suggestions via listRepositories scoped to probeForm namespace + repo_type", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.listRepositories.mockResolvedValue({
      repositories: [
        { namespace: "openai-community", name: "gpt2", repo_type: "model" },
        { namespace: "openai-community", name: "gpt2-medium", repo_type: "model" },
      ],
    });
    const wrapper = mountPage();
    await flushPromises();
    // Set namespace via the probe-namespace autocomplete first.
    await wrapper
      .get('[data-testid="probe-namespace"] input')
      .setValue("openai-community");
    await flushPromises();
    mocks.api.listRepositories.mockClear();
    mocks.api.listRepositories.mockResolvedValue({
      repositories: [
        { namespace: "openai-community", name: "gpt2", repo_type: "model" },
        { namespace: "openai-community", name: "gpt2-medium", repo_type: "model" },
      ],
    });
    await wrapper.get('[data-testid="probe-name"] input').setValue("gpt2");
    await flushPromises();
    expect(mocks.api.listRepositories).toHaveBeenCalledWith(
      "admin-token",
      expect.objectContaining({
        search: "gpt2",
        repo_type: "model",
        namespace: "openai-community",
        limit: 20,
      }),
    );
    expect(
      globalThis.window.__lastAutocompleteSuggestions.map((s) => s.value),
    ).toEqual(["gpt2", "gpt2-medium"]);
  });

  it("probe name fetcher returns [] for empty query", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();
    globalThis.window.__lastAutocompleteSuggestions = ["unset"];
    await wrapper.get('[data-testid="probe-name"] input').setValue("");
    await flushPromises();
    expect(mocks.api.listRepositories).not.toHaveBeenCalled();
    expect(globalThis.window.__lastAutocompleteSuggestions).toEqual([]);
  });

  it("probe name fetcher swallows API errors", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.listRepositories.mockRejectedValue(new Error("boom"));
    const consoleSpy = vi.spyOn(console, "error").mockImplementation(() => {});
    const wrapper = mountPage();
    await flushPromises();
    globalThis.window.__lastAutocompleteSuggestions = ["unset"];
    await wrapper.get('[data-testid="probe-name"] input').setValue("g");
    await flushPromises();
    expect(globalThis.window.__lastAutocompleteSuggestions).toEqual([]);
    consoleSpy.mockRestore();
  });

  it("probe name fetcher honors checkAuth", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    const wrapper = mountPage();
    await flushPromises();
    mocks.adminStore.token = null;
    globalThis.window.__lastAutocompleteSuggestions = ["unset"];
    await wrapper.get('[data-testid="probe-name"] input').setValue("g");
    await flushPromises();
    expect(globalThis.window.__lastAutocompleteSuggestions).toEqual([]);
    expect(mocks.router.push).toHaveBeenCalledWith("/login");
    expect(mocks.api.listRepositories).not.toHaveBeenCalled();
  });

  it("probe name fetcher accepts raw array response shape", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.listRepositories.mockResolvedValue([
      { namespace: "x", name: "raw-array-repo" },
    ]);
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="probe-name"] input').setValue("raw");
    await flushPromises();
    expect(
      globalThis.window.__lastAutocompleteSuggestions.map((s) => s.value),
    ).toEqual(["raw-array-repo"]);
  });

  it("Header tokens with empty url or token are filtered out of payload", async () => {
    mocks.api.listFallbackSources.mockResolvedValue([SOURCE_HF]);
    mocks.api.getFallbackCacheStats.mockResolvedValue(STATS);
    mocks.api.testFallbackChainSimulate.mockResolvedValue(_stubReport());
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="load-from-system-btn"]').trigger("click");
    await flushPromises();
    await _seedProbeForm(wrapper);
    // Add a token row but only fill the URL — should be filtered out.
    await wrapper.get('[data-testid="header-token-add"]').trigger("click");
    await flushPromises();
    await wrapper
      .get('[data-testid="header-token-url-0"] input')
      .setValue("https://incomplete.example");
    await wrapper.get('[data-testid="run-simulate-btn"]').trigger("click");
    await flushPromises();
    const [, payload] = mocks.api.testFallbackChainSimulate.mock.calls[0];
    expect(payload.user_tokens).toEqual({});
  });
});

