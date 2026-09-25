import { defineComponent, h } from "vue";
import { flushPromises, mount } from "@vue/test-utils";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ElementPlusStubs } from "../helpers/vue";

const mocks = vi.hoisted(() => ({
  router: { push: vi.fn() },
  adminStore: { token: "admin-token", logout: vi.fn() },
  api: {
    listTasks: vi.fn(),
    getTask: vi.fn(),
    retryTask: vi.fn(),
    deleteTask: vi.fn(),
  },
}));

vi.mock("vue-router", () => ({
  useRouter: () => mocks.router,
}));

vi.mock("@/stores/admin", () => ({
  useAdminStore: () => mocks.adminStore,
}));

vi.mock("@/utils/api", () => ({
  listTasks: (...args) => mocks.api.listTasks(...args),
  getTask: (...args) => mocks.api.getTask(...args),
  retryTask: (...args) => mocks.api.retryTask(...args),
  deleteTask: (...args) => mocks.api.deleteTask(...args),
}));

vi.mock("@/components/AdminLayout.vue", () => ({
  default: defineComponent({
    name: "AdminLayoutStub",
    setup(_, { slots }) {
      return () =>
        h("div", { "data-testid": "admin-layout" }, slots.default?.());
    },
  }),
}));

// See test_cache_page.test.js for why element-plus is resolved lazily and
// its message helpers are spied on instead of mocked.
import TasksPage from "@/pages/tasks.vue";

const messageBoxConfirmSpy = vi.fn();
const messageSuccessSpy = vi.fn();
const messageErrorSpy = vi.fn();
let elementPlusModule;

function makeTask(overrides = {}) {
  return {
    id: 1,
    kind: "tasks.cleanup",
    queue: "default",
    payload: {},
    status: "queued",
    priority: 0,
    dedupe_key: null,
    attempts: 0,
    max_attempts: 5,
    run_after: "2026-09-25T10:00:00+00:00",
    locked_by: null,
    locked_until: null,
    lease_expired: false,
    last_error: null,
    created_at: "2026-09-25T09:59:00+00:00",
    started_at: null,
    finished_at: null,
    ...overrides,
  };
}

const QUEUED = makeTask({ id: 3 });
const RUNNING = makeTask({
  id: 2,
  kind: "repo.recalc",
  status: "running",
  attempts: 1,
  started_at: "2026-09-25T10:01:00+00:00",
  locked_by: "host:1:abcd",
  locked_until: "2026-09-25T10:02:00+00:00",
  lease_expired: true,
});
const FAILED = makeTask({
  id: 1,
  kind: "repo.recalc",
  status: "failed",
  attempts: 5,
  payload: { repo_id: 42 },
  last_error: "RuntimeError: " + "x".repeat(120),
  finished_at: "2026-09-25T10:03:00+00:00",
});

function listResponse(tasks = [QUEUED, RUNNING, FAILED], extra = {}) {
  return {
    tasks,
    total: tasks.length,
    counts: { queued: 1, running: 1, succeeded: 4, failed: 1 },
    kinds: ["repo.recalc", "tasks.cleanup"],
    limit: 20,
    offset: 0,
    ...extra,
  };
}

const noopDirective = { mounted() {}, updated() {}, beforeUnmount() {} };

function mountPage() {
  return mount(TasksPage, {
    global: {
      stubs: ElementPlusStubs,
      directives: { loading: noopDirective },
    },
  });
}

function httpError(status, detail) {
  const error = new Error(`HTTP ${status}`);
  error.response = { status, data: { detail } };
  return error;
}

describe("admin background tasks page", () => {
  beforeEach(async () => {
    vi.useFakeTimers();
    mocks.router.push.mockReset();
    mocks.adminStore.logout.mockReset();
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
    vi.useRealTimers();
  });

  it("loads tasks on mount and renders counts, rows, and state-specific cells", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    const wrapper = mountPage();
    await flushPromises();

    expect(mocks.api.listTasks).toHaveBeenCalledWith("admin-token", {
      status: undefined,
      kind: undefined,
      limit: 20,
      offset: 0,
    });
    expect(
      wrapper.get('[data-testid="tasks-count-succeeded"]').text(),
    ).toContain("4");
    const table = wrapper.get('[data-el-table="true"]');
    expect(table.attributes("data-row-count")).toBe("3");

    const text = table.text();
    expect(text).toContain("due 2026-09-25");
    expect(text).toContain("started 2026-09-25");
    expect(text).toContain("finished 2026-09-25");
    expect(text).toContain("0 / 5");
    expect(text).toContain("…"); // long error is truncated
    expect(wrapper.findAll('[data-testid="tasks-lease-expired"]')).toHaveLength(
      1,
    );

    // Retry only for failed; discard for queued and failed; never for running.
    expect(wrapper.find('[data-testid="tasks-retry-1"]').exists()).toBe(true);
    expect(wrapper.find('[data-testid="tasks-retry-3"]').exists()).toBe(false);
    expect(wrapper.find('[data-testid="tasks-discard-3"]').exists()).toBe(true);
    expect(wrapper.find('[data-testid="tasks-discard-2"]').exists()).toBe(
      false,
    );

    // Kind filter options come from the response.
    const kindSelect = wrapper.get('[data-testid="tasks-filter-kind"]');
    expect(kindSelect.text()).toContain("repo.recalc");
  });

  it("shows the empty state when there are no tasks", async () => {
    mocks.api.listTasks.mockResolvedValue(
      listResponse([], {
        counts: { queued: 0, running: 0, succeeded: 0, failed: 0 },
        kinds: [],
      }),
    );
    const wrapper = mountPage();
    await flushPromises();

    expect(wrapper.find('[data-testid="tasks-empty"]').exists()).toBe(true);
    expect(wrapper.find('[data-el-table="true"]').exists()).toBe(false);
  });

  it("filters by status card toggle, status select, and kind select", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="tasks-count-failed"]').trigger("click");
    await flushPromises();
    expect(mocks.api.listTasks).toHaveBeenLastCalledWith(
      "admin-token",
      expect.objectContaining({ status: "failed", offset: 0 }),
    );
    expect(
      wrapper.get('[data-testid="tasks-count-failed"]').classes(),
    ).toContain("active");

    // Clicking the active card clears the filter.
    await wrapper.get('[data-testid="tasks-count-failed"]').trigger("click");
    await flushPromises();
    expect(mocks.api.listTasks).toHaveBeenLastCalledWith(
      "admin-token",
      expect.objectContaining({ status: undefined }),
    );

    await wrapper
      .get('[data-testid="tasks-filter-status"]')
      .setValue("running");
    await flushPromises();
    expect(mocks.api.listTasks).toHaveBeenLastCalledWith(
      "admin-token",
      expect.objectContaining({ status: "running" }),
    );

    await wrapper
      .get('[data-testid="tasks-filter-kind"]')
      .setValue("tasks.cleanup");
    await flushPromises();
    expect(mocks.api.listTasks).toHaveBeenLastCalledWith(
      "admin-token",
      expect.objectContaining({ status: "running", kind: "tasks.cleanup" }),
    );
  });

  it("paginates through the pagination control", async () => {
    mocks.api.listTasks.mockResolvedValue(
      listResponse([QUEUED], { total: 45 }),
    );
    const wrapper = mountPage();
    await flushPromises();

    wrapper
      .findComponent({ name: "ElPagination" })
      .vm.$emit("current-change", 3);
    await flushPromises();

    expect(mocks.api.listTasks).toHaveBeenLastCalledWith(
      "admin-token",
      expect.objectContaining({ limit: 20, offset: 40 }),
    );
  });

  it("opens the detail dialog with payload and error, and closes it", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    mocks.api.getTask.mockResolvedValue(FAILED);
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="tasks-detail-1"]').trigger("click");
    await flushPromises();

    expect(mocks.api.getTask).toHaveBeenCalledWith("admin-token", 1);
    const detail = wrapper.get('[data-testid="tasks-detail"]');
    expect(detail.text()).toContain('"repo_id": 42');
    expect(detail.text()).toContain("RuntimeError");
    expect(
      wrapper.find('[data-testid="tasks-detail-lease-expired"]').exists(),
    ).toBe(false);
    expect(detail.text()).toContain("—"); // empty fields render a dash
    expect(wrapper.find('[data-testid="tasks-detail-retry"]').exists()).toBe(
      true,
    );
    expect(wrapper.find('[data-testid="tasks-detail-discard"]').exists()).toBe(
      true,
    );

    const close = wrapper
      .findAll("footer button")
      .find((b) => b.text() === "Close");
    await close.trigger("click");
    expect(wrapper.find('[data-testid="tasks-detail"]').exists()).toBe(false);
  });

  it("shows short errors untruncated and closes the dialog via v-model", async () => {
    const shortError = makeTask({
      id: 9,
      status: "failed",
      last_error: "Timed out after 5s",
    });
    mocks.api.listTasks.mockResolvedValue(listResponse([shortError]));
    mocks.api.getTask.mockResolvedValue(shortError);
    const wrapper = mountPage();
    await flushPromises();

    expect(wrapper.get('[data-el-table="true"]').text()).toContain(
      "Timed out after 5s",
    );
    expect(wrapper.get('[data-el-table="true"]').text()).not.toContain("…");

    await wrapper.get('[data-testid="tasks-detail-9"]').trigger("click");
    await flushPromises();
    wrapper
      .findComponent({ name: "ElDialog" })
      .vm.$emit("update:modelValue", false);
    await flushPromises();
    expect(wrapper.find('[data-testid="tasks-detail"]').exists()).toBe(false);
  });

  it("hides dialog actions for a running task without an error", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    mocks.api.getTask.mockResolvedValue(RUNNING);
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="tasks-detail-2"]').trigger("click");
    await flushPromises();

    expect(wrapper.get('[data-testid="tasks-detail"]').text()).not.toContain(
      "Last error",
    );
    expect(
      wrapper.find('[data-testid="tasks-detail-lease-expired"]').exists(),
    ).toBe(true);
    expect(wrapper.find('[data-testid="tasks-detail-retry"]').exists()).toBe(
      false,
    );
    expect(wrapper.find('[data-testid="tasks-detail-discard"]').exists()).toBe(
      false,
    );
  });

  it("reports a failure to load task detail", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    mocks.api.getTask.mockRejectedValue(
      httpError(404, { error: "Task not found: 1" }),
    );
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="tasks-detail-1"]').trigger("click");
    await flushPromises();

    expect(messageErrorSpy).toHaveBeenCalledWith("Task not found: 1");
  });

  it("retries a failed task after confirmation and reloads", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    mocks.api.retryTask.mockResolvedValue({ ...FAILED, status: "queued" });
    messageBoxConfirmSpy.mockResolvedValue("confirm");
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="tasks-retry-1"]').trigger("click");
    await flushPromises();

    expect(mocks.api.retryTask).toHaveBeenCalledWith("admin-token", 1);
    expect(messageSuccessSpy).toHaveBeenCalledWith("Task #1 requeued");
    expect(mocks.api.listTasks).toHaveBeenCalledTimes(2);
  });

  it("retries and discards from the detail dialog", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    mocks.api.getTask.mockResolvedValue(FAILED);
    mocks.api.retryTask.mockResolvedValue({});
    mocks.api.deleteTask.mockResolvedValue({ success: true, id: 1 });
    messageBoxConfirmSpy.mockResolvedValue("confirm");
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="tasks-detail-1"]').trigger("click");
    await flushPromises();
    await wrapper.get('[data-testid="tasks-detail-retry"]').trigger("click");
    await flushPromises();
    expect(mocks.api.retryTask).toHaveBeenCalledWith("admin-token", 1);
    expect(wrapper.find('[data-testid="tasks-detail"]').exists()).toBe(false);

    await wrapper.get('[data-testid="tasks-detail-1"]').trigger("click");
    await flushPromises();
    await wrapper.get('[data-testid="tasks-detail-discard"]').trigger("click");
    await flushPromises();
    expect(mocks.api.deleteTask).toHaveBeenCalledWith("admin-token", 1);
  });

  it("does nothing when the operator cancels retry or discard", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    messageBoxConfirmSpy.mockRejectedValue("cancel");
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="tasks-retry-1"]').trigger("click");
    await wrapper.get('[data-testid="tasks-discard-3"]').trigger("click");
    await flushPromises();

    expect(mocks.api.retryTask).not.toHaveBeenCalled();
    expect(mocks.api.deleteTask).not.toHaveBeenCalled();
  });

  it("discards a queued task and surfaces backend conflicts", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    mocks.api.deleteTask
      .mockResolvedValueOnce({ success: true, id: 3 })
      .mockRejectedValueOnce(
        httpError(409, {
          error:
            "Only queued or failed tasks can be discarded (task is running)",
        }),
      );
    mocks.api.retryTask.mockRejectedValueOnce(new Error("network down"));
    messageBoxConfirmSpy.mockResolvedValue("confirm");
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="tasks-discard-3"]').trigger("click");
    await flushPromises();
    expect(mocks.api.deleteTask).toHaveBeenCalledWith("admin-token", 3);
    expect(messageSuccessSpy).toHaveBeenCalledWith("Task #3 discarded");

    await wrapper.get('[data-testid="tasks-discard-1"]').trigger("click");
    await flushPromises();
    expect(messageErrorSpy).toHaveBeenCalledWith(
      "Only queued or failed tasks can be discarded (task is running)",
    );

    await wrapper.get('[data-testid="tasks-retry-1"]').trigger("click");
    await flushPromises();
    expect(messageErrorSpy).toHaveBeenLastCalledWith("Failed to retry task");
  });

  it("shows a generic error when loading fails", async () => {
    mocks.api.listTasks.mockRejectedValue(new Error("boom"));
    mountPage();
    await flushPromises();

    expect(messageErrorSpy).toHaveBeenCalledWith(
      "Failed to load background tasks",
    );
    expect(mocks.adminStore.logout).not.toHaveBeenCalled();
  });

  it.each([401, 403])("logs the operator out on HTTP %i", async (status) => {
    mocks.api.listTasks.mockRejectedValue(httpError(status, "denied"));
    mountPage();
    await flushPromises();

    expect(mocks.adminStore.logout).toHaveBeenCalledTimes(1);
    expect(mocks.router.push).toHaveBeenCalledWith("/login");
  });

  it("redirects to /login without an admin token", async () => {
    mocks.adminStore.token = "";
    mountPage();
    await flushPromises();

    expect(mocks.router.push).toHaveBeenCalledWith("/login");
    expect(mocks.api.listTasks).not.toHaveBeenCalled();
  });

  it("auto-refreshes on the selected interval and stops on unmount", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="tasks-refresh-interval"]').setValue(5);
    await flushPromises();
    vi.advanceTimersByTime(10_000);
    await flushPromises();
    expect(mocks.api.listTasks).toHaveBeenCalledTimes(3);

    await wrapper.get('[data-testid="tasks-refresh-interval"]').setValue(0);
    await flushPromises();
    vi.advanceTimersByTime(30_000);
    await flushPromises();
    expect(mocks.api.listTasks).toHaveBeenCalledTimes(3);

    await wrapper.get('[data-testid="tasks-refresh-interval"]').setValue(15);
    wrapper.unmount();
    vi.advanceTimersByTime(60_000);
    await flushPromises();
    expect(mocks.api.listTasks).toHaveBeenCalledTimes(3);
  });

  it("reloads when Refresh is clicked", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="tasks-refresh"]').trigger("click");
    await flushPromises();

    expect(mocks.api.listTasks).toHaveBeenCalledTimes(2);
  });
});
