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
    cancelTask: vi.fn(),
    getTaskLogs: vi.fn(),
    downloadTaskLogs: vi.fn(),
    getTaskStats: vi.fn(),
    listWorkers: vi.fn(),
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
  cancelTask: (...args) => mocks.api.cancelTask(...args),
  getTaskLogs: (...args) => mocks.api.getTaskLogs(...args),
  downloadTaskLogs: (...args) => mocks.api.downloadTaskLogs(...args),
  getTaskStats: (...args) => mocks.api.getTaskStats(...args),
  listWorkers: (...args) => mocks.api.listWorkers(...args),
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
    cancel_requested: false,
    stalled: false,
    stall_seconds: 600,
    progress: null,
    last_error: null,
    created_at: "2026-09-25T09:59:00+00:00",
    started_at: null,
    finished_at: null,
    ...overrides,
  };
}

// What GET /tasks/{id} adds on top of a list row.
function detailOf(task, overrides = {}) {
  return {
    ...task,
    checkpoint: null,
    events: [],
    runs: [],
    log_lines: 0,
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
    counts: { queued: 1, running: 1, succeeded: 4, failed: 1, cancelled: 2 },
    kinds: ["repo.recalc", "tasks.cleanup"],
    limit: 20,
    offset: 0,
    ...extra,
  };
}

function statsResponse(overrides = {}) {
  return {
    window: "1h",
    window_seconds: 3600,
    bucket_seconds: 60,
    generated_at: "2026-09-25T10:00:00+00:00",
    health: {
      status: "degraded",
      reasons: [
        { level: "degraded", message: "1 task(s) failed in the window" },
      ],
    },
    thresholds: {
      backlog_warn_seconds: 60,
      backlog_critical_seconds: 300,
      failure_rate_warn: 0.1,
      failure_rate_critical: 0.5,
    },
    summary: {
      finished: 4,
      succeeded: 3,
      failed: 1,
      succeeded_after_retry: 0,
      failure_rate: 0.25,
      throughput_per_minute: 0.07,
      duration_p50: 2,
      duration_p95: 5,
      enqueued: 5,
    },
    backlog: {
      due: 1,
      scheduled: 0,
      retrying: 0,
      oldest_due_seconds: 12,
      running: 1,
      stuck: 0,
      active_workers: 1,
    },
    series: [
      {
        start: "2026-09-25T09:00:00+00:00",
        succeeded: 3,
        failed: 1,
        enqueued: 5,
      },
    ],
    kinds: [
      {
        kind: "repo.recalc",
        succeeded: 3,
        failed: 1,
        queued: 1,
        running: 1,
        stuck: 0,
        failure_rate: 0.25,
        duration_p95: 5,
        timeline: [{ succeeded: 3, failed: 1 }],
        last_error: "RuntimeError: boom",
        last_failed_at: "2026-09-25T09:30:00+00:00",
      },
    ],
    errors: [],
    ...overrides,
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

function rosterResponse(overrides = {}) {
  return {
    workers: [
      {
        id: "host-a:7:aaaa1111",
        name: "host-a",
        hostname: "host-a",
        pid: 7,
        queues: [],
        concurrency: 4,
        status: "online",
        running: 1,
        succeeded: 10,
        failed: 0,
        started_at: "2026-09-25T09:00:00+00:00",
        last_heartbeat_at: "2026-09-25T10:00:00+00:00",
        heartbeat_age_seconds: 3,
        stopped_at: null,
        tasks: [{ id: 2, kind: "repo.recalc", progress: null }],
      },
    ],
    counts: { online: 1, draining: 1, lost: 0, stopped: 0 },
    hidden: 0,
    lost_after_seconds: 30,
    inactive_after_seconds: 86400,
    ...overrides,
  };
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
    mocks.api.getTaskStats.mockResolvedValue(statsResponse());
    mocks.api.listWorkers.mockResolvedValue(rosterResponse());
    mocks.api.getTaskLogs.mockResolvedValue({
      lines: [],
      next_after_id: 0,
      has_more: false,
      status: "failed",
    });
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

    expect(
      wrapper.get('[data-testid="tasks-count-cancelled"]').text(),
    ).toContain("2");

    // Cancel while queued or running; retry and discard once it ended badly.
    const has = (id) => wrapper.find(`[data-testid="${id}"]`).exists();
    expect(has("tasks-cancel-3") && has("tasks-cancel-2")).toBe(true);
    expect(has("tasks-cancel-1")).toBe(false);
    expect(has("tasks-retry-1") && has("tasks-discard-1")).toBe(true);
    expect(has("tasks-retry-3") || has("tasks-discard-3")).toBe(false);
    expect(has("tasks-discard-2")).toBe(false);

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
    expect(wrapper.find('[data-testid="tasks-table"]').exists()).toBe(false);
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
    mocks.api.getTask.mockResolvedValue(detailOf(FAILED));
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
    mocks.api.getTask.mockResolvedValue(detailOf(shortError));
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

  it("offers only Cancel for a running task without an error", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    mocks.api.getTask.mockResolvedValue(detailOf(RUNNING));
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
    expect(wrapper.find('[data-testid="tasks-detail-cancel"]').exists()).toBe(
      true,
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
    mocks.api.getTask.mockResolvedValue(detailOf(FAILED));
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

  it("does nothing when the operator backs out of an action", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    messageBoxConfirmSpy.mockRejectedValue("cancel");
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="tasks-retry-1"]').trigger("click");
    await wrapper.get('[data-testid="tasks-discard-1"]').trigger("click");
    await wrapper.get('[data-testid="tasks-cancel-3"]').trigger("click");
    await flushPromises();

    expect(mocks.api.retryTask).not.toHaveBeenCalled();
    expect(mocks.api.deleteTask).not.toHaveBeenCalled();
    expect(mocks.api.cancelTask).not.toHaveBeenCalled();
  });

  it("discards a failed task and surfaces backend conflicts", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    mocks.api.deleteTask
      .mockResolvedValueOnce({ success: true, id: 1 })
      .mockRejectedValueOnce(
        httpError(409, {
          error: "Only failed or cancelled tasks can be discarded",
        }),
      );
    mocks.api.retryTask.mockRejectedValueOnce(new Error("network down"));
    messageBoxConfirmSpy.mockResolvedValue("confirm");
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="tasks-discard-1"]').trigger("click");
    await flushPromises();
    expect(mocks.api.deleteTask).toHaveBeenCalledWith("admin-token", 1);
    expect(messageSuccessSpy).toHaveBeenCalledWith("Task #1 discarded");

    await wrapper.get('[data-testid="tasks-discard-1"]').trigger("click");
    await flushPromises();
    expect(messageErrorSpy).toHaveBeenCalledWith(
      "Only failed or cancelled tasks can be discarded",
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
    expect(mocks.api.getTaskStats).not.toHaveBeenCalled();
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

  it("loads queue health on mount and shows the health chip", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    const wrapper = mountPage();
    await flushPromises();

    expect(mocks.api.getTaskStats).toHaveBeenCalledWith("admin-token", "1h");
    expect(wrapper.get('[data-testid="tasks-health"]').text()).toBe("Degraded");
    expect(wrapper.find('[data-testid="task-overview"]').exists()).toBe(true);
  });

  it("shows a placeholder until health loads and reports load errors", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    mocks.api.getTaskStats.mockRejectedValue(
      httpError(500, { error: "stats exploded" }),
    );
    const wrapper = mountPage();
    await flushPromises();

    expect(wrapper.find('[data-testid="tasks-health"]').exists()).toBe(false);
    expect(wrapper.find('[data-testid="task-overview"]').exists()).toBe(false);
    expect(
      wrapper.find('[data-description="Could not load task health"]').exists(),
    ).toBe(true);
    expect(messageErrorSpy).toHaveBeenCalledWith("stats exploded");
  });

  it("reloads health when the window changes", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="task-window-24h"]').trigger("click");
    await flushPromises();

    expect(mocks.api.getTaskStats).toHaveBeenLastCalledWith(
      "admin-token",
      "24h",
    );
  });

  it("opens the task list filtered by a kind or status picked on the overview", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    const wrapper = mountPage();
    await flushPromises();
    const tabs = () =>
      wrapper.get('[data-el-tabs="true"]').attributes("data-active");
    expect(tabs()).toBe("overview");
    // Emit from the overview directly: the ElTabs test stub re-selects the
    // pane a click lands in, which real Element Plus does not do.
    const overview = wrapper.findComponent({ name: "TaskOverview" });

    overview.vm.$emit("select-kind", "repo.recalc");
    await flushPromises();
    expect(tabs()).toBe("tasks");
    expect(mocks.api.listTasks).toHaveBeenLastCalledWith(
      "admin-token",
      expect.objectContaining({ kind: "repo.recalc", status: undefined }),
    );

    overview.vm.$emit("select-status", "running");
    await flushPromises();
    expect(mocks.api.listTasks).toHaveBeenLastCalledWith(
      "admin-token",
      expect.objectContaining({ kind: undefined, status: "running" }),
    );
  });

  it("switches to the task list when a status card is clicked from the overview", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    const wrapper = mountPage();
    await flushPromises();

    const card = wrapper.get('[data-testid="tasks-count-queued"]');
    expect(card.classes()).toContain("status-queued");
    expect(card.classes()).not.toContain("active");
    await card.trigger("click");
    await flushPromises();

    expect(wrapper.get('[data-el-tabs="true"]').attributes("data-active")).toBe(
      "tasks",
    );
    expect(card.classes()).toContain("active");
  });

  it("ignores a stats response for a window that is no longer selected", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    let resolveSlow;
    mocks.api.getTaskStats.mockImplementation((_token, window) =>
      window === "7d"
        ? new Promise((resolve) => {
            resolveSlow = resolve;
          })
        : Promise.resolve(statsResponse({ window })),
    );
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="task-window-7d"]').trigger("click");
    await wrapper.get('[data-testid="task-window-15m"]').trigger("click");
    await flushPromises();
    resolveSlow(
      statsResponse({
        window: "7d",
        summary: { ...statsResponse().summary, finished: 999 },
      }),
    );
    await flushPromises();

    expect(
      wrapper.get('[data-testid="task-kpi-finished"]').text(),
    ).not.toContain("999");
    expect(wrapper.get('[data-testid="task-window-15m"]').classes()).toContain(
      "active",
    );
  });

  it("refreshes queue health after a retry or discard", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    mocks.api.retryTask.mockResolvedValue({});
    mocks.api.deleteTask.mockResolvedValue({ success: true, id: 3 });
    messageBoxConfirmSpy.mockResolvedValue("confirm");
    const wrapper = mountPage();
    await flushPromises();
    expect(mocks.api.getTaskStats).toHaveBeenCalledTimes(1);

    await wrapper.get('[data-testid="tasks-retry-1"]').trigger("click");
    await flushPromises();
    expect(mocks.api.getTaskStats).toHaveBeenCalledTimes(2);

    await wrapper.get('[data-testid="tasks-discard-1"]').trigger("click");
    await flushPromises();
    expect(mocks.api.getTaskStats).toHaveBeenCalledTimes(3);
  });

  it("cancels a queued task and requests cancellation of a running one", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    mocks.api.cancelTask
      .mockResolvedValueOnce({ ...QUEUED, status: "cancelled" })
      .mockResolvedValueOnce({ ...RUNNING, cancel_requested: true })
      .mockRejectedValueOnce(
        httpError(409, { error: "cancellation was already requested" }),
      );
    messageBoxConfirmSpy.mockResolvedValue("confirm");
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="tasks-cancel-3"]').trigger("click");
    await flushPromises();
    expect(messageBoxConfirmSpy.mock.calls[0][0]).toContain("before it starts");
    expect(messageSuccessSpy).toHaveBeenCalledWith("Task #3 cancelled");

    await wrapper.get('[data-testid="tasks-cancel-2"]').trigger("click");
    await flushPromises();
    expect(messageBoxConfirmSpy.mock.calls[1][0]).toContain("grace period");
    expect(messageSuccessSpy).toHaveBeenCalledWith(
      "Cancellation of task #2 requested",
    );
    expect(mocks.api.listTasks).toHaveBeenCalledTimes(3);

    await wrapper.get('[data-testid="tasks-cancel-2"]').trigger("click");
    await flushPromises();
    expect(messageErrorSpy).toHaveBeenLastCalledWith(
      "cancellation was already requested",
    );
  });

  it("marks stalled and cancelling tasks and shows their progress", async () => {
    const stalled = makeTask({
      id: 5,
      status: "running",
      stalled: true,
      cancel_requested: true,
      progress: {
        done: 25,
        total: 100,
        stage: "copying",
        updated_at: null,
        eta_seconds: 90,
      },
    });
    mocks.api.listTasks.mockResolvedValue(listResponse([stalled]));
    const wrapper = mountPage();
    await flushPromises();

    expect(wrapper.find('[data-testid="tasks-stalled-5"]').exists()).toBe(true);
    expect(wrapper.find('[data-testid="tasks-cancelling-5"]').exists()).toBe(
      true,
    );
    // Cancellation was already requested, so it is not offered again.
    expect(wrapper.find('[data-testid="tasks-cancel-5"]').exists()).toBe(false);
    expect(wrapper.get('[data-testid="task-progress"]').text()).toContain(
      "ETA 1m 30s",
    );
  });

  it("keeps an open detail live on refresh and cancels from it", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    mocks.api.getTask
      .mockResolvedValueOnce(detailOf(RUNNING))
      .mockResolvedValueOnce(detailOf({ ...RUNNING, cancel_requested: true }))
      .mockRejectedValueOnce(new Error("gone"));
    mocks.api.cancelTask.mockResolvedValue({
      ...RUNNING,
      cancel_requested: true,
    });
    messageBoxConfirmSpy.mockResolvedValue("confirm");
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="tasks-detail-2"]').trigger("click");
    await flushPromises();
    await wrapper.get('[data-testid="tasks-detail-cancel"]').trigger("click");
    await flushPromises();
    expect(mocks.api.cancelTask).toHaveBeenCalledWith("admin-token", 2);
    // The refresh after cancelling reloaded the open detail.
    expect(mocks.api.getTask).toHaveBeenCalledTimes(2);
    expect(
      wrapper.find('[data-testid="tasks-detail-cancelling"]').exists(),
    ).toBe(true);

    // A task discarded meanwhile does not break the refresh.
    await wrapper.get('[data-testid="tasks-refresh"]').trigger("click");
    await flushPromises();
    expect(messageErrorSpy).not.toHaveBeenCalled();
    expect(wrapper.find('[data-testid="tasks-detail"]').exists()).toBe(true);
  });

  it("drops a detail reload that returns after the dialog was closed", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    let resolveReload;
    mocks.api.getTask
      .mockResolvedValueOnce(detailOf(FAILED))
      .mockImplementationOnce(
        () => new Promise((resolve) => (resolveReload = resolve)),
      );
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="tasks-detail-1"]').trigger("click");
    await flushPromises();

    await wrapper.get('[data-testid="tasks-refresh"]').trigger("click");
    wrapper
      .findComponent({ name: "ElDialog" })
      .vm.$emit("update:modelValue", false);
    resolveReload(detailOf({ ...FAILED, kind: "stale.kind" }));
    await flushPromises();

    expect(wrapper.find('[data-testid="tasks-detail"]').exists()).toBe(false);
  });

  it("reports log loading errors from the detail view", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    mocks.api.getTask.mockResolvedValue(detailOf(FAILED));
    mocks.api.getTaskLogs.mockRejectedValue(new Error("offline"));
    const wrapper = mountPage();
    await flushPromises();

    await wrapper.get('[data-testid="tasks-detail-1"]').trigger("click");
    await flushPromises();

    expect(messageErrorSpy).toHaveBeenCalledWith("Failed to load task logs");
  });

  it("offers a retry when queue health fails to load", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    mocks.api.getTaskStats
      .mockRejectedValueOnce(new Error("offline"))
      .mockResolvedValue(statsResponse());
    const wrapper = mountPage();
    await flushPromises();

    expect(messageErrorSpy).toHaveBeenCalledWith("Failed to load task health");
    const retry = wrapper.get('[data-testid="tasks-stats-retry"]');
    await retry.trigger("click");
    await flushPromises();

    expect(wrapper.find('[data-testid="task-overview"]').exists()).toBe(true);
    expect(wrapper.find('[data-testid="tasks-stats-retry"]').exists()).toBe(
      false,
    );
  });

  it("shows how many workers are alive and opens the roster", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    const wrapper = mountPage();
    await flushPromises();

    expect(mocks.api.listWorkers).toHaveBeenCalledWith("admin-token", {
      includeInactive: false,
    });
    const chip = wrapper.get('[data-testid="tasks-workers-chip"]');
    expect(chip.text()).toBe("2 worker(s) online"); // online + draining
    expect(chip.classes()).toContain("success");
    await chip.trigger("click");
    expect(wrapper.get('[data-el-tabs="true"]').attributes("data-active")).toBe(
      "workers",
    );
    expect(wrapper.find('[data-testid="task-workers"]').exists()).toBe(true);
  });

  it("flags when no worker is online", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    mocks.api.listWorkers.mockResolvedValue(
      rosterResponse({
        workers: [],
        counts: { online: 0, draining: 0, lost: 1, stopped: 0 },
      }),
    );
    const wrapper = mountPage();
    await flushPromises();

    const chip = wrapper.get('[data-testid="tasks-workers-chip"]');
    expect(chip.text()).toBe("0 worker(s) online");
    expect(chip.classes()).toContain("danger");
  });

  it("reloads the roster with inactive workers and opens a worker's task", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    mocks.api.getTask.mockResolvedValue(detailOf(RUNNING));
    const wrapper = mountPage();
    await flushPromises();

    wrapper
      .getComponent({ name: "TaskWorkers" })
      .vm.$emit("update:includeInactive", true);
    await flushPromises();
    expect(mocks.api.listWorkers).toHaveBeenLastCalledWith("admin-token", {
      includeInactive: true,
    });

    await wrapper.get('[data-testid="worker-task-2"]').trigger("click");
    await flushPromises();
    expect(mocks.api.getTask).toHaveBeenCalledWith("admin-token", 2);
    expect(wrapper.find('[data-testid="tasks-detail"]').exists()).toBe(true);
  });

  it("jumps from a task to the worker running it", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    mocks.api.getTask.mockResolvedValue(detailOf(RUNNING));
    const wrapper = mountPage();
    await flushPromises();
    await wrapper.get('[data-testid="tasks-detail-2"]').trigger("click");
    await flushPromises();

    await wrapper.get('[data-testid="tasks-detail-worker"]').trigger("click");
    await flushPromises();

    expect(wrapper.find('[data-testid="tasks-detail"]').exists()).toBe(false);
    expect(wrapper.get('[data-el-tabs="true"]').attributes("data-active")).toBe(
      "workers",
    );
    expect(
      wrapper.getComponent({ name: "TaskWorkers" }).props("highlight"),
    ).toBe("host:1:abcd");
  });

  it("reports a roster that fails to load", async () => {
    mocks.api.listTasks.mockResolvedValue(listResponse());
    mocks.api.listWorkers.mockRejectedValue(new Error("offline"));
    const wrapper = mountPage();
    await flushPromises();

    expect(messageErrorSpy).toHaveBeenCalledWith("Failed to load workers");
    expect(wrapper.find('[data-testid="tasks-workers-chip"]').exists()).toBe(
      false,
    );
  });
});
