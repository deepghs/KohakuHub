import { flushPromises, mount } from "@vue/test-utils";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ElementPlusStubs } from "../helpers/vue";

const api = vi.hoisted(() => ({
  getTaskLogs: vi.fn(),
  downloadTaskLogs: vi.fn(),
}));

vi.mock("@/utils/api", () => ({
  getTaskLogs: (...args) => api.getTaskLogs(...args),
  downloadTaskLogs: (...args) => api.downloadTaskLogs(...args),
}));

import TaskDetail from "@/components/tasks/TaskDetail.vue";
import TaskLogViewer from "@/components/tasks/TaskLogViewer.vue";
import TaskProgress from "@/components/tasks/TaskProgress.vue";
import {
  formatDate,
  formatSeconds,
  secondsBetween,
} from "@/components/tasks/taskFormat.js";

const stubs = { global: { stubs: ElementPlusStubs } };

function task(overrides = {}) {
  return {
    id: 7,
    kind: "repo.cleanup_storage",
    queue: "default",
    payload: { repo: 1 },
    status: "running",
    priority: 0,
    dedupe_key: null,
    attempts: 1,
    max_attempts: 5,
    run_after: "2026-09-25T10:00:00+00:00",
    locked_by: "host:1:abcd",
    locked_until: "2026-09-25T10:02:00+00:00",
    lease_expired: false,
    cancel_requested: false,
    stalled: false,
    stall_seconds: 600,
    progress: null,
    last_error: null,
    created_at: "2026-09-25T09:59:00+00:00",
    started_at: "2026-09-25T10:00:00+00:00",
    finished_at: null,
    checkpoint: null,
    events: [],
    runs: [],
    log_lines: 0,
    ...overrides,
  };
}

function progress(overrides = {}) {
  return {
    done: 40,
    total: 160,
    stage: "deleting objects",
    updated_at: "2026-09-25T10:01:00+00:00",
    eta_seconds: 3725,
    ...overrides,
  };
}

function logPage(lines, overrides = {}) {
  return {
    lines,
    next_after_id: lines.length ? lines[lines.length - 1].id : 0,
    has_more: false,
    status: "running",
    ...overrides,
  };
}

function line(id, overrides = {}) {
  return {
    id,
    attempt: 1,
    at: "2026-09-25T10:00:01.250+00:00",
    level: "INFO",
    message: `line ${id}`,
    ...overrides,
  };
}

describe("taskFormat", () => {
  it("formats durations, dates and spans", () => {
    expect(formatSeconds(null)).toBeNull();
    expect(formatSeconds(undefined)).toBeNull();
    expect(formatSeconds(44.6)).toBe("45s");
    expect(formatSeconds(200)).toBe("3m 20s");
    expect(formatSeconds(3900)).toBe("1h 5m");
    expect(formatDate(null)).toBe("—");
    expect(formatDate("2026-09-25T10:00:00")).toBe("2026-09-25 10:00:00");
    expect(secondsBetween(null, "x")).toBeNull();
    expect(secondsBetween("2026-09-25T10:00:00Z", "2026-09-25T10:01:30Z")).toBe(
      90,
    );
  });

  it("measures an open span up to now", () => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-09-25T10:00:10Z"));
    expect(secondsBetween("2026-09-25T10:00:00Z", null)).toBe(10);
    vi.useRealTimers();
  });
});

describe("TaskProgress", () => {
  it("shows a bar, counts, stage and ETA for a running task", () => {
    const wrapper = mount(TaskProgress, {
      props: { task: task({ progress: progress() }) },
      ...stubs,
    });

    expect(
      wrapper.get('[data-el-progress="true"]').attributes("data-percentage"),
    ).toBe("25");
    expect(wrapper.get('[data-testid="task-progress-count"]').text()).toBe(
      "40 / 160",
    );
    expect(wrapper.get('[data-testid="task-progress-eta"]').text()).toBe(
      "ETA 1h 2m",
    );
    expect(wrapper.get('[data-testid="task-progress-stage"]').text()).toBe(
      "deleting objects",
    );
  });

  it("counts without a bar when the total is unknown", () => {
    const wrapper = mount(TaskProgress, {
      props: {
        task: task({
          progress: progress({ total: null, eta_seconds: null, stage: null }),
        }),
        compact: true,
      },
      ...stubs,
    });

    expect(wrapper.find('[data-el-progress="true"]').exists()).toBe(false);
    expect(wrapper.text()).toContain("40 done");
    expect(wrapper.find('[data-testid="task-progress-eta"]').exists()).toBe(
      false,
    );
    expect(wrapper.find('[data-testid="task-progress-stage"]').exists()).toBe(
      false,
    );
  });

  it("colours the bar by outcome and hides the ETA once finished", () => {
    const bar = (overrides) =>
      mount(TaskProgress, {
        props: { task: task({ progress: progress(), ...overrides }) },
        ...stubs,
      });

    expect(
      bar({ status: "succeeded" })
        .get('[data-el-progress="true"]')
        .attributes("data-status"),
    ).toBe("success");
    expect(
      bar({ status: "failed" })
        .get('[data-el-progress="true"]')
        .attributes("data-status"),
    ).toBe("exception");
    expect(
      bar({ status: "cancelled" })
        .get('[data-el-progress="true"]')
        .attributes("data-status"),
    ).toBe("warning");
    expect(
      bar({ stalled: true })
        .get('[data-el-progress="true"]')
        .attributes("data-status"),
    ).toBe("warning");
    expect(
      bar({ status: "succeeded" })
        .find('[data-testid="task-progress-eta"]')
        .exists(),
    ).toBe(false);
  });

  it("caps the bar at 100% and shows a stage-only report", () => {
    const over = mount(TaskProgress, {
      props: { task: task({ progress: progress({ done: 200, total: 160 }) }) },
      ...stubs,
    });
    expect(
      over.get('[data-el-progress="true"]').attributes("data-percentage"),
    ).toBe("100");

    const stageOnly = mount(TaskProgress, {
      props: {
        task: task({ progress: progress({ done: null, total: null }) }),
      },
      ...stubs,
    });
    expect(stageOnly.find('[data-testid="task-progress-count"]').exists()).toBe(
      false,
    );
    expect(stageOnly.text()).toContain("deleting objects");
  });

  it("renders a dash without progress", () => {
    const wrapper = mount(TaskProgress, { props: { task: task() }, ...stubs });
    expect(wrapper.text()).toBe("—");
  });
});

describe("TaskDetail", () => {
  beforeEach(() => {
    api.getTaskLogs.mockReset();
    api.getTaskLogs.mockResolvedValue(logPage([]));
  });

  const event = (id, type, attempt, detail = {}, worker = "w-1") => ({
    id,
    type,
    attempt,
    worker,
    detail,
    at: "2026-09-25T10:00:00+00:00",
  });

  it("describes every event type in the timeline", () => {
    const events = [
      event(1, "created", 0, {}, null),
      event(2, "claimed", 1),
      event(3, "stage", 1, { stage: "listing" }),
      event(4, "retry_scheduled", 1, {
        error: "RuntimeError: flaky",
        run_after: "2026-09-25T10:00:05+00:00",
      }),
      event(5, "claimed", 2, {}, null),
      event(6, "released", 2),
      event(7, "lease_expired", 2),
      event(8, "cancel_requested", 2),
      event(9, "cancelled", 2),
      event(10, "cancelled", 0, { by: "admin" }, null),
      event(11, "retried", 2, { by: "admin" }, null),
      event(12, "succeeded", 3),
      event(13, "failed", 4, { error: "KeyError: 'x'" }),
      event(14, "something_new", 4),
    ];
    const wrapper = mount(TaskDetail, {
      props: { task: task({ events }), token: "t" },
      ...stubs,
    });

    const items = wrapper.findAll('[data-testid="tasks-detail-timeline"] li');
    const texts = items.map((item) => item.get(".timeline-text").text());
    expect(texts).toEqual([
      "Created",
      "Attempt 1 started on w-1",
      "Stage: listing",
      `Attempt 1 failed; retry at ${formatDate("2026-09-25T10:00:05+00:00")}`,
      "Attempt 2 started",
      "Attempt 2 handed back (worker shutting down)",
      "Lease of attempt 2 expired on w-1; reclaimed",
      "Cancellation requested",
      "Attempt 2 stopped by cancellation",
      "Cancelled while queued",
      "Requeued by an admin",
      "Attempt 3 succeeded",
      "Attempt 4 failed; no retries left",
      "something_new",
    ]);
    expect(items[3].classes()).toContain("warning");
    expect(items[12].classes()).toContain("danger");
    expect(items[13].classes()).toContain("muted");
    expect(items[3].text()).toContain("RuntimeError: flaky");
  });

  it("lists attempts with outcome, duration, logs and stages or error", () => {
    const runs = [
      {
        attempt: 1,
        worker: "w-1",
        started_at: "2026-09-25T10:00:00+00:00",
        finished_at: "2026-09-25T10:00:30+00:00",
        outcome: "failed",
        error: "RuntimeError: flaky",
        stages: [],
        log_lines: 12,
      },
      {
        attempt: 2,
        worker: "w-2",
        started_at: "2026-09-25T10:01:00+00:00",
        finished_at: "2026-09-25T10:04:20+00:00",
        outcome: "succeeded",
        error: null,
        stages: [{ stage: "listing" }, { stage: "deleting" }],
        log_lines: 3,
      },
      {
        attempt: 3,
        worker: "w-3",
        started_at: "2026-09-25T10:05:00+00:00",
        finished_at: "2026-09-25T10:05:01+00:00",
        outcome: "released",
        error: null,
        stages: [],
        log_lines: 0,
      },
    ];
    const wrapper = mount(TaskDetail, {
      props: { task: task({ runs, status: "succeeded" }), token: "t" },
      ...stubs,
    });

    const rows = wrapper
      .get('[data-testid="tasks-detail-attempts"]')
      .findAll("tr");
    expect(rows).toHaveLength(3);
    expect(rows[0].text()).toContain("30s");
    expect(rows[0].text()).toContain("RuntimeError: flaky");
    expect(rows[1].text()).toContain("3m 20s");
    expect(rows[1].text()).toContain("listing → deleting");
    expect(rows[2].text()).toContain("—");
    expect(
      wrapper.find('[data-testid="tasks-detail-no-attempts"]').exists(),
    ).toBe(false);
  });

  it("shows progress, badges, checkpoint and an empty attempt list", () => {
    const wrapper = mount(TaskDetail, {
      props: {
        task: task({
          progress: progress(),
          stalled: true,
          cancel_requested: true,
          stall_seconds: null,
          checkpoint: { cursor: "b/7" },
          last_error: "RuntimeError: earlier",
        }),
        token: "t",
      },
      ...stubs,
    });

    expect(wrapper.find('[data-testid="task-progress"]').exists()).toBe(true);
    expect(wrapper.find('[data-testid="tasks-detail-stalled"]').exists()).toBe(
      true,
    );
    expect(
      wrapper.find('[data-testid="tasks-detail-cancelling"]').exists(),
    ).toBe(true);
    expect(
      wrapper.get('[data-testid="tasks-detail-checkpoint"]').text(),
    ).toContain('"cursor": "b/7"');
    expect(wrapper.text()).toContain("never"); // stall detection disabled
    expect(wrapper.text()).toContain("1 / 5"); // attempts used, not the run list
    expect(wrapper.text()).toContain("RuntimeError: earlier");
    expect(
      wrapper.find('[data-testid="tasks-detail-no-attempts"]').exists(),
    ).toBe(true);
  });

  it("switches between its tabs", async () => {
    const wrapper = mount(TaskDetail, {
      props: { task: task(), token: "t" },
      ...stubs,
    });
    const tabs = wrapper.get('[data-el-tabs="true"]');
    expect(tabs.attributes("data-active")).toBe("overview");

    await wrapper.get('[data-tab="logs"]').trigger("click");
    expect(tabs.attributes("data-active")).toBe("logs");
  });

  it("passes log errors up", async () => {
    api.getTaskLogs.mockRejectedValue(new Error("offline"));
    const wrapper = mount(TaskDetail, {
      props: { task: task(), token: "t" },
      ...stubs,
    });
    await flushPromises();

    expect(wrapper.emitted("error")[0][0].message).toBe("offline");
  });
});

describe("TaskLogViewer", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    api.getTaskLogs.mockReset();
    api.downloadTaskLogs.mockReset();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  function mountViewer(props = {}) {
    return mount(TaskLogViewer, {
      props: {
        token: "t",
        taskId: 7,
        running: false,
        attempts: [1, 2],
        ...props,
      },
      ...stubs,
    });
  }

  it("loads every page and renders levels", async () => {
    api.getTaskLogs
      .mockResolvedValueOnce(logPage([line(1), line(2)], { has_more: true }))
      .mockResolvedValueOnce(
        logPage([line(3, { level: "ERROR", message: "Traceback ..." })]),
      );
    const wrapper = mountViewer();
    await flushPromises();

    expect(api.getTaskLogs).toHaveBeenNthCalledWith(1, "t", 7, {
      attempt: undefined,
      afterId: 0,
      limit: 1000,
    });
    expect(api.getTaskLogs).toHaveBeenNthCalledWith(2, "t", 7, {
      attempt: undefined,
      afterId: 2,
      limit: 1000,
    });
    expect(wrapper.get('[data-testid="task-logs-count"]').text()).toBe(
      "3 line(s)",
    );
    const lines = wrapper.findAll(".log-line");
    expect(lines[2].classes()).toContain("level-error");
    expect(lines[2].text()).toContain("Traceback ...");
  });

  it("shows an empty state and loads new records on demand", async () => {
    api.getTaskLogs
      .mockResolvedValueOnce(logPage([]))
      .mockResolvedValueOnce(logPage([line(1)]));
    const wrapper = mountViewer();
    await flushPromises();
    expect(wrapper.text()).toContain("No log records captured.");

    await wrapper.get('[data-testid="task-logs-refresh"]').trigger("click");
    await flushPromises();
    expect(wrapper.findAll(".log-line")).toHaveLength(1);
  });

  it("tails while the task runs and stops when it ends or follow is off", async () => {
    api.getTaskLogs.mockResolvedValue(logPage([]));
    const wrapper = mountViewer({ running: true });
    await flushPromises();
    expect(api.getTaskLogs).toHaveBeenCalledTimes(1);

    await vi.advanceTimersByTimeAsync(2000);
    expect(api.getTaskLogs).toHaveBeenCalledTimes(2);

    wrapper
      .getComponent({ name: "ElCheckbox" })
      .vm.$emit("update:modelValue", false);
    await flushPromises();
    await vi.advanceTimersByTimeAsync(4000);
    expect(api.getTaskLogs).toHaveBeenCalledTimes(2);

    wrapper
      .getComponent({ name: "ElCheckbox" })
      .vm.$emit("update:modelValue", true);
    await flushPromises();
    await wrapper.setProps({ running: false });
    await vi.advanceTimersByTimeAsync(4000);
    expect(api.getTaskLogs).toHaveBeenCalledTimes(2);
    wrapper.unmount();
  });

  it("skips a tail tick while a load is still in flight", async () => {
    let finish;
    api.getTaskLogs
      .mockResolvedValueOnce(logPage([]))
      .mockImplementationOnce(
        () => new Promise((resolve) => (finish = resolve)),
      )
      .mockResolvedValue(logPage([]));
    const wrapper = mountViewer({ running: true });
    await flushPromises();

    await vi.advanceTimersByTimeAsync(2000); // starts the slow load
    await vi.advanceTimersByTimeAsync(2000); // skipped: still loading
    expect(api.getTaskLogs).toHaveBeenCalledTimes(2);
    finish(logPage([line(1)]));
    await flushPromises();
    await vi.advanceTimersByTimeAsync(2000);
    expect(api.getTaskLogs).toHaveBeenCalledTimes(3);
    wrapper.unmount();
  });

  it("restarts from the beginning when the attempt filter changes", async () => {
    let finishStale;
    api.getTaskLogs
      .mockImplementationOnce(
        () => new Promise((resolve) => (finishStale = resolve)),
      )
      .mockResolvedValueOnce(logPage([line(5, { attempt: 2 })]));
    const wrapper = mountViewer();
    await flushPromises();

    wrapper.getComponent({ name: "ElSelect" }).vm.$emit("update:modelValue", 2);
    await flushPromises();
    // The first request answers late; its lines belong to the old filter.
    finishStale(logPage([line(1), line(2)]));
    await flushPromises();

    expect(api.getTaskLogs).toHaveBeenLastCalledWith("t", 7, {
      attempt: 2,
      afterId: 0,
      limit: 1000,
    });
    expect(wrapper.findAll(".log-line").map((l) => l.text())).toEqual([
      expect.stringContaining("line 5"),
    ]);
    expect(
      wrapper
        .get('[data-testid="task-logs-refresh"]')
        .attributes("data-loading"),
    ).toBe("false");
  });

  it("restarts for a different task", async () => {
    api.getTaskLogs.mockResolvedValue(logPage([line(1)]));
    const wrapper = mountViewer();
    await flushPromises();

    await wrapper.setProps({ taskId: 8 });
    await flushPromises();

    expect(api.getTaskLogs).toHaveBeenLastCalledWith("t", 8, {
      attempt: undefined,
      afterId: 0,
      limit: 1000,
    });
    expect(wrapper.findAll(".log-line")).toHaveLength(1);
  });

  it("keeps the scroll position when not following", async () => {
    api.getTaskLogs.mockResolvedValueOnce(logPage([]));
    const wrapper = mountViewer();
    await flushPromises();
    wrapper
      .getComponent({ name: "ElCheckbox" })
      .vm.$emit("update:modelValue", false);
    const viewport = wrapper.get('[data-testid="task-logs-lines"]').element;
    viewport.scrollTop = 0;
    api.getTaskLogs.mockResolvedValueOnce(logPage([line(1)]));

    await wrapper.get('[data-testid="task-logs-refresh"]').trigger("click");
    await flushPromises();
    expect(viewport.scrollTop).toBe(0);
  });

  it("downloads the whole log or one attempt's", async () => {
    api.getTaskLogs.mockResolvedValue(logPage([]));
    api.downloadTaskLogs.mockResolvedValue(new Blob(["log"]));
    const createObjectURL = vi.fn(() => "blob:log");
    const revokeObjectURL = vi.fn();
    vi.stubGlobal("URL", { createObjectURL, revokeObjectURL });
    const click = vi
      .spyOn(HTMLAnchorElement.prototype, "click")
      .mockImplementation(() => {});
    const wrapper = mountViewer();
    await flushPromises();

    await wrapper.get('[data-testid="task-logs-download"]').trigger("click");
    await flushPromises();
    expect(api.downloadTaskLogs).toHaveBeenLastCalledWith("t", 7, undefined);
    expect(click.mock.contexts[0].download).toBe("task-7.log");

    wrapper.getComponent({ name: "ElSelect" }).vm.$emit("update:modelValue", 2);
    await flushPromises();
    await wrapper.get('[data-testid="task-logs-download"]').trigger("click");
    await flushPromises();
    expect(api.downloadTaskLogs).toHaveBeenLastCalledWith("t", 7, 2);
    expect(click.mock.contexts[1].download).toBe("task-7-attempt-2.log");
    expect(revokeObjectURL).toHaveBeenCalledWith("blob:log");

    click.mockRestore();
    vi.unstubAllGlobals();
  });

  it("reports download errors", async () => {
    api.getTaskLogs.mockResolvedValue(logPage([]));
    api.downloadTaskLogs.mockRejectedValue(new Error("forbidden"));
    const wrapper = mountViewer();
    await flushPromises();

    await wrapper.get('[data-testid="task-logs-download"]').trigger("click");
    await flushPromises();

    expect(wrapper.emitted("error")[0][0].message).toBe("forbidden");
    expect(
      wrapper
        .get('[data-testid="task-logs-download"]')
        .attributes("data-loading"),
    ).toBe("false");
  });
});
