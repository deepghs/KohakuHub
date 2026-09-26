import { mount } from "@vue/test-utils";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import TaskOverview from "@/components/tasks/TaskOverview.vue";
import TaskTimeline from "@/components/tasks/TaskTimeline.vue";

const THRESHOLDS = {
  backlog_warn_seconds: 60,
  backlog_critical_seconds: 300,
  failure_rate_warn: 0.1,
  failure_rate_critical: 0.5,
};

function makeStats(overrides = {}) {
  return {
    window: "1h",
    generated_at: "2026-09-25T10:00:00+00:00",
    health: { status: "healthy", reasons: [] },
    thresholds: THRESHOLDS,
    summary: {
      finished: 20,
      succeeded: 19,
      failed: 1,
      succeeded_after_retry: 2,
      failure_rate: 0.05,
      throughput_per_minute: 0.333,
      duration_p50: 0.25,
      duration_p95: 42,
      enqueued: 21,
    },
    backlog: {
      due: 0,
      scheduled: 3,
      retrying: 0,
      oldest_due_seconds: null,
      running: 2,
      stuck: 0,
      active_workers: 1,
    },
    series: [
      {
        start: "2026-09-25T09:00:00+00:00",
        succeeded: 4,
        failed: 0,
        enqueued: 4,
      },
      {
        start: "2026-09-25T09:30:00+00:00",
        succeeded: 2,
        failed: 2,
        enqueued: 5,
      },
    ],
    kinds: [],
    errors: [],
    ...overrides,
  };
}

function kind(overrides = {}) {
  return {
    kind: "repo.recalc",
    succeeded: 3,
    failed: 0,
    queued: 0,
    running: 0,
    stuck: 0,
    failure_rate: 0,
    duration_p95: 5,
    timeline: [{ succeeded: 3, failed: 0 }],
    last_error: null,
    last_failed_at: null,
    ...overrides,
  };
}

function mountOverview(stats, window = "1h") {
  return mount(TaskOverview, { props: { stats, window } });
}

const tile = (wrapper, key) => wrapper.get(`[data-testid="task-kpi-${key}"]`);

describe("TaskOverview", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(new Date("2026-09-25T10:00:00Z"));
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("renders KPI tiles with tones derived from the thresholds", () => {
    const wrapper = mountOverview(makeStats());

    expect(tile(wrapper, "finished").text()).toContain("20");
    expect(tile(wrapper, "finished").text()).toContain(
      "0.33/min · 21 enqueued",
    );
    expect(tile(wrapper, "success").text()).toContain("95.0%");
    expect(tile(wrapper, "success").text()).toContain("2 recovered by retry");
    expect(tile(wrapper, "success").classes()).toContain("ok");
    expect(tile(wrapper, "backlog").text()).toContain("nothing waiting");
    expect(tile(wrapper, "backlog").classes()).toContain("muted");
    expect(tile(wrapper, "retrying").classes()).toContain("muted");
    expect(tile(wrapper, "running").text()).toContain("1 active worker(s)");
    expect(tile(wrapper, "duration").text()).toContain("250 ms");
    expect(tile(wrapper, "duration").text()).toContain("p95 42.0 s");
    expect(wrapper.find('[data-testid="task-health-reasons"]').exists()).toBe(
      false,
    );
    expect(wrapper.find('[data-testid="task-kinds-empty"]').exists()).toBe(
      true,
    );
    expect(wrapper.find('[data-testid="task-errors-empty"]').exists()).toBe(
      true,
    );
  });

  it("flags backlog, retries, stuck tasks and failure rates by severity", () => {
    const warn = mountOverview(
      makeStats({
        summary: { ...makeStats().summary, failure_rate: 0.2 },
        backlog: {
          ...makeStats().backlog,
          due: 4,
          oldest_due_seconds: 90,
          retrying: 2,
        },
      }),
    );
    expect(tile(warn, "success").classes()).toContain("warn");
    expect(tile(warn, "backlog").classes()).toContain("warn");
    expect(tile(warn, "backlog").text()).toContain("oldest waiting 1m");
    expect(tile(warn, "retrying").classes()).toContain("warn");

    const bad = mountOverview(
      makeStats({
        health: {
          status: "unhealthy",
          reasons: [
            {
              level: "unhealthy",
              message: "1 running task(s) have an expired lease",
            },
            { level: "degraded", message: "2 task(s) are waiting to retry" },
          ],
        },
        summary: { ...makeStats().summary, failure_rate: 0.6 },
        backlog: { ...makeStats().backlog, oldest_due_seconds: 4000, stuck: 1 },
      }),
    );
    expect(tile(bad, "success").classes()).toContain("bad");
    expect(tile(bad, "backlog").classes()).toContain("bad");
    expect(tile(bad, "backlog").text()).toContain("1h 6m");
    expect(tile(bad, "running").classes()).toContain("bad");
    expect(tile(bad, "running").text()).toContain("1 stuck (lease expired)");

    const slow = mountOverview(
      makeStats({
        summary: { ...makeStats().summary, cancelled: 3 },
        backlog: {
          ...makeStats().backlog,
          stuck: 1,
          stalled: 2,
          cancel_requested: 1,
        },
      }),
    );
    expect(tile(slow, "running").text()).toContain(
      "1 stuck (lease expired) · 2 stalled · 1 cancelling",
    );
    expect(tile(slow, "success").text()).toContain("3 cancelled");
    const stalled = mountOverview(
      makeStats({ backlog: { ...makeStats().backlog, stalled: 1 } }),
    );
    expect(tile(stalled, "running").classes()).toContain("warn");
    expect(tile(stalled, "running").text()).toContain("1 stalled");
    const reasons = bad.findAll('[data-testid="task-health-reasons"] li');
    expect(reasons.map((r) => r.classes()[0])).toEqual([
      "unhealthy",
      "degraded",
    ]);

    const okBacklog = mountOverview(
      makeStats({
        backlog: { ...makeStats().backlog, due: 1, oldest_due_seconds: 5 },
      }),
    );
    expect(tile(okBacklog, "backlog").classes()).toContain("ok");
    expect(tile(okBacklog, "backlog").text()).toContain("oldest waiting 5s");
  });

  it("shows placeholders when nothing finished", () => {
    const wrapper = mountOverview(
      makeStats({
        summary: {
          ...makeStats().summary,
          finished: 0,
          failure_rate: null,
          duration_p50: null,
          duration_p95: null,
        },
      }),
    );

    expect(tile(wrapper, "success").text()).toContain("—");
    expect(tile(wrapper, "success").classes()).toContain("muted");
    expect(tile(wrapper, "duration").text()).toContain("p95 —");
  });

  it("formats long durations and whole percentages", () => {
    const wrapper = mountOverview(
      makeStats({
        summary: { ...makeStats().summary, failure_rate: 0, duration_p50: 125 },
      }),
    );

    expect(tile(wrapper, "success").text()).toContain("100%");
    expect(tile(wrapper, "duration").text()).toContain("2m");
  });

  it("scales activity bars to the busiest bucket", () => {
    const wrapper = mountOverview(makeStats());
    const bars = wrapper.findAll('[data-testid="task-activity"] > div');

    expect(bars).toHaveLength(2);
    expect(bars[0].get(".ok").attributes("style")).toContain("height: 100%");
    expect(bars[1].get(".fail").attributes("style")).toContain("height: 50%");
    expect(bars[1].attributes("title")).toContain(
      "2 succeeded, 2 failed, 5 enqueued",
    );
  });

  it("emits window changes and marks the active window", async () => {
    const wrapper = mountOverview(makeStats(), "6h");

    expect(wrapper.get('[data-testid="task-window-6h"]').classes()).toContain(
      "active",
    );
    await wrapper.get('[data-testid="task-window-7d"]').trigger("click");

    expect(wrapper.emitted("update:window")).toEqual([["7d"]]);
  });

  it("lists kinds with their health and emits a kind selection", async () => {
    const wrapper = mountOverview(
      makeStats({
        kinds: [
          kind({
            kind: "repo.sync",
            failed: 3,
            stuck: 1,
            failure_rate: 0.5,
            last_error: "RuntimeError: boom",
            timeline: [{ succeeded: 3, failed: 3 }],
          }),
          kind({
            kind: "repo.idle",
            succeeded: 0,
            failure_rate: null,
            queued: 2,
          }),
          kind(),
        ],
      }),
    );

    const sync = wrapper.get('[data-testid="task-kind-repo.sync"]');
    expect(sync.get(".kind-rate").text()).toBe("50.0%");
    expect(sync.get(".kind-rate").classes()).toContain("bad");
    expect(sync.text()).toContain("1 stuck");
    expect(sync.text()).toContain("RuntimeError: boom");
    expect(
      wrapper.get('[data-testid="task-kind-repo.idle"] .kind-rate').text(),
    ).toBe("—");
    expect(
      wrapper.get('[data-testid="task-kind-repo.recalc"] .kind-rate').text(),
    ).toBe("100%");

    await sync.trigger("click");
    expect(wrapper.emitted("select-kind")).toEqual([["repo.sync"]]);
  });

  it("only the running tile opens a filtered list", async () => {
    const wrapper = mountOverview(makeStats());

    // These tiles summarise the window or a subset of a status, so a status
    // filter would show more than they claim; they are not clickable.
    for (const key of [
      "finished",
      "success",
      "backlog",
      "retrying",
      "duration",
    ]) {
      expect(tile(wrapper, key).element.tagName).toBe("DIV");
      await tile(wrapper, key).trigger("click");
    }
    expect(tile(wrapper, "running").element.tagName).toBe("BUTTON");
    await tile(wrapper, "running").trigger("click");

    expect(wrapper.emitted("select-status")).toEqual([["running"]]);
  });

  it("shows error groups scaled to the largest and lets you jump to a kind", async () => {
    const wrapper = mountOverview(
      makeStats({
        errors: [
          {
            error: "RuntimeError",
            failed: 3,
            retrying: 1,
            kinds: ["repo.sync", "repo.recalc"],
            example: "RuntimeError: LakeFS returned 503",
            last_seen: "2026-09-25T09:55:00+00:00",
          },
          {
            error: "KeyError",
            failed: 2,
            retrying: 0,
            kinds: ["repo.recalc"],
            example: "KeyError: 'repo_id'",
            last_seen: "2026-09-25T08:00:00+00:00",
          },
        ],
      }),
    );

    const runtime = wrapper.get('[data-testid="task-error-RuntimeError"]');
    expect(runtime.text()).toContain("3 failed · 1 retrying");
    expect(runtime.text()).toContain("last seen 5m ago");
    expect(runtime.get(".fail").attributes("style")).toContain("width: 75%");
    expect(runtime.get(".retry").attributes("style")).toContain("width: 25%");
    const keyError = wrapper.get('[data-testid="task-error-KeyError"]');
    expect(keyError.text()).not.toContain("retrying");
    expect(keyError.text()).toContain("last seen 2h 0m ago");

    await runtime.findAll(".chip")[0].trigger("click");
    expect(wrapper.emitted("select-kind")).toEqual([["repo.sync"]]);
  });
});

describe("TaskTimeline", () => {
  it("colours each bucket by its outcome and labels it", () => {
    const wrapper = mount(TaskTimeline, {
      props: {
        buckets: [
          { succeeded: 0, failed: 0 },
          { succeeded: 5, failed: 0 },
          { succeeded: 4, failed: 1 },
          { succeeded: 1, failed: 4 },
        ],
        starts: ["2026-09-25T09:00:00+00:00"],
      },
    });

    const bars = wrapper.findAll(".bar");
    expect(bars.map((b) => b.attributes("data-level"))).toEqual([
      "idle",
      "ok",
      "warn",
      "bad",
    ]);
    expect(bars[0].attributes("title")).toMatch(
      /^09-25 \d\d:00 · 0 succeeded, 0 failed$/,
    );
    expect(bars[3].attributes("title")).toBe("1 succeeded, 4 failed");
  });
});
