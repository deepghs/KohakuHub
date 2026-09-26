import { mount } from "@vue/test-utils";
import { describe, expect, it } from "vitest";

import { ElementPlusStubs } from "../helpers/vue";
import TaskWorkers from "@/components/tasks/TaskWorkers.vue";

function worker(overrides = {}) {
  return {
    id: "3f9a1c2e7b10:1:aaaa1111",
    name: "3f9a1c2e7b10",
    hostname: "3f9a1c2e7b10",
    pid: 1,
    queues: [],
    concurrency: 4,
    status: "online",
    running: 3,
    succeeded: 120,
    failed: 2,
    started_at: "2026-09-25T09:00:00+00:00",
    last_heartbeat_at: "2026-09-25T10:00:00+00:00",
    heartbeat_age_seconds: 4.2,
    stopped_at: null,
    tasks: [
      { id: 11, kind: "repo.cleanup_storage", progress: null },
      { id: 12, kind: "tasks.cleanup", progress: null },
    ],
    ...overrides,
  };
}

function roster(workers, overrides = {}) {
  return {
    workers,
    counts: { online: 1, draining: 1, lost: 1, stopped: 1 },
    hidden: 0,
    lost_after_seconds: 30,
    inactive_after_seconds: 86400,
    ...overrides,
  };
}

function mountRoster(props) {
  return mount(TaskWorkers, {
    props,
    global: { stubs: ElementPlusStubs },
  });
}

describe("TaskWorkers", () => {
  it("lists workers with status, load, counters and running tasks", () => {
    const wrapper = mountRoster({
      roster: roster([
        worker(),
        worker({
          id: "gpu-box:9:bbbb",
          name: "gpu-box-host",
          status: "stopped",
          queues: ["bulk", "sync"],
          running: 0,
          failed: 0,
          stopped_at: "2026-09-25T08:00:00+00:00",
          tasks: [],
        }),
      ]),
    });

    const rows = wrapper.findAll("tr");
    expect(rows[0].text()).toContain("3f9a1c2e7b10:1:aaaa1111");
    expect(rows[0].text()).toContain("pid 1");
    expect(rows[0].text()).toContain("heartbeat 4s ago");
    expect(rows[0].text()).toContain("all");
    expect(rows[0].text()).toContain("3 / 4 running");
    expect(
      rows[0].get('[data-el-progress="true"]').attributes("data-percentage"),
    ).toBe("75");
    expect(rows[0].get(".bad").text()).toBe("2 failed");
    expect(rows[0].text()).toContain("#11 repo.cleanup_storage");
    expect(rows[1].text()).toContain("stopped 2026-09-25");
    expect(rows[1].text()).toContain("bulk, sync");
    expect(rows[1].find(".bad").exists()).toBe(false);
    expect(rows[1].text()).toContain("—");
    for (const status of ["online", "draining", "lost", "stopped"]) {
      expect(
        wrapper.get(`[data-testid="workers-count-${status}"]`).text(),
      ).toContain("1");
    }
  });

  it("emits task selection and the inactive toggle, and names hidden workers", async () => {
    const wrapper = mountRoster({
      roster: roster([worker()], { hidden: 5 }),
    });

    await wrapper.get('[data-testid="worker-task-12"]').trigger("click");
    expect(wrapper.emitted("select-task")[0]).toEqual([12]);
    const toggle = wrapper.get('[data-testid="workers-include-inactive"]');
    expect(toggle.text()).toContain("inactive for over 24h");
    expect(toggle.text()).toContain("(5 hidden)");
    wrapper
      .getComponent({ name: "ElCheckbox" })
      .vm.$emit("update:modelValue", true);
    expect(wrapper.emitted("update:includeInactive")[0]).toEqual([true]);
  });

  it("highlights the requested worker and copes with zero concurrency", () => {
    const wrapper = mountRoster({
      roster: roster([worker({ concurrency: 0, running: 0 })]),
      highlight: "3f9a1c2e7b10:1:aaaa1111",
    });
    const rowClass = wrapper.getComponent({ name: "ElTable" }).vm.$attrs[
      "row-class-name"
    ];
    expect(rowClass({ row: { id: "3f9a1c2e7b10:1:aaaa1111" } })).toBe(
      "highlighted",
    );
    expect(rowClass({ row: { id: "other" } })).toBe("");
    expect(
      wrapper.get('[data-el-progress="true"]').attributes("data-percentage"),
    ).toBe("0");
    expect(wrapper.text()).not.toContain("hidden");
  });

  it("explains how to start a worker when none registered", () => {
    const wrapper = mountRoster({ roster: roster([]) });
    expect(
      wrapper
        .get('[data-testid="workers-empty"]')
        .attributes("data-description"),
    ).toContain("make worker");
  });
});
