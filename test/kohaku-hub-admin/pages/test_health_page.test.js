import { defineComponent, h } from "vue";
import { flushPromises, mount } from "@vue/test-utils";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { ElementPlusStubs } from "../helpers/vue";

const mocks = vi.hoisted(() => ({
  router: {
    push: vi.fn(),
  },
  adminStore: {
    token: "admin-token",
    logout: vi.fn(),
  },
  api: {
    getDependencyHealth: vi.fn(),
  },
}));

vi.mock("vue-router", () => ({
  useRouter: () => mocks.router,
}));

vi.mock("@/stores/admin", () => ({
  useAdminStore: () => mocks.adminStore,
}));

vi.mock("@/utils/api", () => ({
  getDependencyHealth: (...args) => mocks.api.getDependencyHealth(...args),
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

vi.mock("element-plus", async () => {
  const actual = await vi.importActual("element-plus");
  return actual;
});

import HealthPage from "@/pages/health.vue";

const SAMPLE_PAYLOAD = {
  overall_status: "ok",
  checked_at_ms: 1_700_000_000_000,
  elapsed_ms: 47,
  timeout_seconds: 2,
  dependencies: [
    {
      name: "postgres",
      status: "ok",
      latency_ms: 12,
      version: "PostgreSQL 15.5",
      endpoint: "postgresql://hub@127.0.0.1:5432/hub",
      detail: null,
    },
    {
      name: "minio",
      status: "ok",
      latency_ms: 9,
      version: "MinIO",
      endpoint: "http://127.0.0.1:9000",
      detail: null,
    },
    {
      name: "lakefs",
      status: "ok",
      latency_ms: 18,
      version: "1.80.0",
      endpoint: "http://127.0.0.1:8000",
      detail: null,
    },
    {
      name: "smtp",
      status: "disabled",
      latency_ms: null,
      version: null,
      endpoint: null,
      detail: "SMTP is disabled in configuration",
    },
  ],
};

const noop = () => {};
const noopDirective = {
  mounted: noop,
  updated: noop,
  beforeUnmount: noop,
};

function mountPage() {
  return mount(HealthPage, {
    global: {
      stubs: ElementPlusStubs,
      directives: {
        loading: noopDirective,
      },
    },
  });
}

describe("admin health page", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    mocks.router.push.mockReset();
    mocks.adminStore.logout.mockReset();
    mocks.adminStore.token = "admin-token";
    mocks.api.getDependencyHealth.mockReset();
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("loads dependency health on mount and renders one card per dependency", async () => {
    mocks.api.getDependencyHealth.mockResolvedValue(SAMPLE_PAYLOAD);
    const wrapper = mountPage();
    await flushPromises();

    expect(mocks.api.getDependencyHealth).toHaveBeenCalledWith("admin-token");

    const cards = wrapper.findAll('[data-testid^="health-card-"]');
    expect(cards).toHaveLength(SAMPLE_PAYLOAD.dependencies.length);

    const overall = wrapper.get('[data-testid="health-overall"]');
    expect(overall.text()).toContain("OK");

    const postgresCard = wrapper.get('[data-testid="health-card-postgres"]');
    expect(postgresCard.text()).toContain("PostgreSQL");
    expect(postgresCard.text()).toContain("12 ms");
    expect(postgresCard.text()).toContain("PostgreSQL 15.5");
    expect(postgresCard.text()).toContain("postgresql://hub@127.0.0.1:5432/hub");

    const smtpCard = wrapper.get('[data-testid="health-card-smtp"]');
    expect(smtpCard.text()).toContain("Disabled");
    expect(smtpCard.text()).toContain("SMTP is disabled in configuration");
  });

  it("re-fetches when the user clicks Re-check", async () => {
    mocks.api.getDependencyHealth.mockResolvedValue(SAMPLE_PAYLOAD);
    const wrapper = mountPage();
    await flushPromises();

    const button = wrapper.get('[data-testid="health-recheck"]');
    await button.trigger("click");
    await flushPromises();

    expect(mocks.api.getDependencyHealth).toHaveBeenCalledTimes(2);
  });

  it("logs the operator out when the backend returns 401", async () => {
    const failure = new Error("unauthorized");
    failure.response = { status: 401, data: { detail: { error: "no auth" } } };
    mocks.api.getDependencyHealth.mockRejectedValueOnce(failure);

    mountPage();
    await flushPromises();

    expect(mocks.adminStore.logout).toHaveBeenCalledTimes(1);
    expect(mocks.router.push).toHaveBeenCalledWith("/login");
  });

  it("redirects to /login when no admin token is present", async () => {
    mocks.adminStore.token = "";
    mountPage();
    await flushPromises();

    expect(mocks.router.push).toHaveBeenCalledWith("/login");
    expect(mocks.api.getDependencyHealth).not.toHaveBeenCalled();
  });

  it("kicks off auto-refresh when the interval is changed and stops when reset", async () => {
    mocks.api.getDependencyHealth.mockResolvedValue(SAMPLE_PAYLOAD);
    const wrapper = mountPage();
    await flushPromises();

    expect(mocks.api.getDependencyHealth).toHaveBeenCalledTimes(1);

    const select = wrapper.get('[data-el-select="true"]');
    await select.setValue(30);
    await flushPromises();

    vi.advanceTimersByTime(60_000);
    await flushPromises();
    // Two ticks of 30 s each fired while auto-refresh was on.
    expect(mocks.api.getDependencyHealth).toHaveBeenCalledTimes(3);

    await select.setValue(0);
    await flushPromises();

    vi.advanceTimersByTime(120_000);
    await flushPromises();
    // No additional calls after auto-refresh was reset to "Off".
    expect(mocks.api.getDependencyHealth).toHaveBeenCalledTimes(3);
  });
});
