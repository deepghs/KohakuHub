import { mount } from "@vue/test-utils";
import { defineComponent, h, nextTick, ref } from "vue";
import { describe, expect, it, vi } from "vitest";

const mountCounts = {
  repoViewer: 0,
};

const routeState = ref({
  path: "/models/mai_lin/lineart-caption-base",
});

// Render functions, not `template:` strings: vitest.config.js aliases `vue` to
// the runtime-only build, which cannot compile templates at runtime. A
// `template` stub silently renders nothing there, so the assertions below would
// fail on the stub rather than on App.vue. Matches RepoViewerStub below.
vi.mock("@/components/layout/TheHeader.vue", () => ({
  default: defineComponent({
    name: "TheHeader",
    setup() {
      return () => h("header", { "data-header": "true" }, "Header");
    },
  }),
}));

vi.mock("@/components/layout/TheFooter.vue", () => ({
  default: defineComponent({
    name: "TheFooter",
    setup() {
      return () => h("footer", { "data-footer": "true" }, "Footer");
    },
  }),
}));

const RepoViewerStub = defineComponent({
  name: "RepoViewer",
  setup() {
    mountCounts.repoViewer += 1;
    return () =>
      h("div", { "data-repo-viewer": "true" }, routeState.value.path);
  },
});

const RouterViewStub = defineComponent({
  name: "RouterView",
  setup(_, { slots }) {
    return () =>
      slots.default({
        Component: RepoViewerStub,
        route: routeState.value,
      });
  },
});

import App from "@/App.vue";

describe("App shell", () => {
  it("renders the layout and reuses repo views for the same repository", async () => {
    mountCounts.repoViewer = 0;
    routeState.value = {
      path: "/models/mai_lin/lineart-caption-base",
    };

    const wrapper = mount(App, {
      global: {
        components: {
          RouterView: RouterViewStub,
        },
      },
    });

    expect(wrapper.find('[data-header="true"]').exists()).toBe(true);
    expect(wrapper.find('[data-footer="true"]').exists()).toBe(true);
    expect(mountCounts.repoViewer).toBe(1);

    routeState.value = {
      path: "/models/mai_lin/lineart-caption-base/tree/main",
    };
    await nextTick();
    expect(mountCounts.repoViewer).toBe(1);

    routeState.value = {
      path: "/models/mai_lin/another-repo",
    };
    await nextTick();
    expect(mountCounts.repoViewer).toBe(2);

    wrapper.unmount();
  });

  it("uses the raw path as the route key for non-repository pages", async () => {
    mountCounts.repoViewer = 0;
    routeState.value = {
      path: "/login",
    };

    const wrapper = mount(App, {
      global: {
        components: {
          RouterView: RouterViewStub,
        },
      },
    });

    expect(mountCounts.repoViewer).toBe(1);

    routeState.value = {
      path: "/register",
    };
    await nextTick();

    expect(mountCounts.repoViewer).toBe(2);

    wrapper.unmount();
  });
});
