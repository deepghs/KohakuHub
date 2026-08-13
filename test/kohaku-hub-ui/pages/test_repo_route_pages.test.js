import { mount } from "@vue/test-utils";
import { defineComponent, h } from "vue";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { RouterLinkStub } from "../helpers/vue";

const mocks = vi.hoisted(() => ({
  route: {
    path: "/models/mai_lin/demo",
    params: {
      namespace: "mai_lin",
      name: "demo",
      branch: "main",
    },
    query: {},
  },
}));

vi.mock("vue-router/auto", () => ({
  useRoute: () => mocks.route,
}));

// Render functions, not `template:` strings: vitest.config.js aliases `vue` to
// the runtime-only build, which cannot compile templates at runtime, so a
// `template` stub renders nothing and the prop assertions below would compare
// against an empty node instead of the routed props.
vi.mock("@/components/pages/RepoListPage.vue", () => ({
  default: defineComponent({
    name: "RepoListPage",
    props: ["repoType"],
    setup(props) {
      return () =>
        h("div", { "data-repo-list-page": "true" }, props.repoType);
    },
  }),
}));

vi.mock("@/components/repo/RepoViewer.vue", () => ({
  default: defineComponent({
    name: "RepoViewer",
    props: ["repoType", "namespace", "name", "tab", "branch", "currentPath"],
    setup(props) {
      return () =>
        h(
          "div",
          { "data-repo-viewer": "true" },
          [
            props.repoType,
            props.namespace,
            props.name,
            props.tab,
            props.branch || "",
            props.currentPath || "",
          ].join("|"),
        );
    },
  }),
}));

import DatasetPage from "@/pages/datasets.vue";
import ModelPage from "@/pages/models.vue";
import SpacePage from "@/pages/spaces.vue";
import RepoIndexPage from "@/pages/[type]s/[namespace]/[name]/index.vue";
import RepoTreePage from "@/pages/[type]s/[namespace]/[name]/tree/[branch]/index.vue";
import RepoTreePathPage from "@/pages/[type]s/[namespace]/[name]/tree/[branch]/[...path].vue";

describe("repo route pages", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.route.path = "/models/mai_lin/demo";
    mocks.route.params = {
      namespace: "mai_lin",
      name: "demo",
      branch: "main",
    };
    mocks.route.query = {};
  });

  function mountPage(component) {
    return mount(component, {
      global: {
        stubs: {
          RouterLink: RouterLinkStub,
        },
      },
    });
  }

  it("binds the correct repository type for list pages", () => {
    expect(mountPage(ModelPage).text()).toContain("model");
    expect(mountPage(DatasetPage).text()).toContain("dataset");
    expect(mountPage(SpacePage).text()).toContain("space");
  });

  it("passes route-derived props into repo viewer wrappers", () => {
    mocks.route.path = "/datasets/aurora-labs/vision-set";
    mocks.route.params = {
      namespace: "aurora-labs",
      name: "vision-set",
      branch: "release",
    };
    mocks.route.query = { tab: "files" };

    const indexWrapper = mountPage(RepoIndexPage);
    expect(indexWrapper.text()).toContain(
      "dataset|aurora-labs|vision-set|files||",
    );

    mocks.route.path = "/spaces/mai_lin/demo/tree/dev";
    const treeWrapper = mountPage(RepoTreePage);
    expect(treeWrapper.text()).toContain("space|aurora-labs|vision-set|files|release|");
  });

  it("normalizes catch-all tree paths before passing them to repo viewer", () => {
    mocks.route.path = "/datasets/open-media-lab/demo/tree/main/catalog/section-01";
    mocks.route.params = {
      namespace: "open-media-lab",
      name: "demo",
      branch: "main",
      path: ["catalog", "section-01"],
    };

    const wrapper = mountPage(RepoTreePathPage);
    expect(wrapper.text()).toContain(
      "dataset|open-media-lab|demo|files|main|catalog/section-01",
    );
  });
});
