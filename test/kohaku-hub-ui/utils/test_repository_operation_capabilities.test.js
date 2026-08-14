import { describe, expect, it } from "vitest";

import { getRepositoryOperationCapabilities } from "@/utils/repositoryOperationCapabilities";

describe("repository operation capabilities", () => {
  it("fails closed when the site config is missing or malformed", () => {
    expect(getRepositoryOperationCapabilities()).toEqual({
      revert: false,
      reset: false,
      squash: false,
    });
    expect(getRepositoryOperationCapabilities({ capabilities: null })).toEqual({
      revert: false,
      reset: false,
      squash: false,
    });
  });

  it("accepts only explicit true values for each operation", () => {
    expect(
      getRepositoryOperationCapabilities({
        capabilities: {
          repository_operations: {
            revert: true,
            reset: 1,
            squash: "true",
          },
        },
      }),
    ).toEqual({
      revert: true,
      reset: false,
      squash: false,
    });
  });
});
