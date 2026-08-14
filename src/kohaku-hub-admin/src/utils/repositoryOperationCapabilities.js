const DISABLED_REPOSITORY_OPERATIONS = Object.freeze({
  revert: false,
  reset: false,
  squash: false,
});

export function getRepositoryOperationCapabilities(siteConfig) {
  const operations = siteConfig?.capabilities?.repository_operations;

  if (!operations || typeof operations !== "object") {
    return { ...DISABLED_REPOSITORY_OPERATIONS };
  }

  return {
    revert: operations.revert === true,
    reset: operations.reset === true,
    squash: operations.squash === true,
  };
}
