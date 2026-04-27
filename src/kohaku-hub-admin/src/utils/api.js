/**
 * Admin API client for KohakuHub
 * All functions require admin token to be passed explicitly (no storage)
 */

import axios from "axios";

/**
 * Create axios instance with admin token
 * @param {string} token - Admin token
 * @returns {import('axios').AxiosInstance} Axios instance
 */
function createAdminClient(token) {
  return axios.create({
    baseURL: "/admin/api",
    headers: {
      "X-Admin-Token": token,
    },
  });
}

// ===== User Management =====

/**
 * List all users
 * @param {string} token - Admin token
 * @param {Object} params - Query parameters
 * @param {string} params.search - Search by username or email
 * @param {number} params.limit - Max users to return
 * @param {number} params.offset - Offset for pagination
 * @returns {Promise<Object>} User list response
 */
export async function listUsers(
  token,
  { search, limit = 100, offset = 0, include_orgs = false } = {},
) {
  const client = createAdminClient(token);
  const response = await client.get("/users", {
    params: { search, limit, offset, include_orgs },
  });
  return response.data;
}

/**
 * Get user details
 * @param {string} token - Admin token
 * @param {string} username - Username
 * @returns {Promise<Object>} User information
 */
export async function getUserInfo(token, username) {
  const client = createAdminClient(token);
  const response = await client.get(`/users/${username}`);
  return response.data;
}

/**
 * Create new user
 * @param {string} token - Admin token
 * @param {Object} userData - User data
 * @returns {Promise<Object>} Created user
 */
export async function createUser(token, userData) {
  const client = createAdminClient(token);
  const response = await client.post("/users", userData);
  return response.data;
}

/**
 * Delete user
 * @param {string} token - Admin token
 * @param {string} username - Username to delete
 * @param {boolean} force - Force delete even if user owns repositories
 * @returns {Promise<Object>} Deletion result
 */
export async function deleteUser(token, username, force = false) {
  const client = createAdminClient(token);
  const response = await client.delete(`/users/${username}`, {
    params: { force },
  });
  return response.data;
}

/**
 * Set email verification status
 * @param {string} token - Admin token
 * @param {string} username - Username
 * @param {boolean} verified - Verification status
 * @returns {Promise<Object>} Updated user info
 */
export async function setEmailVerification(token, username, verified) {
  const client = createAdminClient(token);
  const response = await client.patch(
    `/users/${username}/email-verification`,
    null,
    {
      params: { verified },
    },
  );
  return response.data;
}

/**
 * Update user/org quota
 * @param {string} token - Admin token
 * @param {string} username - Username or org name
 * @param {number|null} privateQuotaBytes - Private quota in bytes (null = unlimited)
 * @param {number|null} publicQuotaBytes - Public quota in bytes (null = unlimited)
 * @returns {Promise<Object>} Updated quota info
 */
export async function updateUserQuota(
  token,
  username,
  privateQuotaBytes,
  publicQuotaBytes,
) {
  const client = createAdminClient(token);
  const response = await client.put(`/users/${username}/quota`, {
    private_quota_bytes: privateQuotaBytes,
    public_quota_bytes: publicQuotaBytes,
  });
  return response.data;
}

// ===== Quota Management =====

/**
 * Get quota information
 * @param {string} token - Admin token
 * @param {string} namespace - Username or org name
 * @param {boolean} isOrg - Is organization
 * @returns {Promise<Object>} Quota information
 */
export async function getQuota(token, namespace, isOrg = false) {
  const client = createAdminClient(token);
  const response = await client.get(`/quota/${namespace}`, {
    params: { is_org: isOrg },
  });
  return response.data;
}

/**
 * Set quota
 * @param {string} token - Admin token
 * @param {string} namespace - Username or org name
 * @param {Object} quotaData - Quota data
 * @param {number|null} quotaData.private_quota_bytes - Private quota
 * @param {number|null} quotaData.public_quota_bytes - Public quota
 * @param {boolean} isOrg - Is organization
 * @returns {Promise<Object>} Updated quota information
 */
export async function setQuota(token, namespace, quotaData, isOrg = false) {
  const client = createAdminClient(token);
  const response = await client.put(`/quota/${namespace}`, quotaData, {
    params: { is_org: isOrg },
  });
  return response.data;
}

/**
 * Recalculate storage usage
 * @param {string} token - Admin token
 * @param {string} namespace - Username or org name
 * @param {boolean} isOrg - Is organization
 * @returns {Promise<Object>} Updated quota information
 */
export async function recalculateQuota(token, namespace, isOrg = false) {
  const client = createAdminClient(token);
  const response = await client.post(`/quota/${namespace}/recalculate`, null, {
    params: { is_org: isOrg },
  });
  return response.data;
}

/**
 * Get quota overview with warnings
 * @param {string} token - Admin token
 * @returns {Promise<Object>} Quota overview data
 */
export async function getQuotaOverview(token) {
  const client = createAdminClient(token);
  const response = await client.get("/quota/overview");
  return response.data;
}

// ===== System Stats =====

/**
 * Get system statistics
 * @param {string} token - Admin token
 * @returns {Promise<Object>} System stats
 */
export async function getSystemStats(token) {
  const client = createAdminClient(token);
  const response = await client.get("/stats");
  return response.data;
}

/**
 * Get detailed system statistics
 * @param {string} token - Admin token
 * @returns {Promise<Object>} Detailed stats
 */
export async function getDetailedStats(token) {
  const client = createAdminClient(token);
  const response = await client.get("/stats/detailed");
  return response.data;
}

/**
 * Get time-series statistics
 * @param {string} token - Admin token
 * @param {number} days - Number of days
 * @returns {Promise<Object>} Time-series data
 */
export async function getTimeseriesStats(token, days = 30) {
  const client = createAdminClient(token);
  const response = await client.get("/stats/timeseries", { params: { days } });
  return response.data;
}

/**
 * Get top repositories
 * @param {string} token - Admin token
 * @param {number} limit - Number of top repos
 * @param {string} by - Sort by 'commits' or 'size'
 * @returns {Promise<Object>} Top repositories
 */
export async function getTopRepositories(token, limit = 10, by = "commits") {
  const client = createAdminClient(token);
  const response = await client.get("/stats/top-repos", {
    params: { limit, by },
  });
  return response.data;
}

/**
 * Verify admin token is valid
 * @param {string} token - Admin token
 * @returns {Promise<boolean>} True if token is valid
 */
export async function verifyAdminToken(token) {
  try {
    const client = createAdminClient(token);
    await client.get("/stats");
    return true;
  } catch (error) {
    if (error.response?.status === 401 || error.response?.status === 403) {
      return false;
    }
    throw error;
  }
}

// ===== Dependency Health =====

/**
 * Probe Postgres / MinIO / LakeFS / SMTP and return their status.
 *
 * @param {string} token - Admin token
 * @param {Object} [options]
 * @param {number} [options.timeoutSeconds] - Per-probe timeout in seconds
 * @returns {Promise<Object>} Aggregated probe report
 */
export async function getDependencyHealth(token, { timeoutSeconds } = {}) {
  const client = createAdminClient(token);
  const params = {};
  if (timeoutSeconds !== undefined && timeoutSeconds !== null) {
    params.timeout_seconds = timeoutSeconds;
  }
  const response = await client.get("/health/dependencies", { params });
  return response.data;
}

// ===== Repository Management =====

/**
 * List all repositories
 * @param {string} token - Admin token
 * @param {Object} params - Query parameters
 * @param {string} params.search - Search by repository full_id or name
 * @param {string} params.repo_type - Filter by type (model/dataset/space)
 * @param {string} params.namespace - Filter by namespace
 * @param {number} params.limit - Max repositories to return
 * @param {number} params.offset - Offset for pagination
 * @returns {Promise<Object>} Repository list
 */
export async function listRepositories(
  token,
  { search, repo_type, namespace, limit = 100, offset = 0 } = {},
) {
  const client = createAdminClient(token);
  const response = await client.get("/repositories", {
    params: { search, repo_type, namespace, limit, offset },
  });
  return response.data;
}

/**
 * Get repository details
 * @param {string} token - Admin token
 * @param {string} repo_type - Repository type
 * @param {string} namespace - Namespace
 * @param {string} name - Repository name
 * @returns {Promise<Object>} Repository details
 */
export async function getRepositoryDetails(token, repo_type, namespace, name) {
  const client = createAdminClient(token);
  const response = await client.get(
    `/repositories/${repo_type}/${namespace}/${name}`,
  );
  return response.data;
}

/**
 * Get repository files with LFS metadata
 * @param {string} token - Admin token
 * @param {string} repo_type - Repository type
 * @param {string} namespace - Namespace
 * @param {string} name - Repository name
 * @param {string} ref - Branch or commit reference
 * @returns {Promise<Object>} File list with LFS info
 */
export async function getRepositoryFiles(
  token,
  repo_type,
  namespace,
  name,
  ref = "main",
) {
  const client = createAdminClient(token);
  const response = await client.get(
    `/repositories/${repo_type}/${namespace}/${name}/files`,
    { params: { ref } },
  );
  return response.data;
}

/**
 * Get repository storage breakdown
 * @param {string} token - Admin token
 * @param {string} repo_type - Repository type
 * @param {string} namespace - Namespace
 * @param {string} name - Repository name
 * @returns {Promise<Object>} Storage analytics
 */
export async function getRepositoryStorageBreakdown(
  token,
  repo_type,
  namespace,
  name,
) {
  const client = createAdminClient(token);
  const response = await client.get(
    `/repositories/${repo_type}/${namespace}/${name}/storage-breakdown`,
  );
  return response.data;
}

// ===== Commit History =====

/**
 * List commits
 * @param {string} token - Admin token
 * @param {Object} params - Query parameters
 * @returns {Promise<Object>} Commit list
 */
export async function listCommits(
  token,
  { repo_full_id, username, limit = 100, offset = 0 } = {},
) {
  const client = createAdminClient(token);
  const response = await client.get("/commits", {
    params: { repo_full_id, username, limit, offset },
  });
  return response.data;
}

// ===== S3 Storage =====

/**
 * List S3 buckets
 * @param {string} token - Admin token
 * @returns {Promise<Object>} Bucket list
 */
export async function listS3Buckets(token) {
  const client = createAdminClient(token);
  const response = await client.get("/storage/buckets");
  return response.data;
}

/**
 * List S3 objects in a bucket
 * @param {string} token - Admin token
 * @param {string} bucket - Bucket name (empty = use configured bucket)
 * @param {Object} params - Query parameters
 * @returns {Promise<Object>} Object list
 */
export async function listS3Objects(
  token,
  bucket,
  { prefix = "", limit = 1000 } = {},
) {
  const client = createAdminClient(token);
  // Use /storage/objects (no bucket) to use configured bucket
  const url = bucket ? `/storage/objects/${bucket}` : "/storage/objects";
  const response = await client.get(url, {
    params: { prefix, limit },
  });
  return response.data;
}

// ===== Utility Functions =====

/**
 * Format bytes to human-readable size (decimal units: 1000 bytes = 1 KB)
 * @param {number} bytes - Bytes
 * @param {number} decimals - Decimal places
 * @returns {string} Formatted size
 */
export function formatBytes(bytes, decimals = 2) {
  if (bytes === null || bytes === undefined) return "Unlimited";
  if (bytes === 0) return "0 Bytes";

  const k = 1000;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ["Bytes", "KB", "MB", "GB", "TB", "PB"];

  const i = Math.floor(Math.log(bytes) / Math.log(k));

  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + " " + sizes[i];
}

/**
 * Parse human-readable size to bytes (decimal units: 1 KB = 1000 bytes)
 * @param {string} sizeStr - Size string (e.g., "10GB", "500MB")
 * @returns {number|null} Bytes, or null for unlimited
 */
export function parseSize(sizeStr) {
  if (!sizeStr || sizeStr.toLowerCase() === "unlimited") return null;

  const units = {
    b: 1,
    kb: 1000,
    mb: 1000 ** 2,
    gb: 1000 ** 3,
    tb: 1000 ** 4,
    pb: 1000 ** 5,
  };

  const match = sizeStr.match(/^(\d+(?:\.\d+)?)\s*([a-z]+)$/i);
  if (!match) return null;

  const value = parseFloat(match[1]);
  const unit = match[2].toLowerCase();

  return Math.floor(value * (units[unit] || 1));
}

/**
 * Recalculate storage for all repositories (bulk operation)
 * @param {string} token - Admin token
 * @param {Object} params - Query parameters
 * @param {string} params.repo_type - Optional filter by repository type
 * @param {string} params.namespace - Optional filter by namespace
 * @returns {Promise<Object>} Recalculation summary
 */
export async function recalculateAllRepoStorage(
  token,
  { repo_type, namespace } = {},
) {
  const client = createAdminClient(token);
  const response = await client.post("/repositories/recalculate-all", null, {
    params: { repo_type, namespace },
  });
  return response.data;
}

// ===== Invitation Management =====

/**
 * Create registration invitation
 * @param {string} token - Admin token
 * @param {Object} invitationData - Invitation data
 * @param {number|null} invitationData.org_id - Optional organization ID to join
 * @param {string} invitationData.role - Role in organization (if org_id provided)
 * @param {number|null} invitationData.max_usage - Max usage (null=one-time, -1=unlimited, N=max)
 * @param {number} invitationData.expires_days - Days until expiration
 * @returns {Promise<Object>} Created invitation
 */
export async function createRegisterInvitation(token, invitationData) {
  const client = createAdminClient(token);
  const response = await client.post("/invitations/register", invitationData);
  return response.data;
}

/**
 * List all invitations
 * @param {string} token - Admin token
 * @param {Object} params - Query parameters
 * @param {string} params.action - Filter by action type
 * @param {number} params.limit - Maximum number to return
 * @param {number} params.offset - Offset for pagination
 * @returns {Promise<Object>} Invitations list
 */
export async function listInvitations(
  token,
  { action, limit = 100, offset = 0 } = {},
) {
  const client = createAdminClient(token);
  const response = await client.get("/invitations", {
    params: { action, limit, offset },
  });
  return response.data;
}

/**
 * Delete invitation
 * @param {string} token - Admin token
 * @param {string} invitationToken - Invitation token to delete
 * @returns {Promise<Object>} Deletion result
 */
export async function deleteInvitation(token, invitationToken) {
  const client = createAdminClient(token);
  const response = await client.delete(`/invitations/${invitationToken}`);
  return response.data;
}

// ===== Global Search =====

/**
 * Global search across users, repositories, and commits
 * @param {string} token - Admin token
 * @param {string} q - Search query
 * @param {Array<string>} types - Types to search (users, repos, commits)
 * @param {number} limit - Max results per type
 * @returns {Promise<Object>} Grouped search results
 */
export async function globalSearch(
  token,
  q,
  types = ["users", "repos", "commits"],
  limit = 20,
) {
  const client = createAdminClient(token);
  const response = await client.get("/search", {
    params: { q, types, limit },
  });
  return response.data;
}

// ===== Database Viewer =====

/**
 * List database tables
 * @param {string} token - Admin token
 * @returns {Promise<Object>} Tables with schemas
 */
export async function listDatabaseTables(token) {
  const client = createAdminClient(token);
  const response = await client.get("/database/tables");
  return response.data;
}

/**
 * Get query templates
 * @param {string} token - Admin token
 * @returns {Promise<Object>} Pre-defined query templates
 */
export async function getDatabaseQueryTemplates(token) {
  const client = createAdminClient(token);
  const response = await client.get("/database/templates");
  return response.data;
}

/**
 * Execute SQL query (read-only)
 * @param {string} token - Admin token
 * @param {string} sql - SQL query string
 * @returns {Promise<Object>} Query results
 */
export async function executeDatabaseQuery(token, sql) {
  const client = createAdminClient(token);
  const response = await client.post("/database/query", { sql });
  return response.data;
}

// ===== Fallback Sources Management =====

/**
 * List all fallback sources
 * @param {string} token - Admin token
 * @param {Object} params - Query parameters
 * @param {string} params.namespace - Filter by namespace
 * @param {boolean} params.enabled - Filter by enabled status
 * @returns {Promise<Array>} Fallback sources list
 */
export async function listFallbackSources(token, { namespace, enabled } = {}) {
  const client = createAdminClient(token);
  const response = await client.get("/fallback-sources", {
    params: { namespace, enabled },
  });
  return response.data;
}

/**
 * Get specific fallback source
 * @param {string} token - Admin token
 * @param {number} sourceId - Source ID
 * @returns {Promise<Object>} Fallback source details
 */
export async function getFallbackSource(token, sourceId) {
  const client = createAdminClient(token);
  const response = await client.get(`/fallback-sources/${sourceId}`);
  return response.data;
}

/**
 * Create new fallback source
 * @param {string} token - Admin token
 * @param {Object} sourceData - Source data
 * @param {string} sourceData.namespace - Namespace ("" for global)
 * @param {string} sourceData.url - Base URL
 * @param {string} sourceData.token - Optional API token
 * @param {number} sourceData.priority - Priority (lower = higher)
 * @param {string} sourceData.name - Display name
 * @param {string} sourceData.source_type - "huggingface" or "kohakuhub"
 * @param {boolean} sourceData.enabled - Enabled status
 * @returns {Promise<Object>} Created fallback source
 */
export async function createFallbackSource(token, sourceData) {
  const client = createAdminClient(token);
  const response = await client.post("/fallback-sources", sourceData);
  return response.data;
}

/**
 * Update fallback source
 * @param {string} token - Admin token
 * @param {number} sourceId - Source ID
 * @param {Object} updateData - Fields to update
 * @returns {Promise<Object>} Updated fallback source
 */
export async function updateFallbackSource(token, sourceId, updateData) {
  const client = createAdminClient(token);
  const response = await client.put(
    `/fallback-sources/${sourceId}`,
    updateData,
  );
  return response.data;
}

/**
 * Delete fallback source
 * @param {string} token - Admin token
 * @param {number} sourceId - Source ID
 * @returns {Promise<Object>} Deletion result
 */
export async function deleteFallbackSource(token, sourceId) {
  const client = createAdminClient(token);
  const response = await client.delete(`/fallback-sources/${sourceId}`);
  return response.data;
}

/**
 * Get fallback cache statistics
 * @param {string} token - Admin token
 * @returns {Promise<Object>} Cache stats
 */
export async function getFallbackCacheStats(token) {
  const client = createAdminClient(token);
  const response = await client.get("/fallback-sources/cache/stats");
  return response.data;
}

/**
 * Clear fallback cache
 * @param {string} token - Admin token
 * @returns {Promise<Object>} Clear result
 */
export async function clearFallbackCache(token) {
  const client = createAdminClient(token);
  const response = await client.delete("/fallback-sources/cache/clear");
  return response.data;
}

// ===== Repository Management =====

/**
 * Delete repository (admin)
 * @param {string} token - Admin token
 * @param {string} repoType - Repository type (model/dataset/space)
 * @param {string} namespace - Repository namespace
 * @param {string} name - Repository name
 * @returns {Promise<Object>} Deletion result
 */
export async function deleteRepositoryAdmin(token, repoType, namespace, name) {
  const response = await axios.delete("/api/repos/delete", {
    headers: { "X-Admin-Token": token },
    data: { type: repoType, name: name, organization: namespace },
  });
  return response.data;
}

/**
 * Move repository (admin)
 * @param {string} token - Admin token
 * @param {string} repoType - Repository type
 * @param {string} namespace - Source namespace
 * @param {string} name - Source name
 * @param {string} toNamespace - Target namespace
 * @param {string} toName - Target name
 * @returns {Promise<Object>} Move result
 */
export async function moveRepositoryAdmin(
  token,
  repoType,
  namespace,
  name,
  toNamespace,
  toName,
) {
  const response = await axios.post(
    "/api/repos/move",
    {
      fromRepo: `${namespace}/${name}`,
      toRepo: `${toNamespace}/${toName}`,
      type: repoType,
    },
    {
      headers: { "X-Admin-Token": token },
    },
  );
  return response.data;
}

/**
 * Squash repository (admin)
 * @param {string} token - Admin token
 * @param {string} repoType - Repository type
 * @param {string} namespace - Repository namespace
 * @param {string} name - Repository name
 * @returns {Promise<Object>} Squash result
 */
export async function squashRepositoryAdmin(token, repoType, namespace, name) {
  const response = await axios.post(
    "/api/repos/squash",
    {
      repo: `${namespace}/${name}`,
      type: repoType,
    },
    {
      headers: { "X-Admin-Token": token },
    },
  );
  return response.data;
}

// ===== S3 Storage Management =====

/**
 * Delete S3 object
 * @param {string} token - Admin token
 * @param {string} key - Object key
 * @returns {Promise<Object>} Deletion result
 */
export async function deleteS3Object(token, key) {
  const client = createAdminClient(token);
  const response = await client.delete(
    `/storage/objects/${encodeURIComponent(key)}`,
  );
  return response.data;
}

/**
 * Prepare S3 prefix deletion (step 1)
 * @param {string} token - Admin token
 * @param {string} prefix - S3 prefix
 * @returns {Promise<Object>} Confirmation token and estimated count
 */
export async function prepareDeleteS3Prefix(token, prefix) {
  const client = createAdminClient(token);
  const response = await client.post("/storage/prefix/prepare-delete", null, {
    params: { prefix },
  });
  return response.data;
}

/**
 * Delete S3 prefix (step 2)
 * @param {string} token - Admin token
 * @param {string} prefix - S3 prefix
 * @param {string} confirmToken - Confirmation token from prepare step
 * @returns {Promise<Object>} Deletion result with count
 */
export async function deleteS3Prefix(token, prefix, confirmToken) {
  const client = createAdminClient(token);
  const response = await client.delete("/storage/prefix", {
    params: { prefix, confirm_token: confirmToken },
  });
  return response.data;
}
