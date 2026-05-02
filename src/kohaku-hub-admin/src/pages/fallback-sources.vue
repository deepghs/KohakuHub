<script setup>
import { ref, onMounted } from "vue";
import { useRouter } from "vue-router";
import AdminLayout from "@/components/AdminLayout.vue";
import { useAdminStore } from "@/stores/admin";
import {
  listFallbackSources,
  createFallbackSource,
  updateFallbackSource,
  deleteFallbackSource,
  getFallbackCacheStats,
  clearFallbackCache,
  invalidateFallbackRepoCache,
  invalidateFallbackUserCacheById,
  invalidateFallbackUserCacheByUsername,
  listUsers,
  listRepositories,
} from "@/utils/api";
import { ElMessage, ElMessageBox } from "element-plus";
import dayjs from "dayjs";

const router = useRouter();
const adminStore = useAdminStore();

const sources = ref([]);
const cacheStats = ref(null);
const loading = ref(false);

// Form dialog
const dialogVisible = ref(false);
const dialogMode = ref("create"); // 'create' or 'edit'
const formData = ref({
  id: null,
  namespace: "",
  url: "",
  token: "",
  priority: 100,
  name: "",
  source_type: "huggingface",
  enabled: true,
});

// Per-repo eviction dialog (#79 admin tooling).
const evictRepoDialogVisible = ref(false);
const evictRepoForm = ref({
  repo_type: "model",
  namespace: "",
  name: "",
});

// Per-user eviction dialog. ``mode`` toggles between username- and
// user_id-keyed paths so an admin who only knows one of the two doesn't
// have to leave the page to look up the other.
const evictUserDialogVisible = ref(false);
const evictUserForm = ref({
  mode: "username", // "username" | "user_id"
  username: "",
  user_id: null,
});

function checkAuth() {
  if (!adminStore.token) {
    router.push("/login");
    return false;
  }
  return true;
}

async function loadSources() {
  if (!checkAuth()) return;

  loading.value = true;
  try {
    sources.value = await listFallbackSources(adminStore.token);
  } catch (error) {
    console.error("Failed to load fallback sources:", error);
    ElMessage.error(
      error.response?.data?.detail?.error || "Failed to load fallback sources",
    );
  } finally {
    loading.value = false;
  }
}

async function loadCacheStats() {
  if (!checkAuth()) return;

  try {
    cacheStats.value = await getFallbackCacheStats(adminStore.token);
  } catch (error) {
    console.error("Failed to load cache stats:", error);
  }
}

function openCreateDialog() {
  dialogMode.value = "create";
  formData.value = {
    id: null,
    namespace: "",
    url: "",
    token: "",
    priority: 100,
    name: "",
    source_type: "huggingface",
    enabled: true,
  };
  dialogVisible.value = true;
}

function openEditDialog(source) {
  dialogMode.value = "edit";
  formData.value = {
    id: source.id,
    namespace: source.namespace,
    url: source.url,
    token: source.token || "",
    priority: source.priority,
    name: source.name,
    source_type: source.source_type,
    enabled: source.enabled,
  };
  dialogVisible.value = true;
}

async function handleSubmit() {
  if (!checkAuth()) return;

  loading.value = true;
  try {
    if (dialogMode.value === "create") {
      await createFallbackSource(adminStore.token, formData.value);
      ElMessage.success("Fallback source created successfully");
    } else {
      const { id, ...updateData } = formData.value;
      await updateFallbackSource(adminStore.token, id, updateData);
      ElMessage.success("Fallback source updated successfully");
    }

    dialogVisible.value = false;
    await loadSources();
    await loadCacheStats();
  } catch (error) {
    console.error("Failed to save fallback source:", error);
    ElMessage.error(
      error.response?.data?.detail?.error || "Failed to save fallback source",
    );
  } finally {
    loading.value = false;
  }
}

async function handleDelete(source) {
  if (!checkAuth()) return;

  try {
    await ElMessageBox.confirm(
      `Are you sure you want to delete "${source.name}"? This action cannot be undone.`,
      "Confirm Delete",
      {
        confirmButtonText: "Delete",
        cancelButtonText: "Cancel",
        type: "warning",
      },
    );

    loading.value = true;
    await deleteFallbackSource(adminStore.token, source.id);
    ElMessage.success(`Fallback source "${source.name}" deleted`);
    await loadSources();
    await loadCacheStats();
  } catch (error) {
    if (error !== "cancel") {
      console.error("Failed to delete fallback source:", error);
      ElMessage.error(
        error.response?.data?.detail?.error ||
          "Failed to delete fallback source",
      );
    }
  } finally {
    loading.value = false;
  }
}

async function handleToggleEnabled(source) {
  if (!checkAuth()) return;

  loading.value = true;
  try {
    await updateFallbackSource(adminStore.token, source.id, {
      enabled: !source.enabled,
    });
    ElMessage.success(
      `Source "${source.name}" ${!source.enabled ? "enabled" : "disabled"}`,
    );
    await loadSources();
  } catch (error) {
    console.error("Failed to toggle source:", error);
    ElMessage.error(
      error.response?.data?.detail?.error || "Failed to toggle source",
    );
  } finally {
    loading.value = false;
  }
}

async function handleClearCache() {
  if (!checkAuth()) return;

  try {
    await ElMessageBox.confirm(
      "Are you sure you want to clear the fallback cache? This will remove all cached repository→source mappings.",
      "Confirm Clear Cache",
      {
        confirmButtonText: "Clear Cache",
        cancelButtonText: "Cancel",
        type: "warning",
      },
    );

    loading.value = true;
    const result = await clearFallbackCache(adminStore.token);
    ElMessage.success(result.message);
    await loadCacheStats();
  } catch (error) {
    if (error !== "cancel") {
      console.error("Failed to clear cache:", error);
      ElMessage.error(
        error.response?.data?.detail?.error || "Failed to clear cache",
      );
    }
  } finally {
    loading.value = false;
  }
}

function openEvictRepoDialog() {
  evictRepoForm.value = {
    repo_type: "model",
    namespace: "",
    name: "",
  };
  evictRepoDialogVisible.value = true;
}

// Autocomplete suggestion fetchers (#79). Each is scoped to the right
// admin lookup for the field it serves:
//
//  - namespace (evict-by-repo dialog) → ``listUsers(include_orgs=true)``
//    because a repository's namespace is either a user **or** an
//    organisation, and listUsers in that mode returns the union.
//
//  - repo name (evict-by-repo dialog, given a fixed namespace) →
//    ``listRepositories(namespace, repo_type)`` for the
//    namespace-scoped enumeration. The query string filters
//    server-side via the existing ``search`` param.
//
//  - username (evict-by-user dialog, username mode) →
//    ``listUsers(include_orgs=false)``. Per the post-#79 cache key
//    shape ``(user_id_or_None, tokens_hash, repo_type, ns, name)``,
//    user_id is the request originator — orgs never originate a
//    request, so they have no cache bucket to evict and don't belong
//    in these suggestions.
//
// All three return ``{value: string}[]`` per Element Plus's
// ``el-autocomplete`` ``fetch-suggestions`` contract.
async function fetchNamespaceSuggestions(query, cb) {
  if (!checkAuth()) {
    cb([]);
    return;
  }
  try {
    if (!query || query.length < 1) {
      cb([]);
      return;
    }
    const data = await listUsers(adminStore.token, {
      search: query,
      limit: 20,
      include_orgs: true, // namespaces span both users and orgs
    });
    const items = Array.isArray(data) ? data : data?.users || data?.items || [];
    cb(
      items
        .filter((u) => u.username)
        .map((u) => ({ value: u.username })),
    );
  } catch (error) {
    console.error("Failed to fetch namespace suggestions:", error);
    cb([]);
  }
}

async function fetchRepoNameSuggestions(query, cb) {
  if (!checkAuth()) {
    cb([]);
    return;
  }
  try {
    if (!query || query.length < 1) {
      cb([]);
      return;
    }
    // Namespace-scoped enumeration. Without a chosen namespace we
    // can't bound the search to a single namespace, so fall back to
    // a coarse filter — but this dialog asks the operator to pick
    // namespace first, so the namespace-empty case is a UX hint to
    // fill that field first.
    const data = await listRepositories(adminStore.token, {
      search: query,
      repo_type: evictRepoForm.value.repo_type,
      namespace: evictRepoForm.value.namespace || undefined,
      limit: 20,
    });
    // Backend wraps under ``{repositories: [...]}`` (admin /repositories
    // route shape); also tolerate ``{items: [...]}`` and a raw array
    // for forward compatibility.
    const items = Array.isArray(data)
      ? data
      : data?.repositories || data?.items || [];
    cb(
      items
        .filter((repo) => repo.name)
        .map((repo) => ({ value: repo.name })),
    );
  } catch (error) {
    console.error("Failed to fetch repo name suggestions:", error);
    cb([]);
  }
}

async function fetchUsernameSuggestions(query, cb) {
  if (!checkAuth()) {
    cb([]);
    return;
  }
  try {
    if (!query || query.length < 1) {
      cb([]);
      return;
    }
    // ``include_orgs: false`` — fallback cache buckets are keyed by the
    // real user_id of the request originator. Orgs never make requests
    // themselves, so they have no bucket to evict and don't belong in
    // these suggestions.
    const data = await listUsers(adminStore.token, {
      search: query,
      limit: 20,
      include_orgs: false,
    });
    const items = Array.isArray(data) ? data : data?.users || data?.items || [];
    cb(
      items
        .filter((u) => u.username)
        .map((u) => ({ value: u.username })),
    );
  } catch (error) {
    console.error("Failed to fetch username suggestions:", error);
    cb([]);
  }
}

function openEvictUserDialog() {
  evictUserForm.value = {
    mode: "username",
    username: "",
    user_id: null,
  };
  evictUserDialogVisible.value = true;
}

async function handleEvictRepo() {
  if (!checkAuth()) return;

  const { repo_type, namespace, name } = evictRepoForm.value;
  if (!repo_type || !namespace || !name) {
    ElMessage.error("repo_type, namespace, and name are all required");
    return;
  }

  loading.value = true;
  try {
    const result = await invalidateFallbackRepoCache(
      adminStore.token,
      repo_type,
      namespace.trim(),
      name.trim(),
    );
    ElMessage.success(
      `Evicted ${result.evicted} cache entr${result.evicted === 1 ? "y" : "ies"} for ${repo_type}/${namespace}/${name}`,
    );
    evictRepoDialogVisible.value = false;
    await loadCacheStats();
  } catch (error) {
    console.error("Failed to evict repo cache:", error);
    ElMessage.error(
      error.response?.data?.detail?.error || "Failed to evict repo cache",
    );
  } finally {
    loading.value = false;
  }
}

async function handleEvictUser() {
  if (!checkAuth()) return;

  const { mode, username, user_id } = evictUserForm.value;

  if (mode === "username") {
    if (!username || !username.trim()) {
      ElMessage.error("Username is required");
      return;
    }
  } else {
    if (user_id == null || Number.isNaN(Number(user_id)) || Number(user_id) <= 0) {
      ElMessage.error("user_id must be a positive integer");
      return;
    }
  }

  // Strong confirm — per-user eviction crosses every repo this user has
  // bound. See PR #81 review item #5–#12 noted comment for the
  // confirm-on-broad-scope rule.
  try {
    const targetLabel =
      mode === "username" ? `username "${username}"` : `user_id ${user_id}`;
    await ElMessageBox.confirm(
      `Evict every cached fallback binding for ${targetLabel}? This drops every repo binding the user has cached and forces a re-probe on their next request.`,
      "Confirm User Cache Eviction",
      {
        confirmButtonText: "Evict",
        cancelButtonText: "Cancel",
        type: "warning",
      },
    );
  } catch (error) {
    if (error !== "cancel") {
      console.error("Confirm dialog dismissed unexpectedly:", error);
    }
    return;
  }

  loading.value = true;
  try {
    let result;
    if (mode === "username") {
      result = await invalidateFallbackUserCacheByUsername(
        adminStore.token,
        username.trim(),
      );
    } else {
      result = await invalidateFallbackUserCacheById(
        adminStore.token,
        Number(user_id),
      );
    }
    const tail =
      mode === "username"
        ? `${username} (user_id=${result.user_id})`
        : `user_id=${user_id}`;
    ElMessage.success(
      `Evicted ${result.evicted} cache entr${result.evicted === 1 ? "y" : "ies"} for ${tail}`,
    );
    evictUserDialogVisible.value = false;
    await loadCacheStats();
  } catch (error) {
    console.error("Failed to evict user cache:", error);
    ElMessage.error(
      error.response?.data?.detail?.error || "Failed to evict user cache",
    );
  } finally {
    loading.value = false;
  }
}

function formatDate(dateStr) {
  return dayjs(dateStr).format("YYYY-MM-DD HH:mm:ss");
}

onMounted(() => {
  loadSources();
  loadCacheStats();
});
</script>

<template>
  <AdminLayout>
    <div class="page-container">
      <div class="flex justify-between items-center mb-6 gap-4 flex-wrap">
        <div>
          <h1 class="text-3xl font-bold text-gray-900 dark:text-gray-100">
            Fallback Sources
          </h1>
          <p class="text-gray-500 dark:text-gray-400 text-sm mt-1">
            Manage external repository sources (HuggingFace, other KohakuHub
            instances).
          </p>
        </div>
      </div>

      <!-- Cache Stats Card -->
      <el-card v-if="cacheStats" class="stats-card" shadow="hover">
        <template #header>
          <div class="card-header">
            <span>Cache Statistics</span>
            <div class="cache-actions">
              <el-button
                type="warning"
                size="small"
                @click="openEvictRepoDialog"
                :loading="loading"
                data-testid="evict-repo-button"
              >
                <i class="i-carbon-cube mr-1"></i>
                Evict by Repo...
              </el-button>
              <el-button
                type="warning"
                size="small"
                @click="openEvictUserDialog"
                :loading="loading"
                data-testid="evict-user-button"
              >
                <i class="i-carbon-user mr-1"></i>
                Evict by User...
              </el-button>
              <el-button
                type="danger"
                size="small"
                @click="handleClearCache"
                :loading="loading"
              >
                <i class="i-carbon-trash-can mr-1"></i>
                Clear Cache
              </el-button>
            </div>
          </div>
        </template>
        <div class="stats-grid">
          <div class="stat-item">
            <div class="stat-label">Size</div>
            <div class="stat-value">{{ cacheStats.size }}</div>
          </div>
          <div class="stat-item">
            <div class="stat-label">Max Size</div>
            <div class="stat-value">{{ cacheStats.maxsize }}</div>
          </div>
          <div class="stat-item">
            <div class="stat-label">TTL</div>
            <div class="stat-value">{{ cacheStats.ttl_seconds }}s</div>
          </div>
          <div class="stat-item">
            <div class="stat-label">Usage</div>
            <div class="stat-value">{{ cacheStats.usage_percent }}%</div>
          </div>
        </div>
      </el-card>

      <!-- Sources List Card -->
      <el-card class="sources-card" shadow="hover">
        <template #header>
          <div class="card-header">
            <span>Configured Sources</span>
            <el-button
              type="primary"
              size="small"
              @click="openCreateDialog"
              :loading="loading"
            >
              <i class="i-carbon-add mr-1"></i>
              Add Source
            </el-button>
          </div>
        </template>

        <div v-if="loading && sources.length === 0" class="empty-state">
          <i class="i-carbon-loading animate-spin text-4xl"></i>
          <p>Loading...</p>
        </div>

        <div v-else-if="sources.length === 0" class="empty-state">
          <i class="i-carbon-cloud-offline text-6xl opacity-30"></i>
          <p>No fallback sources configured</p>
          <p class="text-sm opacity-60">
            Add a source to enable fallback to HuggingFace or other hubs
          </p>
        </div>

        <div v-else class="sources-list">
          <div
            v-for="source in sources"
            :key="source.id"
            class="source-item"
            :class="{ disabled: !source.enabled }"
          >
            <div class="source-header">
              <div class="source-title">
                <h3>{{ source.name }}</h3>
                <div class="badges">
                  <el-tag
                    :type="
                      source.source_type === 'huggingface'
                        ? 'primary'
                        : 'success'
                    "
                    size="small"
                  >
                    {{ source.source_type }}
                  </el-tag>
                  <el-tag
                    :type="source.enabled ? 'success' : 'info'"
                    size="small"
                  >
                    {{ source.enabled ? "Enabled" : "Disabled" }}
                  </el-tag>
                  <el-tag v-if="source.namespace" type="warning" size="small">
                    {{ source.namespace }}
                  </el-tag>
                </div>
              </div>
              <div class="source-actions">
                <el-button
                  :type="source.enabled ? 'default' : 'success'"
                  size="small"
                  @click="handleToggleEnabled(source)"
                  :loading="loading"
                >
                  {{ source.enabled ? "Disable" : "Enable" }}
                </el-button>
                <el-button
                  type="primary"
                  size="small"
                  @click="openEditDialog(source)"
                  :loading="loading"
                >
                  <i class="i-carbon-edit"></i>
                </el-button>
                <el-button
                  type="danger"
                  size="small"
                  @click="handleDelete(source)"
                  :loading="loading"
                >
                  <i class="i-carbon-trash-can"></i>
                </el-button>
              </div>
            </div>

            <div class="source-details">
              <div class="detail-row">
                <span class="detail-label">URL:</span>
                <code>{{ source.url }}</code>
              </div>
              <div class="detail-row">
                <span class="detail-label">Priority:</span>
                <span>{{ source.priority }} (lower = higher priority)</span>
              </div>
              <div class="detail-row">
                <span class="detail-label">Namespace:</span>
                <span>{{ source.namespace || "(global)" }}</span>
              </div>
              <div class="detail-row">
                <span class="detail-label">Created:</span>
                <span>{{ formatDate(source.created_at) }}</span>
              </div>
            </div>
          </div>
        </div>
      </el-card>

      <!-- Create/Edit Dialog -->
      <el-dialog
        v-model="dialogVisible"
        :title="
          dialogMode === 'create'
            ? 'Create Fallback Source'
            : 'Edit Fallback Source'
        "
        width="600px"
      >
        <el-form :model="formData" label-width="120px">
          <el-form-item label="Name" required>
            <el-input v-model="formData.name" placeholder="HuggingFace" />
          </el-form-item>

          <el-form-item label="URL" required>
            <el-input
              v-model="formData.url"
              placeholder="https://huggingface.co"
            />
          </el-form-item>

          <el-form-item label="Source Type" required>
            <el-select v-model="formData.source_type" style="width: 100%">
              <el-option label="HuggingFace" value="huggingface" />
              <el-option label="KohakuHub" value="kohakuhub" />
            </el-select>
          </el-form-item>

          <el-form-item label="Token">
            <el-input
              v-model="formData.token"
              type="password"
              placeholder="Optional API token (hf_xxx...)"
              show-password
            />
            <div class="form-help">
              Admin-configured token for accessing private repos
            </div>
          </el-form-item>

          <el-form-item label="Priority" required>
            <el-input-number
              v-model="formData.priority"
              :min="1"
              :max="1000"
              style="width: 100%"
            />
            <div class="form-help">
              Lower values = higher priority (checked first)
            </div>
          </el-form-item>

          <el-form-item label="Namespace">
            <el-input
              v-model="formData.namespace"
              placeholder="(empty for global)"
            />
            <div class="form-help">
              Empty = global, or specify user/org name
            </div>
          </el-form-item>

          <el-form-item label="Enabled">
            <el-switch v-model="formData.enabled" />
          </el-form-item>
        </el-form>

        <template #footer>
          <el-button @click="dialogVisible = false">Cancel</el-button>
          <el-button type="primary" @click="handleSubmit" :loading="loading">
            {{ dialogMode === "create" ? "Create" : "Update" }}
          </el-button>
        </template>
      </el-dialog>

      <!-- Per-repo cache eviction dialog (#79) -->
      <el-dialog
        v-model="evictRepoDialogVisible"
        title="Evict Fallback Cache by Repo"
        width="540px"
        data-testid="evict-repo-dialog"
      >
        <p class="dialog-help">
          Drop every cached source binding for one repository, across all
          users. Useful when you know an upstream repo's state changed and
          you don't want to wait for the TTL.
        </p>
        <el-form :model="evictRepoForm" label-width="120px">
          <el-form-item label="Repo type" required>
            <el-select
              v-model="evictRepoForm.repo_type"
              style="width: 100%"
              data-testid="evict-repo-type"
            >
              <el-option label="model" value="model" />
              <el-option label="dataset" value="dataset" />
              <el-option label="space" value="space" />
            </el-select>
          </el-form-item>
          <el-form-item label="Namespace" required>
            <el-autocomplete
              v-model="evictRepoForm.namespace"
              :fetch-suggestions="fetchNamespaceSuggestions"
              placeholder="owner / org name"
              clearable
              style="width: 100%"
              data-testid="evict-repo-namespace"
            />
          </el-form-item>
          <el-form-item label="Name" required>
            <el-autocomplete
              v-model="evictRepoForm.name"
              :fetch-suggestions="fetchRepoNameSuggestions"
              placeholder="repo name"
              clearable
              style="width: 100%"
              data-testid="evict-repo-name"
            />
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="evictRepoDialogVisible = false">Cancel</el-button>
          <el-button
            type="warning"
            @click="handleEvictRepo"
            :loading="loading"
            data-testid="evict-repo-submit"
          >
            Evict
          </el-button>
        </template>
      </el-dialog>

      <!-- Per-user cache eviction dialog (#79) -->
      <el-dialog
        v-model="evictUserDialogVisible"
        title="Evict Fallback Cache by User"
        width="540px"
        data-testid="evict-user-dialog"
      >
        <p class="dialog-help">
          Drop every cached source binding belonging to one user (across
          every repo they have a binding for). The next request from that
          user re-probes the chain. Address by username (resolved
          server-side) or numeric user_id (script-friendly).
        </p>
        <el-form :model="evictUserForm" label-width="120px">
          <el-form-item label="Mode" required>
            <el-radio-group
              v-model="evictUserForm.mode"
              data-testid="evict-user-mode"
            >
              <el-radio value="username">By username</el-radio>
              <el-radio value="user_id">By user_id</el-radio>
            </el-radio-group>
          </el-form-item>
          <el-form-item
            v-if="evictUserForm.mode === 'username'"
            label="Username"
            required
          >
            <el-autocomplete
              v-model="evictUserForm.username"
              :fetch-suggestions="fetchUsernameSuggestions"
              placeholder="e.g. mai_lin"
              clearable
              style="width: 100%"
              data-testid="evict-user-username"
            />
          </el-form-item>
          <el-form-item v-else label="User ID" required>
            <el-input-number
              v-model="evictUserForm.user_id"
              :min="1"
              style="width: 100%"
              data-testid="evict-user-userid"
            />
          </el-form-item>
        </el-form>
        <template #footer>
          <el-button @click="evictUserDialogVisible = false">Cancel</el-button>
          <el-button
            type="warning"
            @click="handleEvictUser"
            :loading="loading"
            data-testid="evict-user-submit"
          >
            Evict...
          </el-button>
        </template>
      </el-dialog>
    </div>
  </AdminLayout>
</template>

<style scoped>
.page-container {
  padding: 24px;
  max-width: 1280px;
  margin: 0 auto;
}

.page-header {
  margin-bottom: 24px;
}

.page-header h1 {
  font-size: 24px;
  font-weight: 600;
  margin-bottom: 8px;
}

.page-header p {
  color: var(--el-text-color-secondary);
  margin: 0;
}

.stats-card {
  margin-bottom: 20px;
}

.card-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.cache-actions {
  display: flex;
  gap: 8px;
  flex-wrap: wrap;
}

.dialog-help {
  margin: 0 0 16px 0;
  color: var(--el-text-color-secondary);
  font-size: 13px;
  line-height: 1.5;
}

.stats-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 20px;
}

.stat-item {
  text-align: center;
}

.stat-label {
  font-size: 14px;
  color: var(--el-text-color-secondary);
  margin-bottom: 8px;
}

.stat-value {
  font-size: 28px;
  font-weight: 600;
  color: var(--el-color-primary);
}

.sources-card {
  margin-bottom: 20px;
}

.empty-state {
  text-align: center;
  padding: 60px 20px;
  color: var(--el-text-color-secondary);
}

.empty-state i {
  display: block;
  margin-bottom: 16px;
}

.sources-list {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.source-item {
  border: 1px solid var(--el-border-color-light);
  border-radius: 8px;
  padding: 16px;
  transition: all 0.3s;
}

.source-item:hover {
  border-color: var(--el-color-primary);
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
}

.source-item.disabled {
  opacity: 0.5;
}

.source-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 12px;
}

.source-title h3 {
  font-size: 18px;
  font-weight: 600;
  margin: 0 0 8px 0;
}

.badges {
  display: flex;
  gap: 8px;
}

.source-actions {
  display: flex;
  gap: 8px;
}

.source-details {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.detail-row {
  display: flex;
  align-items: center;
  font-size: 14px;
}

.detail-label {
  font-weight: 600;
  min-width: 100px;
  color: var(--el-text-color-secondary);
}

.detail-row code {
  background: var(--el-fill-color-light);
  padding: 2px 8px;
  border-radius: 4px;
  font-family: "Consolas", "Monaco", monospace;
}

.form-help {
  font-size: 12px;
  color: var(--el-text-color-secondary);
  margin-top: 4px;
}
</style>
