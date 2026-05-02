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
  bulkReplaceFallbackSources,
  testFallbackChainSimulate,
  testFallbackChainReal,
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

// =====================================================================
// Chain Tester (#78). System-state draft + user-state simulation +
// probe runner with timeline rendering.
// =====================================================================

// HF-API-equivalent labels for the op dropdown — operators recognise
// these by their hf_hub method names rather than the internal op string.
const PROBE_OP_OPTIONS = [
  {
    value: "info",
    label: "Repo info  (HfApi.model_info / dataset_info / space_info)",
  },
  {
    value: "tree",
    label: "List files  (HfApi.list_repo_files / list_repo_tree)",
  },
  {
    value: "resolve",
    label: "Resolve file  (hf_hub_download / HEAD on /resolve/)",
  },
  {
    value: "paths_info",
    label: "paths_info  (HfApi.get_paths_info)",
  },
];
const PROBE_OPS = PROBE_OP_OPTIONS.map((o) => o.value);
const REPO_TYPES = ["model", "dataset", "space"];
const SOURCE_TYPES = ["huggingface", "kohakuhub"];

// Probe target / op selector.
const probeForm = ref({
  op: "info",
  repo_type: "model",
  namespace: "",
  name: "",
  revision: "main",
  file_path: "",
  paths_csv: "", // comma-separated; parsed to a list before submit
});

// System-state draft. Independent from ``sources`` (the live config).
// Edits accumulate; only ``pushDraftToSystem`` propagates them back.
const draftSources = ref([]);
const draftDirty = ref(false);

function _blankDraftSource() {
  return {
    namespace: "",
    url: "",
    token: "",
    priority: 100,
    name: "",
    source_type: "huggingface",
    enabled: true,
  };
}

function loadDraftFromSystem() {
  if (!checkAuth()) return;
  // Fresh deep copy of live sources into the draft area.
  draftSources.value = (sources.value || []).map((s) => ({
    namespace: s.namespace || "",
    url: s.url || "",
    token: s.token || "",
    priority: s.priority ?? 100,
    name: s.name || "",
    source_type: s.source_type || "huggingface",
    enabled: s.enabled !== false,
  }));
  draftDirty.value = false;
  ElMessage.success(
    `Loaded ${draftSources.value.length} source(s) from system into draft`,
  );
}

function discardDraft() {
  draftSources.value = [];
  draftDirty.value = false;
  ElMessage.info("Draft discarded");
}

function addDraftSource() {
  draftSources.value.push(_blankDraftSource());
  draftDirty.value = true;
}

function removeDraftSource(index) {
  draftSources.value.splice(index, 1);
  draftDirty.value = true;
}

function _markDirty() {
  draftDirty.value = true;
}

async function pushDraftToSystem() {
  if (!checkAuth()) return;
  // Strong confirm: bulk-replace touches every row + bumps cache gen.
  try {
    await ElMessageBox.confirm(
      `Replace the live fallback source list with the ${draftSources.value.length} draft entr${draftSources.value.length === 1 ? "y" : "ies"}? This atomically swaps the entire table and clears the bind cache.`,
      "Confirm Push Draft to System",
      {
        confirmButtonText: "Push",
        cancelButtonText: "Cancel",
        type: "warning",
      },
    );
  } catch (error) {
    if (error !== "cancel") {
      console.error("Push-draft confirm dismissed unexpectedly:", error);
    }
    return;
  }

  loading.value = true;
  try {
    // Sanitize draft rows: drop empty token strings (treat "" as no token).
    const sanitized = draftSources.value.map((s) => ({
      namespace: s.namespace || "",
      url: s.url,
      token: s.token ? s.token : null,
      priority: Number(s.priority) || 100,
      name: s.name,
      source_type: s.source_type,
      enabled: s.enabled !== false,
    }));
    const result = await bulkReplaceFallbackSources(adminStore.token, sanitized);
    ElMessage.success(
      `Pushed draft to system: ${result.before} → ${result.after} source(s)`,
    );
    draftDirty.value = false;
    await loadSources();
    await loadCacheStats();
  } catch (error) {
    console.error("Failed to push draft:", error);
    ElMessage.error(
      error.response?.data?.detail?.error ||
        "Failed to push draft to system",
    );
  } finally {
    loading.value = false;
  }
}

// User-state simulation.
const userSim = ref({
  mode: "anonymous", // "anonymous" | "username" | "user_id"
  username: "",
  user_id: null,
});
// Authorization-header-style overrides — list of {url, token} so the
// row order is stable and the form can render iteratively.
const headerTokens = ref([]);

function addHeaderToken() {
  headerTokens.value.push({ url: "", token: "" });
}
function removeHeaderToken(index) {
  headerTokens.value.splice(index, 1);
}

function _headerTokensToObj() {
  const out = {};
  for (const row of headerTokens.value) {
    if (row.url && row.token) out[row.url] = row.token;
  }
  return out;
}

// Probe runner.
const probeRunning = ref(false);
const probeReport = ref(null);
const probeError = ref(null);

function _buildBaseProbePayload() {
  const paths =
    probeForm.value.op === "paths_info" && probeForm.value.paths_csv
      ? probeForm.value.paths_csv
          .split(",")
          .map((p) => p.trim())
          .filter(Boolean)
      : null;
  return {
    op: probeForm.value.op,
    repo_type: probeForm.value.repo_type,
    namespace: probeForm.value.namespace,
    name: probeForm.value.name,
    revision: probeForm.value.revision || "main",
    file_path: probeForm.value.file_path || "",
    paths,
  };
}

function _validateProbeTarget() {
  if (!probeForm.value.namespace || !probeForm.value.name) {
    ElMessage.error("namespace and name are required for the probe target");
    return false;
  }
  return true;
}

async function runProbeSimulate() {
  if (!checkAuth()) return;
  if (!_validateProbeTarget()) return;
  if (draftSources.value.length === 0) {
    ElMessage.warning(
      "Draft is empty — load from system first or add at least one draft source",
    );
    return;
  }
  probeRunning.value = true;
  probeError.value = null;
  try {
    const payload = {
      ..._buildBaseProbePayload(),
      sources: draftSources.value.map((s) => ({
        name: s.name || s.url,
        url: s.url,
        source_type: s.source_type,
        token: s.token || null,
        priority: Number(s.priority) || 100,
      })),
      user_tokens: _headerTokensToObj(),
    };
    probeReport.value = await testFallbackChainSimulate(
      adminStore.token,
      payload,
    );
  } catch (error) {
    console.error("simulate probe failed:", error);
    probeError.value =
      error.response?.data?.detail?.error || "Simulate probe failed";
    probeReport.value = null;
  } finally {
    probeRunning.value = false;
  }
}

async function runProbeReal() {
  if (!checkAuth()) return;
  if (!_validateProbeTarget()) return;
  probeRunning.value = true;
  probeError.value = null;
  try {
    const payload = {
      ..._buildBaseProbePayload(),
      header_tokens: _headerTokensToObj(),
    };
    if (userSim.value.mode === "username" && userSim.value.username) {
      payload.as_username = userSim.value.username.trim();
    } else if (
      userSim.value.mode === "user_id" &&
      userSim.value.user_id != null &&
      Number(userSim.value.user_id) > 0
    ) {
      payload.as_user_id = Number(userSim.value.user_id);
    }
    probeReport.value = await testFallbackChainReal(adminStore.token, payload);
  } catch (error) {
    console.error("real probe failed:", error);
    probeError.value =
      error.response?.data?.detail?.error || "Real probe failed";
    probeReport.value = null;
  } finally {
    probeRunning.value = false;
  }
}

function decisionTagType(decision) {
  switch (decision) {
    case "BIND_AND_RESPOND":
      return "success";
    case "BIND_AND_PROPAGATE":
      return "warning";
    case "TRY_NEXT_SOURCE":
      return "info";
    case "TIMEOUT":
    case "NETWORK_ERROR":
      return "danger";
    default:
      return "info";
  }
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

      <!-- Chain Tester Card (#78) -->
      <el-card
        class="tester-card"
        shadow="hover"
        data-testid="tester-card"
      >
        <template #header>
          <div class="card-header">
            <span>
              Chain Tester
              <el-tag
                v-if="draftDirty"
                type="warning"
                size="small"
                class="ml-2"
                data-testid="draft-dirty-tag"
              >
                Draft modified
              </el-tag>
            </span>
            <div class="cache-actions">
              <el-button
                size="small"
                @click="loadDraftFromSystem"
                :loading="loading"
                data-testid="load-from-system-btn"
              >
                <i class="i-carbon-download mr-1"></i>
                Load from System
              </el-button>
              <el-button
                type="primary"
                size="small"
                :disabled="!draftDirty || draftSources.length === 0"
                @click="pushDraftToSystem"
                :loading="loading"
                data-testid="push-to-system-btn"
              >
                <i class="i-carbon-upload mr-1"></i>
                Push to System
              </el-button>
              <el-button
                size="small"
                :disabled="!draftDirty && draftSources.length === 0"
                @click="discardDraft"
                data-testid="discard-draft-btn"
              >
                Discard Draft
              </el-button>
            </div>
          </div>
        </template>

        <p class="tester-help">
          Edit a system-state draft and a user-state simulation, run a probe to
          see how the chain would resolve, then push the draft back to the live
          configuration once you're satisfied. Simulate runs against the draft
          (no live impact); Real runs against the current live config and the
          impersonated identity. Neither writes the production cache.
        </p>

        <!-- Probe target / op -->
        <h3 class="tester-section-title">Probe target</h3>
        <el-form :model="probeForm" label-width="120px" class="tester-form">
          <div class="tester-grid">
            <el-form-item label="Op" required>
              <el-select
                v-model="probeForm.op"
                style="width: 100%"
                data-testid="probe-op"
              >
                <el-option
                  v-for="opt in PROBE_OP_OPTIONS"
                  :key="opt.value"
                  :label="opt.label"
                  :value="opt.value"
                />
              </el-select>
            </el-form-item>
            <el-form-item label="Repo type" required>
              <el-select
                v-model="probeForm.repo_type"
                style="width: 100%"
                data-testid="probe-repo-type"
              >
                <el-option
                  v-for="rt in REPO_TYPES"
                  :key="rt"
                  :label="rt"
                  :value="rt"
                />
              </el-select>
            </el-form-item>
            <el-form-item label="Namespace" required>
              <el-input
                v-model="probeForm.namespace"
                placeholder="owner / org"
                data-testid="probe-namespace"
              />
            </el-form-item>
            <el-form-item label="Name" required>
              <el-input
                v-model="probeForm.name"
                placeholder="repo name"
                data-testid="probe-name"
              />
            </el-form-item>
            <el-form-item label="Revision">
              <el-input
                v-model="probeForm.revision"
                placeholder="main"
                data-testid="probe-revision"
              />
            </el-form-item>
            <el-form-item
              v-if="probeForm.op === 'resolve' || probeForm.op === 'tree'"
              label="File / path"
            >
              <el-input
                v-model="probeForm.file_path"
                :placeholder="
                  probeForm.op === 'resolve' ? 'config.json' : '/'
                "
                data-testid="probe-file-path"
              />
            </el-form-item>
            <el-form-item
              v-if="probeForm.op === 'paths_info'"
              label="Paths (CSV)"
            >
              <el-input
                v-model="probeForm.paths_csv"
                placeholder="config.json, README.md"
                data-testid="probe-paths-csv"
              />
            </el-form-item>
          </div>
        </el-form>

        <!-- System state draft -->
        <h3 class="tester-section-title">
          System state — draft
          <span class="tester-section-hint">
            ({{ draftSources.length }} source{{ draftSources.length === 1 ? "" : "s" }})
          </span>
        </h3>
        <div class="tester-empty" v-if="draftSources.length === 0">
          Draft is empty. Click
          <strong>Load from System</strong> to seed it with the current
          configuration, or
          <a href="javascript:void(0)" @click="addDraftSource">add a source</a>
          manually.
        </div>
        <div
          v-else
          class="draft-list"
          data-testid="draft-list"
        >
          <div
            v-for="(src, idx) in draftSources"
            :key="idx"
            class="draft-row"
            :data-testid="`draft-row-${idx}`"
          >
            <div class="draft-row-grid">
              <el-form-item label="Name">
                <el-input
                  v-model="src.name"
                  @input="_markDirty"
                  placeholder="HuggingFace"
                />
              </el-form-item>
              <el-form-item label="URL">
                <el-input
                  v-model="src.url"
                  @input="_markDirty"
                  placeholder="https://huggingface.co"
                />
              </el-form-item>
              <el-form-item label="Type">
                <el-select
                  v-model="src.source_type"
                  @change="_markDirty"
                  style="width: 100%"
                >
                  <el-option
                    v-for="st in SOURCE_TYPES"
                    :key="st"
                    :label="st"
                    :value="st"
                  />
                </el-select>
              </el-form-item>
              <el-form-item label="Priority">
                <el-input-number
                  v-model="src.priority"
                  :min="1"
                  :max="1000"
                  @change="_markDirty"
                  style="width: 100%"
                />
              </el-form-item>
              <el-form-item label="Namespace">
                <el-input
                  v-model="src.namespace"
                  @input="_markDirty"
                  placeholder="(empty for global)"
                />
              </el-form-item>
              <el-form-item label="Token">
                <el-input
                  v-model="src.token"
                  @input="_markDirty"
                  type="password"
                  placeholder="hf_xxx (optional)"
                  show-password
                />
              </el-form-item>
              <el-form-item label="Enabled">
                <el-switch v-model="src.enabled" @change="_markDirty" />
              </el-form-item>
            </div>
            <div class="draft-row-actions">
              <el-button
                size="small"
                type="danger"
                @click="removeDraftSource(idx)"
                :data-testid="`draft-remove-${idx}`"
              >
                <i class="i-carbon-trash-can"></i>
              </el-button>
            </div>
          </div>
        </div>
        <div class="draft-add-row">
          <el-button
            size="small"
            @click="addDraftSource"
            data-testid="draft-add-btn"
          >
            <i class="i-carbon-add mr-1"></i>
            Add Draft Source
          </el-button>
        </div>

        <!-- User state simulation -->
        <h3 class="tester-section-title">User state — simulation</h3>
        <el-form
          :model="userSim"
          label-width="120px"
          class="tester-form"
        >
          <el-form-item label="Identity">
            <el-radio-group v-model="userSim.mode" data-testid="user-sim-mode">
              <el-radio value="anonymous">Anonymous</el-radio>
              <el-radio value="username">By username</el-radio>
              <el-radio value="user_id">By user_id</el-radio>
            </el-radio-group>
          </el-form-item>
          <el-form-item v-if="userSim.mode === 'username'" label="Username">
            <el-input
              v-model="userSim.username"
              placeholder="e.g. mai_lin"
              data-testid="user-sim-username"
            />
          </el-form-item>
          <el-form-item v-if="userSim.mode === 'user_id'" label="User ID">
            <el-input-number
              v-model="userSim.user_id"
              :min="1"
              style="width: 100%"
              data-testid="user-sim-userid"
            />
          </el-form-item>
        </el-form>
        <h4 class="tester-subsection-title">
          Authorization-header overrides
          <span class="tester-section-hint">
            (per-URL token overrides applied on top of the system source list,
            mirrors the
            <code>Bearer xxx|url,token|...</code>
            client format)
          </span>
        </h4>
        <div
          v-if="headerTokens.length === 0"
          class="tester-empty"
        >
          No header overrides.
          <a href="javascript:void(0)" @click="addHeaderToken">Add one</a>.
        </div>
        <div v-else class="header-tokens" data-testid="header-tokens">
          <div
            v-for="(row, idx) in headerTokens"
            :key="idx"
            class="header-token-row"
          >
            <el-input
              v-model="row.url"
              placeholder="https://huggingface.co"
              :data-testid="`header-token-url-${idx}`"
              style="flex: 2"
            />
            <el-input
              v-model="row.token"
              type="password"
              placeholder="hf_xxx"
              show-password
              :data-testid="`header-token-token-${idx}`"
              style="flex: 2"
            />
            <el-button
              size="small"
              type="danger"
              @click="removeHeaderToken(idx)"
              :data-testid="`header-token-remove-${idx}`"
            >
              <i class="i-carbon-trash-can"></i>
            </el-button>
          </div>
        </div>
        <div class="draft-add-row">
          <el-button
            size="small"
            @click="addHeaderToken"
            data-testid="header-token-add"
          >
            <i class="i-carbon-add mr-1"></i>
            Add header override
          </el-button>
        </div>

        <!-- Run + results -->
        <h3 class="tester-section-title">Run probe</h3>
        <div class="run-buttons">
          <el-button
            type="warning"
            @click="runProbeSimulate"
            :loading="probeRunning"
            data-testid="run-simulate-btn"
          >
            <i class="i-carbon-play mr-1"></i>
            Run with draft (Simulate)
          </el-button>
          <el-button
            type="primary"
            @click="runProbeReal"
            :loading="probeRunning"
            data-testid="run-real-btn"
          >
            <i class="i-carbon-play-filled mr-1"></i>
            Run with live config (Real)
          </el-button>
        </div>

        <div
          v-if="probeError"
          class="probe-error"
          data-testid="probe-error"
        >
          {{ probeError }}
        </div>

        <div
          v-if="probeReport"
          class="probe-report"
          data-testid="probe-report"
        >
          <div class="probe-report-summary">
            <strong>Final outcome:</strong>
            <el-tag
              :type="
                probeReport.final_outcome === 'BIND_AND_RESPOND'
                  ? 'success'
                  : probeReport.final_outcome === 'BIND_AND_PROPAGATE'
                  ? 'warning'
                  : 'info'
              "
              data-testid="probe-final-outcome"
            >
              {{ probeReport.final_outcome }}
            </el-tag>
            <span v-if="probeReport.bound_source" class="probe-bound-source">
              bound to
              <code data-testid="probe-bound-source">
                {{ probeReport.bound_source.name || probeReport.bound_source.url }}
              </code>
            </span>
            <span class="probe-duration">
              {{ probeReport.duration_ms }} ms total
            </span>
          </div>
          <div class="probe-attempts">
            <div
              v-for="(att, idx) in probeReport.attempts"
              :key="idx"
              class="probe-attempt"
              :data-testid="`probe-attempt-${idx}`"
            >
              <div class="probe-attempt-line">
                <el-tag
                  :type="decisionTagType(att.decision)"
                  size="small"
                >
                  {{ att.decision }}
                </el-tag>
                <code class="probe-attempt-source">
                  {{ att.source_name }}
                </code>
                <span class="probe-attempt-method">
                  {{ att.method }}
                </span>
                <span class="probe-attempt-status">
                  <span v-if="att.status_code">{{ att.status_code }}</span>
                  <span v-else class="probe-attempt-error">no response</span>
                </span>
                <span
                  v-if="att.x_error_code"
                  class="probe-attempt-xerror"
                >
                  X-Error-Code: {{ att.x_error_code }}
                </span>
                <span class="probe-attempt-ms">
                  {{ att.duration_ms }} ms
                </span>
              </div>
              <div
                v-if="att.upstream_path"
                class="probe-attempt-path"
              >
                <span class="probe-label">Upstream:</span>
                <code>{{ att.upstream_path }}</code>
              </div>
              <div
                v-if="att.response_headers && Object.keys(att.response_headers).length > 0"
                class="probe-attempt-headers"
                :data-testid="`probe-attempt-${idx}-headers`"
              >
                <span class="probe-label">Response headers:</span>
                <code
                  v-for="(val, key) in att.response_headers"
                  :key="key"
                  class="probe-attempt-header"
                >
                  {{ key }}: {{ val }}
                </code>
              </div>
              <details
                v-if="att.response_body_preview"
                class="probe-attempt-body"
              >
                <summary>
                  Response body preview ({{
                    att.response_body_preview.length
                  }}
                  chars)
                </summary>
                <pre :data-testid="`probe-attempt-${idx}-body`">{{ att.response_body_preview }}</pre>
              </details>
              <div
                v-if="att.error"
                class="probe-attempt-error-detail"
              >
                {{ att.error }}
              </div>
            </div>
          </div>

          <div
            v-if="probeReport.final_response"
            class="probe-final-response"
            data-testid="probe-final-response"
          >
            <h4 class="probe-final-title">
              Final response (what a production caller would see)
            </h4>
            <div class="probe-final-status">
              <span class="probe-label">Status:</span>
              <strong>{{ probeReport.final_response.status_code }}</strong>
            </div>
            <div
              v-if="probeReport.final_response.headers && Object.keys(probeReport.final_response.headers).length > 0"
              class="probe-final-headers"
            >
              <span class="probe-label">Headers:</span>
              <code
                v-for="(val, key) in probeReport.final_response.headers"
                :key="key"
                class="probe-attempt-header"
              >
                {{ key }}: {{ val }}
              </code>
            </div>
            <details
              v-if="probeReport.final_response.body_preview"
              open
              class="probe-attempt-body"
            >
              <summary>Body</summary>
              <pre data-testid="probe-final-body">{{ probeReport.final_response.body_preview }}</pre>
            </details>
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

/* Chain Tester (#78) */
.tester-card {
  margin-bottom: 20px;
}

.tester-help {
  margin: 0 0 16px 0;
  color: var(--el-text-color-secondary);
  font-size: 13px;
  line-height: 1.5;
}

.tester-section-title {
  font-size: 16px;
  font-weight: 600;
  margin: 24px 0 12px 0;
  display: flex;
  align-items: center;
  gap: 8px;
}

.tester-subsection-title {
  font-size: 14px;
  font-weight: 500;
  margin: 16px 0 8px 0;
  color: var(--el-text-color-secondary);
}

.tester-section-hint {
  font-weight: 400;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.tester-form {
  margin-bottom: 16px;
}

.tester-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 0 24px;
}

.tester-empty {
  padding: 12px 16px;
  border: 1px dashed var(--el-border-color);
  border-radius: 4px;
  color: var(--el-text-color-secondary);
  font-size: 13px;
  margin-bottom: 12px;
}

.draft-list {
  display: flex;
  flex-direction: column;
  gap: 12px;
  margin-bottom: 12px;
}

.draft-row {
  display: flex;
  align-items: flex-start;
  gap: 12px;
  padding: 12px;
  border: 1px solid var(--el-border-color-light);
  border-radius: 6px;
  background: var(--el-bg-color-page);
}

.draft-row-grid {
  display: grid;
  grid-template-columns: repeat(2, 1fr);
  gap: 0 16px;
  flex: 1;
}

.draft-row-actions {
  display: flex;
  flex-direction: column;
  gap: 4px;
  padding-top: 4px;
}

.draft-add-row {
  margin-bottom: 12px;
}

.header-tokens {
  display: flex;
  flex-direction: column;
  gap: 8px;
  margin-bottom: 12px;
}

.header-token-row {
  display: flex;
  align-items: center;
  gap: 8px;
}

.run-buttons {
  display: flex;
  gap: 12px;
  margin-bottom: 16px;
  flex-wrap: wrap;
}

.probe-error {
  padding: 12px 16px;
  border: 1px solid var(--el-color-danger);
  border-radius: 4px;
  background: var(--el-color-danger-light-9);
  color: var(--el-color-danger);
  font-size: 13px;
  margin-bottom: 12px;
}

.probe-report {
  border-top: 1px solid var(--el-border-color);
  padding-top: 16px;
}

.probe-report-summary {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
  margin-bottom: 16px;
  font-size: 14px;
}

.probe-bound-source {
  color: var(--el-text-color-secondary);
}

.probe-duration {
  color: var(--el-text-color-secondary);
  font-size: 12px;
  margin-left: auto;
}

.probe-attempts {
  display: flex;
  flex-direction: column;
  gap: 8px;
}

.probe-attempt {
  padding: 12px;
  border: 1px solid var(--el-border-color-light);
  border-radius: 4px;
  background: var(--el-bg-color-page);
}

.probe-attempt-line {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
  font-size: 13px;
}

.probe-attempt-source {
  font-weight: 600;
}

.probe-attempt-method {
  color: var(--el-text-color-secondary);
  font-size: 12px;
}

.probe-attempt-status {
  font-family: monospace;
  font-size: 13px;
}

.probe-attempt-xerror {
  font-size: 12px;
  color: var(--el-color-warning);
}

.probe-attempt-ms {
  margin-left: auto;
  color: var(--el-text-color-secondary);
  font-size: 12px;
}

.probe-attempt-path,
.probe-attempt-headers {
  margin-top: 8px;
  font-size: 12px;
  display: flex;
  flex-wrap: wrap;
  gap: 6px;
  align-items: baseline;
}

.probe-label {
  color: var(--el-text-color-secondary);
  font-size: 12px;
  margin-right: 4px;
}

.probe-attempt-header {
  background: var(--el-fill-color-light);
  padding: 2px 6px;
  border-radius: 3px;
  font-size: 11px;
}

.probe-attempt-body {
  margin-top: 8px;
  font-size: 12px;
}

.probe-attempt-body pre {
  margin: 6px 0 0 0;
  padding: 8px;
  background: var(--el-bg-color);
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 4px;
  max-height: 320px;
  overflow: auto;
  font-size: 12px;
  white-space: pre-wrap;
  word-break: break-all;
}

.probe-attempt-error-detail {
  margin-top: 6px;
  font-size: 12px;
  color: var(--el-color-danger);
}

.probe-final-response {
  margin-top: 16px;
  padding: 12px;
  border: 1px solid var(--el-color-success);
  border-radius: 4px;
  background: var(--el-color-success-light-9);
}

.probe-final-title {
  margin: 0 0 8px 0;
  font-size: 14px;
  font-weight: 600;
}

.probe-final-status {
  font-size: 13px;
  margin-bottom: 6px;
}

.probe-final-headers {
  margin-bottom: 6px;
  font-size: 12px;
}
</style>
