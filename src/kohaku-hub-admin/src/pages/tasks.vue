<script setup>
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { useRouter } from "vue-router";
import AdminLayout from "@/components/AdminLayout.vue";
import { useAdminStore } from "@/stores/admin";
import { deleteTask, getTask, listTasks, retryTask } from "@/utils/api";
import { ElMessage, ElMessageBox } from "element-plus";
import dayjs from "dayjs";

const router = useRouter();
const adminStore = useAdminStore();

const STATUSES = ["queued", "running", "succeeded", "failed"];
const STATUS_TAG = {
  queued: "info",
  running: "primary",
  succeeded: "success",
  failed: "danger",
};
const REFRESH_OPTIONS = [
  { label: "Off", value: 0 },
  { label: "Every 5 s", value: 5 },
  { label: "Every 15 s", value: 15 },
  { label: "Every 60 s", value: 60 },
];

const loading = ref(false);
const tasks = ref([]);
const total = ref(0);
const counts = ref(Object.fromEntries(STATUSES.map((s) => [s, 0])));
const kinds = ref([]);
const filterStatus = ref("");
const filterKind = ref("");
const currentPage = ref(1);
const pageSize = ref(20);
const refreshIntervalSeconds = ref(0);
const detail = ref(null);
const detailVisible = ref(false);
let refreshTimer = null;

const statusOptions = computed(() => [
  { label: "All statuses", value: "" },
  ...STATUSES.map((s) => ({ label: s, value: s })),
]);
const kindOptions = computed(() => [
  { label: "All kinds", value: "" },
  ...kinds.value.map((k) => ({ label: k, value: k })),
]);

function handleError(error, fallback) {
  if (error.response?.status === 401 || error.response?.status === 403) {
    ElMessage.error("Invalid admin token. Please login again.");
    adminStore.logout();
    router.push("/login");
    return;
  }
  ElMessage.error(error.response?.data?.detail?.error || fallback);
}

async function loadTasks() {
  if (!adminStore.token) {
    router.push("/login");
    return;
  }
  loading.value = true;
  try {
    const response = await listTasks(adminStore.token, {
      status: filterStatus.value || undefined,
      kind: filterKind.value || undefined,
      limit: pageSize.value,
      offset: (currentPage.value - 1) * pageSize.value,
    });
    tasks.value = response.tasks;
    total.value = response.total;
    counts.value = response.counts;
    kinds.value = response.kinds;
  } catch (error) {
    handleError(error, "Failed to load background tasks");
  } finally {
    loading.value = false;
  }
}

function applyFilters() {
  currentPage.value = 1;
  loadTasks();
}

function selectStatus(status) {
  filterStatus.value = filterStatus.value === status ? "" : status;
  applyFilters();
}

function handlePageChange(page) {
  currentPage.value = page;
  loadTasks();
}

async function openDetail(task) {
  try {
    detail.value = await getTask(adminStore.token, task.id);
    detailVisible.value = true;
  } catch (error) {
    handleError(error, "Failed to load task");
  }
}

async function confirmAction(message, title) {
  try {
    await ElMessageBox.confirm(message, title, { type: "warning" });
    return true;
  } catch {
    return false;
  }
}

async function handleRetry(task) {
  if (
    !(await confirmAction(
      `Requeue task #${task.id} (${task.kind})?`,
      "Retry task",
    ))
  ) {
    return;
  }
  try {
    await retryTask(adminStore.token, task.id);
    ElMessage.success(`Task #${task.id} requeued`);
    detailVisible.value = false;
    await loadTasks();
  } catch (error) {
    handleError(error, "Failed to retry task");
  }
}

async function handleDiscard(task) {
  if (
    !(await confirmAction(
      `Discard task #${task.id} (${task.kind})? This cannot be undone.`,
      "Discard task",
    ))
  ) {
    return;
  }
  try {
    await deleteTask(adminStore.token, task.id);
    ElMessage.success(`Task #${task.id} discarded`);
    detailVisible.value = false;
    await loadTasks();
  } catch (error) {
    handleError(error, "Failed to discard task");
  }
}

function canRetry(task) {
  return task.status === "failed";
}

function canDiscard(task) {
  return task.status === "queued" || task.status === "failed";
}

function formatDate(value) {
  return value ? dayjs(value).format("YYYY-MM-DD HH:mm:ss") : "—";
}

// The one timestamp that matters for the row's current state.
function stateTime(task) {
  switch (task.status) {
    case "queued":
      return `due ${formatDate(task.run_after)}`;
    case "running":
      return `started ${formatDate(task.started_at)}`;
    default:
      return `finished ${formatDate(task.finished_at)}`;
  }
}

function truncate(text, maxLength = 80) {
  if (!text) return "";
  return text.length > maxLength ? `${text.slice(0, maxLength)}…` : text;
}

function stopTimer() {
  if (refreshTimer) {
    clearInterval(refreshTimer);
    refreshTimer = null;
  }
}

watch(refreshIntervalSeconds, (seconds) => {
  stopTimer();
  if (seconds > 0) {
    refreshTimer = setInterval(loadTasks, seconds * 1000);
  }
});

onMounted(loadTasks);
onBeforeUnmount(stopTimer);
</script>

<template>
  <AdminLayout>
    <div class="page-container">
      <div class="flex justify-between items-center mb-6 gap-4 flex-wrap">
        <div>
          <h1 class="text-3xl font-bold text-gray-900 dark:text-gray-100">
            Background Tasks
          </h1>
          <p class="text-gray-500 dark:text-gray-400 text-sm mt-1">
            Durable tasks executed by <code>khub-worker</code>. Tasks run at
            least once; failed tasks can be retried or discarded here.
          </p>
        </div>
        <div class="flex items-center gap-3">
          <el-select
            v-model="refreshIntervalSeconds"
            class="refresh-select"
            placeholder="Auto-refresh"
            data-testid="tasks-refresh-interval"
          >
            <el-option
              v-for="option in REFRESH_OPTIONS"
              :key="option.value"
              :label="option.label"
              :value="option.value"
            />
          </el-select>
          <el-button
            type="primary"
            :loading="loading"
            @click="loadTasks()"
            data-testid="tasks-refresh"
          >
            <div class="i-carbon-renew mr-1" />
            Refresh
          </el-button>
        </div>
      </div>

      <div class="status-cards mb-4">
        <button
          v-for="status in STATUSES"
          :key="status"
          type="button"
          class="status-card"
          :class="{ active: filterStatus === status }"
          :data-testid="`tasks-count-${status}`"
          @click="selectStatus(status)"
        >
          <span class="status-label">{{ status }}</span>
          <span class="status-value">{{ counts[status] }}</span>
        </button>
      </div>

      <el-card shadow="never">
        <div class="flex gap-3 mb-4 flex-wrap">
          <el-select
            v-model="filterStatus"
            class="filter-select"
            placeholder="Status"
            data-testid="tasks-filter-status"
            @change="applyFilters"
          >
            <el-option
              v-for="option in statusOptions"
              :key="option.value"
              :label="option.label"
              :value="option.value"
            />
          </el-select>
          <el-select
            v-model="filterKind"
            class="filter-select"
            placeholder="Kind"
            data-testid="tasks-filter-kind"
            @change="applyFilters"
          >
            <el-option
              v-for="option in kindOptions"
              :key="option.value"
              :label="option.label"
              :value="option.value"
            />
          </el-select>
        </div>

        <el-empty
          v-if="!loading && tasks.length === 0"
          description="No background tasks"
          data-testid="tasks-empty"
        />
        <el-table
          v-else
          v-loading="loading"
          :data="tasks"
          data-testid="tasks-table"
        >
          <el-table-column prop="id" label="ID" width="90" />
          <el-table-column prop="kind" label="Kind" min-width="180" />
          <el-table-column label="Status" width="170">
            <template #default="{ row }">
              <el-tag :type="STATUS_TAG[row.status]" size="small">
                {{ row.status }}
              </el-tag>
              <el-tag
                v-if="row.lease_expired"
                type="warning"
                size="small"
                class="ml-1"
                data-testid="tasks-lease-expired"
              >
                lease expired
              </el-tag>
            </template>
          </el-table-column>
          <el-table-column label="Attempts" width="100">
            <template #default="{ row }">
              {{ row.attempts }} / {{ row.max_attempts }}
            </template>
          </el-table-column>
          <el-table-column label="When" min-width="200">
            <template #default="{ row }">{{ stateTime(row) }}</template>
          </el-table-column>
          <el-table-column label="Last error" min-width="220">
            <template #default="{ row }">
              <span class="error-text">{{ truncate(row.last_error) }}</span>
            </template>
          </el-table-column>
          <el-table-column label="Actions" width="240" fixed="right">
            <template #default="{ row }">
              <el-button
                size="small"
                :data-testid="`tasks-detail-${row.id}`"
                @click="openDetail(row)"
              >
                Details
              </el-button>
              <el-button
                v-if="canRetry(row)"
                size="small"
                type="warning"
                :data-testid="`tasks-retry-${row.id}`"
                @click="handleRetry(row)"
              >
                Retry
              </el-button>
              <el-button
                v-if="canDiscard(row)"
                size="small"
                type="danger"
                :data-testid="`tasks-discard-${row.id}`"
                @click="handleDiscard(row)"
              >
                Discard
              </el-button>
            </template>
          </el-table-column>
        </el-table>

        <div class="flex justify-end mt-4">
          <el-pagination
            :current-page="currentPage"
            :page-size="pageSize"
            :total="total"
            layout="total, prev, pager, next"
            data-testid="tasks-pagination"
            @current-change="handlePageChange"
          />
        </div>
      </el-card>

      <el-dialog
        v-model="detailVisible"
        :title="detail ? `Task #${detail.id}` : 'Task'"
        width="720px"
      >
        <div v-if="detail" data-testid="tasks-detail">
          <dl class="detail-grid">
            <dt>Kind</dt>
            <dd>{{ detail.kind }}</dd>
            <dt>Status</dt>
            <dd>{{ detail.status }}</dd>
            <dt>Queue</dt>
            <dd>{{ detail.queue }}</dd>
            <dt>Priority</dt>
            <dd>{{ detail.priority }}</dd>
            <dt>Attempts</dt>
            <dd>{{ detail.attempts }} / {{ detail.max_attempts }}</dd>
            <dt>Dedupe key</dt>
            <dd>{{ detail.dedupe_key || "—" }}</dd>
            <dt>Worker</dt>
            <dd>{{ detail.locked_by || "—" }}</dd>
            <dt>Lease until</dt>
            <dd>{{ formatDate(detail.locked_until) }}</dd>
            <dt>Created</dt>
            <dd>{{ formatDate(detail.created_at) }}</dd>
            <dt>Run after</dt>
            <dd>{{ formatDate(detail.run_after) }}</dd>
            <dt>Started</dt>
            <dd>{{ formatDate(detail.started_at) }}</dd>
            <dt>Finished</dt>
            <dd>{{ formatDate(detail.finished_at) }}</dd>
          </dl>
          <h3 class="section-title">Payload</h3>
          <pre class="code-block">{{
            JSON.stringify(detail.payload, null, 2)
          }}</pre>
          <template v-if="detail.last_error">
            <h3 class="section-title">Last error</h3>
            <pre class="code-block error-text">{{ detail.last_error }}</pre>
          </template>
        </div>
        <template #footer>
          <el-button
            v-if="detail && canRetry(detail)"
            type="warning"
            data-testid="tasks-detail-retry"
            @click="handleRetry(detail)"
          >
            Retry
          </el-button>
          <el-button
            v-if="detail && canDiscard(detail)"
            type="danger"
            data-testid="tasks-detail-discard"
            @click="handleDiscard(detail)"
          >
            Discard
          </el-button>
          <el-button @click="detailVisible = false">Close</el-button>
        </template>
      </el-dialog>
    </div>
  </AdminLayout>
</template>

<style scoped>
.refresh-select {
  width: 160px;
}

.filter-select {
  width: 220px;
}

.status-cards {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 12px;
}

.status-card {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 4px;
  padding: 14px 16px;
  border: 1px solid var(--el-border-color);
  border-radius: 10px;
  background: var(--el-bg-color);
  cursor: pointer;
  text-align: left;
}

.status-card.active {
  border-color: var(--el-color-primary);
  box-shadow: 0 0 0 1px var(--el-color-primary);
}

.status-label {
  font-size: 12px;
  color: var(--el-text-color-secondary);
  text-transform: uppercase;
  letter-spacing: 0.04em;
}

.status-value {
  font-size: 24px;
  font-weight: 600;
  color: var(--el-text-color-primary);
}

.error-text {
  color: var(--el-color-danger);
}

.detail-grid {
  display: grid;
  grid-template-columns: 140px 1fr;
  gap: 6px 12px;
  margin: 0 0 16px;
}

.detail-grid dt {
  color: var(--el-text-color-secondary);
}

.detail-grid dd {
  margin: 0;
  word-break: break-all;
}

.section-title {
  font-weight: 600;
  margin: 12px 0 6px;
}

.code-block {
  background: var(--el-fill-color-light);
  border-radius: 6px;
  padding: 10px 12px;
  font-size: 12px;
  white-space: pre-wrap;
  word-break: break-all;
  max-height: 260px;
  overflow: auto;
}
</style>
