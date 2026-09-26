<script setup>
import { computed } from "vue";
import { formatDate, formatSeconds } from "./taskFormat.js";

// The worker roster: every worker process, whether it is alive, how busy it
// is and what it is running. Rows are never deleted; long-inactive workers
// are hidden unless asked for.
const props = defineProps({
  roster: { type: Object, required: true },
  highlight: { type: String, default: null },
  includeInactive: { type: Boolean, default: false },
});
const emit = defineEmits(["update:includeInactive", "select-task"]);

const STATUS = {
  online: { tag: "success", label: "online" },
  draining: { tag: "warning", label: "draining" },
  lost: { tag: "danger", label: "lost" },
  stopped: { tag: "info", label: "stopped" },
};

const workers = computed(() =>
  props.roster.workers.map((worker) => ({
    ...worker,
    load: worker.concurrency
      ? Math.round((worker.running / worker.concurrency) * 100)
      : 0,
    seen:
      worker.status === "stopped"
        ? `stopped ${formatDate(worker.stopped_at)}`
        : `heartbeat ${formatSeconds(worker.heartbeat_age_seconds)} ago`,
  })),
);

const inactiveHours = computed(() =>
  Math.round(props.roster.inactive_after_seconds / 3600),
);

function rowClass({ row }) {
  return row.id === props.highlight ? "highlighted" : "";
}
</script>

<template>
  <div data-testid="task-workers">
    <div class="roster-summary">
      <span
        v-for="(meta, status) in STATUS"
        :key="status"
        class="summary-pill"
        :class="meta.tag"
        :data-testid="`workers-count-${status}`"
      >
        <span class="dot" />{{ meta.label }}
        <strong>{{ roster.counts[status] }}</strong>
      </span>
      <el-checkbox
        class="inactive-toggle"
        :model-value="includeInactive"
        data-testid="workers-include-inactive"
        @update:model-value="emit('update:includeInactive', $event)"
      >
        Show workers inactive for over {{ inactiveHours }}h
        <span v-if="roster.hidden" class="hidden-count"
          >({{ roster.hidden }} hidden)</span
        >
      </el-checkbox>
    </div>

    <el-empty
      v-if="workers.length === 0"
      description="No worker has registered yet. Start one with `make worker` or the khub-worker service."
      data-testid="workers-empty"
    />
    <el-table
      v-else
      :data="workers"
      :row-class-name="rowClass"
      data-testid="workers-table"
    >
      <el-table-column label="Worker" min-width="270">
        <template #default="{ row }">
          <div class="worker-name" :data-testid="`worker-${row.id}`">
            {{ row.name }}
          </div>
          <div class="worker-id">{{ row.id }}</div>
          <div class="worker-id">pid {{ row.pid }}</div>
        </template>
      </el-table-column>
      <el-table-column label="Status" width="170">
        <template #default="{ row }">
          <el-tag :type="STATUS[row.status].tag" size="small">{{
            STATUS[row.status].label
          }}</el-tag>
          <div class="seen">{{ row.seen }}</div>
        </template>
      </el-table-column>
      <el-table-column label="Queues" width="90">
        <template #default="{ row }">{{
          row.queues.length ? row.queues.join(", ") : "all"
        }}</template>
      </el-table-column>
      <el-table-column label="Load" width="130">
        <template #default="{ row }">
          <el-progress
            :percentage="row.load"
            :show-text="false"
            :stroke-width="6"
          />
          <span class="load-text"
            >{{ row.running }} / {{ row.concurrency }} running</span
          >
        </template>
      </el-table-column>
      <el-table-column label="Since start" width="130">
        <template #default="{ row }">
          <span class="ok">{{ row.succeeded }} ok</span>
          ·
          <span :class="{ bad: row.failed }">{{ row.failed }} failed</span>
        </template>
      </el-table-column>
      <el-table-column label="Started" width="160">
        <template #default="{ row }">{{ formatDate(row.started_at) }}</template>
      </el-table-column>
      <el-table-column label="Running tasks" min-width="230">
        <template #default="{ row }">
          <span v-if="row.tasks.length === 0" class="muted">—</span>
          <el-button
            v-for="task in row.tasks"
            :key="task.id"
            link
            type="primary"
            class="task-link"
            :data-testid="`worker-task-${task.id}`"
            @click="emit('select-task', task.id)"
          >
            #{{ task.id }} {{ task.kind }}
          </el-button>
        </template>
      </el-table-column>
    </el-table>
  </div>
</template>

<style scoped>
.roster-summary {
  display: flex;
  align-items: center;
  gap: 10px;
  flex-wrap: wrap;
  margin-bottom: 12px;
}

.summary-pill {
  --pill: var(--el-color-info);
  display: inline-flex;
  align-items: center;
  gap: 6px;
  padding: 4px 12px;
  border-radius: 999px;
  font-size: 13px;
  color: var(--pill);
  background: color-mix(in srgb, var(--pill) 10%, transparent);
}

.summary-pill.success {
  --pill: var(--el-color-success);
}

.summary-pill.warning {
  --pill: var(--el-color-warning);
}

.summary-pill.danger {
  --pill: var(--el-color-danger);
}

.summary-pill .dot {
  width: 7px;
  height: 7px;
  border-radius: 50%;
  background: var(--pill);
}

.inactive-toggle {
  margin-left: auto;
}

.hidden-count,
.worker-id,
.seen,
.load-text,
.muted {
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.worker-name {
  font-weight: 600;
}

.worker-id {
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
}

.ok {
  color: var(--el-color-success);
}

.bad {
  color: var(--el-color-danger);
}

.task-link {
  margin-left: 0 !important;
  margin-right: 8px;
}

:deep(.highlighted) {
  --el-table-tr-bg-color: var(--el-color-primary-light-9);
}
</style>
