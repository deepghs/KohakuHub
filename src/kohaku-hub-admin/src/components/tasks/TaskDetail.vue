<script setup>
import { computed, ref } from "vue";
import TaskLogViewer from "@/components/tasks/TaskLogViewer.vue";
import TaskProgress from "@/components/tasks/TaskProgress.vue";
import { formatDate, formatSeconds, secondsBetween } from "./taskFormat.js";

// Everything about one task: its state and progress, the timeline of every
// lifecycle event, one row per run of an attempt, and the captured log.
const props = defineProps({
  task: { type: Object, required: true },
  token: { type: String, required: true },
});
const emit = defineEmits(["error"]);

const tab = ref("overview");

const EVENT_TONES = {
  created: "muted",
  claimed: "primary",
  stage: "primary",
  succeeded: "success",
  retry_scheduled: "warning",
  failed: "danger",
  released: "muted",
  lease_expired: "warning",
  cancel_requested: "warning",
  cancelled: "warning",
  retried: "muted",
};

function describeEvent(event) {
  const worker = event.worker ? ` on ${event.worker}` : "";
  switch (event.type) {
    case "created":
      return "Created";
    case "claimed":
      return `Attempt ${event.attempt} started${worker}`;
    case "stage":
      return `Stage: ${event.detail.stage}`;
    case "succeeded":
      return `Attempt ${event.attempt} succeeded`;
    case "retry_scheduled":
      return `Attempt ${event.attempt} failed; retry at ${formatDate(event.detail.run_after)}`;
    case "failed":
      return `Attempt ${event.attempt} failed; no retries left`;
    case "released":
      return `Attempt ${event.attempt} handed back (worker shutting down)`;
    case "lease_expired":
      return `Lease of attempt ${event.attempt} expired${worker}; reclaimed`;
    case "cancel_requested":
      return "Cancellation requested";
    case "cancelled":
      return event.detail.by === "admin"
        ? "Cancelled while queued"
        : `Attempt ${event.attempt} stopped by cancellation`;
    case "retried":
      return "Requeued by an admin";
    default:
      return event.type;
  }
}

const events = computed(() =>
  props.task.events.map((event) => ({
    ...event,
    tone: EVENT_TONES[event.type] || "muted",
    text: describeEvent(event),
    error: event.detail.error,
  })),
);

const runs = computed(() =>
  props.task.runs.map((attempt) => ({
    ...attempt,
    duration: formatSeconds(
      secondsBetween(attempt.started_at, attempt.finished_at),
    ),
  })),
);

const attemptNumbers = computed(() => [
  ...new Set(props.task.runs.map((run) => run.attempt)),
]);

const OUTCOME_TAG = {
  running: "primary",
  succeeded: "success",
  failed: "danger",
  cancelled: "warning",
  released: "info",
  "lease expired": "warning",
};
</script>

<template>
  <div data-testid="tasks-detail">
    <el-tabs v-model="tab">
      <el-tab-pane label="Overview" name="overview">
        <div v-if="task.progress" class="progress-block">
          <TaskProgress :task="task" />
        </div>
        <dl class="detail-grid">
          <dt>Kind</dt>
          <dd>{{ task.kind }}</dd>
          <dt>Status</dt>
          <dd>
            {{ task.status }}
            <el-tag
              v-if="task.lease_expired"
              type="warning"
              size="small"
              class="ml-1"
              data-testid="tasks-detail-lease-expired"
            >
              lease expired
            </el-tag>
            <el-tag
              v-if="task.stalled"
              type="danger"
              size="small"
              class="ml-1"
              data-testid="tasks-detail-stalled"
            >
              stalled
            </el-tag>
            <el-tag
              v-if="task.cancel_requested && task.status === 'running'"
              type="warning"
              size="small"
              class="ml-1"
              data-testid="tasks-detail-cancelling"
            >
              cancelling
            </el-tag>
          </dd>
          <dt>Queue</dt>
          <dd>{{ task.queue }}</dd>
          <dt>Priority</dt>
          <dd>{{ task.priority }}</dd>
          <dt>Attempts</dt>
          <dd>{{ task.attempts }} / {{ task.max_attempts }}</dd>
          <dt>Dedupe key</dt>
          <dd>{{ task.dedupe_key || "—" }}</dd>
          <dt>Worker</dt>
          <dd>{{ task.locked_by || "—" }}</dd>
          <dt>Lease until</dt>
          <dd>{{ formatDate(task.locked_until) }}</dd>
          <dt>Stall after</dt>
          <dd>
            {{
              task.stall_seconds ? formatSeconds(task.stall_seconds) : "never"
            }}
          </dd>
          <dt>Created</dt>
          <dd>{{ formatDate(task.created_at) }}</dd>
          <dt>Run after</dt>
          <dd>{{ formatDate(task.run_after) }}</dd>
          <dt>Started</dt>
          <dd>{{ formatDate(task.started_at) }}</dd>
          <dt>Finished</dt>
          <dd>{{ formatDate(task.finished_at) }}</dd>
        </dl>
        <h3 class="section-title">Payload</h3>
        <pre class="code-block">{{
          JSON.stringify(task.payload, null, 2)
        }}</pre>
        <template v-if="task.checkpoint !== null">
          <h3 class="section-title">Checkpoint</h3>
          <pre class="code-block" data-testid="tasks-detail-checkpoint">{{
            JSON.stringify(task.checkpoint, null, 2)
          }}</pre>
        </template>
        <template v-if="task.last_error">
          <h3 class="section-title">Last error</h3>
          <pre class="code-block error-text">{{ task.last_error }}</pre>
        </template>
      </el-tab-pane>

      <el-tab-pane :label="`Timeline (${events.length})`" name="timeline">
        <ol class="timeline" data-testid="tasks-detail-timeline">
          <li
            v-for="event in events"
            :key="event.id"
            class="timeline-item"
            :class="event.tone"
            :data-event="event.type"
          >
            <span class="timeline-dot" />
            <div class="timeline-body">
              <div class="timeline-text">{{ event.text }}</div>
              <div class="timeline-time">{{ formatDate(event.at) }}</div>
              <pre v-if="event.error" class="timeline-error">{{
                event.error
              }}</pre>
            </div>
          </li>
        </ol>
      </el-tab-pane>

      <el-tab-pane :label="`Attempts (${runs.length})`" name="attempts">
        <el-empty
          v-if="runs.length === 0"
          description="Not started yet"
          data-testid="tasks-detail-no-attempts"
        />
        <el-table v-else :data="runs" data-testid="tasks-detail-attempts">
          <el-table-column prop="attempt" label="#" width="50" />
          <el-table-column label="Outcome" width="130">
            <template #default="{ row }">
              <el-tag :type="OUTCOME_TAG[row.outcome]" size="small">{{
                row.outcome
              }}</el-tag>
            </template>
          </el-table-column>
          <el-table-column label="Started" width="170">
            <template #default="{ row }">{{
              formatDate(row.started_at)
            }}</template>
          </el-table-column>
          <el-table-column label="Duration" width="100">
            <template #default="{ row }">{{ row.duration }}</template>
          </el-table-column>
          <el-table-column prop="worker" label="Worker" min-width="160" />
          <el-table-column label="Logs" width="70">
            <template #default="{ row }">{{ row.log_lines }}</template>
          </el-table-column>
          <el-table-column label="Error / stages" min-width="200">
            <template #default="{ row }">
              <span v-if="row.error" class="error-text">{{ row.error }}</span>
              <span v-else>{{
                row.stages.map((stage) => stage.stage).join(" → ") || "—"
              }}</span>
            </template>
          </el-table-column>
        </el-table>
      </el-tab-pane>

      <el-tab-pane :label="`Logs (${task.log_lines})`" name="logs" lazy>
        <TaskLogViewer
          :token="token"
          :task-id="task.id"
          :running="task.status === 'running'"
          :attempts="attemptNumbers"
          @error="emit('error', $event)"
        />
      </el-tab-pane>
    </el-tabs>
  </div>
</template>

<style scoped>
.progress-block {
  margin-bottom: 16px;
  padding: 12px;
  border-radius: 8px;
  background: var(--el-fill-color-light);
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

.error-text {
  color: var(--el-color-danger);
}

.timeline {
  list-style: none;
  margin: 0;
  padding: 0 0 0 4px;
  max-height: 420px;
  overflow: auto;
}

.timeline-item {
  --tone: var(--el-color-info);
  position: relative;
  display: flex;
  gap: 12px;
  padding: 0 0 14px 0;
}

.timeline-item:not(:last-child)::before {
  content: "";
  position: absolute;
  left: 5px;
  top: 14px;
  bottom: 0;
  width: 2px;
  background: var(--el-border-color-lighter);
}

.timeline-item.primary {
  --tone: var(--el-color-primary);
}

.timeline-item.success {
  --tone: var(--el-color-success);
}

.timeline-item.warning {
  --tone: var(--el-color-warning);
}

.timeline-item.danger {
  --tone: var(--el-color-danger);
}

.timeline-dot {
  flex-shrink: 0;
  width: 12px;
  height: 12px;
  margin-top: 3px;
  border-radius: 50%;
  background: var(--tone);
}

.timeline-text {
  font-weight: 500;
}

.timeline-time {
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.timeline-error {
  margin: 4px 0 0;
  font-size: 12px;
  white-space: pre-wrap;
  color: var(--el-color-danger);
}
</style>
