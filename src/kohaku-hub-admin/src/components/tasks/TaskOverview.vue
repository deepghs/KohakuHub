<script setup>
import { computed } from "vue";
import dayjs from "dayjs";
import TaskTimeline from "@/components/tasks/TaskTimeline.vue";

const props = defineProps({
  stats: { type: Object, required: true },
  window: { type: String, required: true },
});
const emit = defineEmits(["update:window", "select-kind", "select-status"]);

const WINDOWS = ["15m", "1h", "6h", "24h", "7d"];

const summary = computed(() => props.stats.summary);
const backlog = computed(() => props.stats.backlog);
const thresholds = computed(() => props.stats.thresholds);

function formatAge(seconds) {
  if (seconds < 60) return `${Math.floor(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  return `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
}

function formatDuration(seconds) {
  if (seconds === null || seconds === undefined) return "—";
  if (seconds < 1) return `${Math.round(seconds * 1000)} ms`;
  if (seconds < 60) return `${seconds.toFixed(1)} s`;
  return formatAge(seconds);
}

function formatPercent(rate) {
  return `${(rate * 100).toFixed(rate === 0 || rate === 1 ? 0 : 1)}%`;
}

// Colour a failure rate the same way the backend judges health.
function rateTone(failureRate) {
  if (failureRate === null || failureRate === undefined) return "muted";
  if (failureRate >= thresholds.value.failure_rate_critical) return "bad";
  if (failureRate >= thresholds.value.failure_rate_warn) return "warn";
  return "ok";
}

const successRate = computed(() =>
  summary.value.failure_rate === null ? null : 1 - summary.value.failure_rate,
);

const backlogTone = computed(() => {
  const age = backlog.value.oldest_due_seconds;
  if (age === null) return "muted";
  if (age >= thresholds.value.backlog_critical_seconds) return "bad";
  if (age >= thresholds.value.backlog_warn_seconds) return "warn";
  return "ok";
});

const tiles = computed(() => [
  {
    key: "finished",
    label: "Finished",
    value: summary.value.finished,
    hint: `${summary.value.throughput_per_minute.toFixed(2)}/min · ${summary.value.enqueued} enqueued`,
    tone: "muted",
  },
  {
    key: "success",
    label: "Success rate",
    value: successRate.value === null ? "—" : formatPercent(successRate.value),
    hint: `${summary.value.failed} failed · ${summary.value.succeeded_after_retry} recovered by retry`,
    tone: rateTone(summary.value.failure_rate),
  },
  {
    key: "backlog",
    label: "Due now",
    value: backlog.value.due,
    hint:
      backlog.value.oldest_due_seconds === null
        ? "nothing waiting"
        : `oldest waiting ${formatAge(backlog.value.oldest_due_seconds)}`,
    tone: backlogTone.value,
  },
  {
    key: "retrying",
    label: "Retrying",
    value: backlog.value.retrying,
    hint: `${backlog.value.scheduled} scheduled for later`,
    tone: backlog.value.retrying ? "warn" : "muted",
  },
  {
    key: "running",
    label: "Running",
    value: backlog.value.running,
    hint: backlog.value.stuck
      ? `${backlog.value.stuck} stuck (lease expired)`
      : `${backlog.value.active_workers} active worker(s)`,
    tone: backlog.value.stuck ? "bad" : "muted",
    status: "running",
  },
  {
    key: "duration",
    label: "Duration p50",
    value: formatDuration(summary.value.duration_p50),
    hint: `p95 ${formatDuration(summary.value.duration_p95)}`,
    tone: "muted",
  },
]);

const peak = computed(() =>
  Math.max(1, ...props.stats.series.map((b) => b.succeeded + b.failed)),
);

const activity = computed(() =>
  props.stats.series.map((bucket) => ({
    okHeight: (bucket.succeeded / peak.value) * 100,
    failHeight: (bucket.failed / peak.value) * 100,
    title:
      `${dayjs(bucket.start).format("MM-DD HH:mm")} · ${bucket.succeeded} succeeded, ` +
      `${bucket.failed} failed, ${bucket.enqueued} enqueued`,
  })),
);

const seriesStarts = computed(() => props.stats.series.map((b) => b.start));

const errorPeak = computed(() =>
  Math.max(1, ...props.stats.errors.map((e) => e.failed + e.retrying)),
);

function relative(iso) {
  const seconds = Math.max(0, dayjs().diff(dayjs(iso), "second"));
  return `${formatAge(seconds)} ago`;
}
</script>

<template>
  <div class="overview" data-testid="task-overview">
    <div class="toolbar">
      <div class="segmented" role="tablist" data-testid="task-window">
        <button
          v-for="option in WINDOWS"
          :key="option"
          type="button"
          role="tab"
          :aria-selected="window === option"
          :class="{ active: window === option }"
          :data-testid="`task-window-${option}`"
          @click="emit('update:window', option)"
        >
          {{ option }}
        </button>
      </div>
      <span class="generated">
        updated {{ dayjs(stats.generated_at).format("HH:mm:ss") }}
      </span>
    </div>

    <ul
      v-if="stats.health.reasons.length"
      class="reasons"
      data-testid="task-health-reasons"
    >
      <li
        v-for="reason in stats.health.reasons"
        :key="reason.message"
        :class="reason.level"
      >
        <span class="dot" />{{ reason.message }}
      </li>
    </ul>

    <div class="tiles">
      <component
        :is="tile.status ? 'button' : 'div'"
        v-for="tile in tiles"
        :key="tile.key"
        :type="tile.status ? 'button' : undefined"
        class="tile"
        :class="[tile.tone, { clickable: tile.status }]"
        :data-testid="`task-kpi-${tile.key}`"
        @click="tile.status && emit('select-status', tile.status)"
      >
        <span class="tile-label">{{ tile.label }}</span>
        <span class="tile-value">{{ tile.value }}</span>
        <span class="tile-hint">{{ tile.hint }}</span>
      </component>
    </div>

    <section class="panel">
      <header class="panel-header">
        <h3>Activity</h3>
        <span class="legend">
          <span class="swatch ok" />succeeded <span class="swatch bad" />failed
        </span>
      </header>
      <div class="activity" data-testid="task-activity">
        <div
          v-for="(bar, index) in activity"
          :key="index"
          class="activity-bar"
          :title="bar.title"
        >
          <span class="fail" :style="{ height: `${bar.failHeight}%` }" />
          <span class="ok" :style="{ height: `${bar.okHeight}%` }" />
        </div>
      </div>
      <div class="axis">
        <span>{{ dayjs(stats.series[0].start).format("MM-DD HH:mm") }}</span>
        <span>now</span>
      </div>
    </section>

    <div class="columns">
      <section class="panel">
        <header class="panel-header">
          <h3>Health by kind</h3>
          <span class="panel-hint">click a kind to list its tasks</span>
        </header>
        <p
          v-if="stats.kinds.length === 0"
          class="empty"
          data-testid="task-kinds-empty"
        >
          No task activity in this window.
        </p>
        <button
          v-for="kind in stats.kinds"
          :key="kind.kind"
          type="button"
          class="kind-row"
          :data-testid="`task-kind-${kind.kind}`"
          @click="emit('select-kind', kind.kind)"
        >
          <div class="kind-head">
            <span class="kind-name">{{ kind.kind }}</span>
            <span class="kind-rate" :class="rateTone(kind.failure_rate)">
              {{
                kind.failure_rate === null
                  ? "—"
                  : formatPercent(1 - kind.failure_rate)
              }}
            </span>
          </div>
          <div class="kind-meta">
            <span>{{ kind.succeeded }} ok</span>
            <span :class="{ 'text-bad': kind.failed }"
              >{{ kind.failed }} failed</span
            >
            <span>{{ kind.queued }} queued</span>
            <span>{{ kind.running }} running</span>
            <span v-if="kind.stuck" class="text-bad"
              >{{ kind.stuck }} stuck</span
            >
            <span>p95 {{ formatDuration(kind.duration_p95) }}</span>
          </div>
          <TaskTimeline :buckets="kind.timeline" :starts="seriesStarts" />
          <p v-if="kind.last_error" class="kind-error" :title="kind.last_error">
            {{ kind.last_error }}
          </p>
        </button>
      </section>

      <section class="panel">
        <header class="panel-header">
          <h3>Errors</h3>
          <span class="panel-hint">final failures and pending retries</span>
        </header>
        <p
          v-if="stats.errors.length === 0"
          class="empty"
          data-testid="task-errors-empty"
        >
          No errors in this window.
        </p>
        <div
          v-for="error in stats.errors"
          :key="error.error"
          class="error-row"
          :data-testid="`task-error-${error.error}`"
        >
          <div class="error-head">
            <span class="error-name">{{ error.error }}</span>
            <span class="error-count">
              {{ error.failed }} failed<template v-if="error.retrying">
                · {{ error.retrying }} retrying</template
              >
            </span>
          </div>
          <div class="error-bar">
            <span
              class="fail"
              :style="{ width: `${(error.failed / errorPeak) * 100}%` }"
            />
            <span
              class="retry"
              :style="{ width: `${(error.retrying / errorPeak) * 100}%` }"
            />
          </div>
          <p class="error-example" :title="error.example">
            {{ error.example }}
          </p>
          <div class="error-meta">
            <button
              v-for="kind in error.kinds"
              :key="kind"
              type="button"
              class="chip"
              @click="emit('select-kind', kind)"
            >
              {{ kind }}
            </button>
            <span class="panel-hint"
              >last seen {{ relative(error.last_seen) }}</span
            >
          </div>
        </div>
      </section>
    </div>
  </div>
</template>

<style scoped>
.overview {
  display: flex;
  flex-direction: column;
  gap: 16px;
}

.toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
}

.segmented {
  display: inline-flex;
  padding: 2px;
  border-radius: 10px;
  background: var(--el-fill-color-light);
  border: 1px solid var(--el-border-color-lighter);
}

.segmented button {
  padding: 4px 14px;
  border: 0;
  border-radius: 8px;
  background: transparent;
  color: var(--el-text-color-secondary);
  font-size: 13px;
  cursor: pointer;
}

.segmented button.active {
  background: var(--el-bg-color);
  color: var(--el-text-color-primary);
  font-weight: 600;
  box-shadow: 0 1px 2px rgb(0 0 0 / 12%);
}

.generated,
.panel-hint {
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.reasons {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.reasons li {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 8px 12px;
  border-radius: 8px;
  font-size: 13px;
}

.reasons .dot {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  flex: none;
}

.reasons .degraded {
  background: var(--el-color-warning-light-9);
  color: var(--el-color-warning-dark-2);
}

.reasons .degraded .dot {
  background: var(--el-color-warning);
}

.reasons .unhealthy {
  background: var(--el-color-danger-light-9);
  color: var(--el-color-danger-dark-2);
}

.reasons .unhealthy .dot {
  background: var(--el-color-danger);
}

.tiles {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
  gap: 12px;
}

.tile {
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  gap: 4px;
  padding: 12px 14px;
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 10px;
  background: var(--el-bg-color);
  text-align: left;
  font: inherit;
}

.tile.clickable {
  cursor: pointer;
}

.tile.clickable:hover {
  border-color: var(--el-border-color);
}

.tile-label {
  font-size: 11px;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: var(--el-text-color-secondary);
}

.tile-value {
  font-size: 24px;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
  color: var(--el-text-color-primary);
}

.tile-hint {
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.tile.ok .tile-value,
.kind-rate.ok {
  color: var(--el-color-success);
}

.tile.warn .tile-value,
.kind-rate.warn {
  color: var(--el-color-warning);
}

.tile.bad .tile-value,
.kind-rate.bad,
.text-bad {
  color: var(--el-color-danger);
}

.kind-rate.muted {
  color: var(--el-text-color-secondary);
}

.panel {
  padding: 14px 16px;
  border: 1px solid var(--el-border-color-lighter);
  border-radius: 10px;
  background: var(--el-bg-color);
  min-width: 0;
}

.panel-header {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 12px;
  margin-bottom: 12px;
}

.panel-header h3 {
  margin: 0;
  font-size: 15px;
  font-weight: 600;
}

.legend {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.swatch {
  width: 10px;
  height: 10px;
  border-radius: 2px;
  margin-left: 6px;
}

.swatch.ok,
.activity-bar .ok {
  background: var(--el-color-success);
}

.swatch.bad,
.activity-bar .fail,
.error-bar .fail {
  background: var(--el-color-danger);
}

.activity {
  display: flex;
  align-items: flex-end;
  gap: 2px;
  height: 120px;
  padding-bottom: 2px;
  border-bottom: 1px solid var(--el-border-color-lighter);
}

.activity-bar {
  flex: 1;
  min-width: 2px;
  height: 100%;
  display: flex;
  flex-direction: column;
  justify-content: flex-end;
}

.activity-bar span {
  display: block;
  width: 100%;
}

.activity-bar .ok {
  border-radius: 0 0 2px 2px;
}

.activity-bar .fail {
  border-radius: 2px 2px 0 0;
}

.axis {
  display: flex;
  justify-content: space-between;
  margin-top: 4px;
  font-size: 11px;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: var(--el-text-color-secondary);
}

.columns {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(380px, 1fr));
  gap: 16px;
}

.empty {
  margin: 12px 0;
  font-size: 13px;
  color: var(--el-text-color-secondary);
}

.kind-row {
  display: flex;
  flex-direction: column;
  gap: 6px;
  width: 100%;
  padding: 10px 8px;
  border: 0;
  border-top: 1px solid var(--el-border-color-lighter);
  background: transparent;
  text-align: left;
  font: inherit;
  color: inherit;
  cursor: pointer;
}

.kind-row:hover {
  background: var(--el-fill-color-light);
}

.kind-head,
.error-head {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 12px;
}

.kind-name,
.error-name {
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 13px;
  font-weight: 600;
}

.kind-rate {
  font-size: 18px;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
}

.kind-meta,
.error-meta {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 4px 12px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.kind-error,
.error-example {
  margin: 0;
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 12px;
  color: var(--el-color-danger);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.error-row {
  display: flex;
  flex-direction: column;
  gap: 6px;
  padding: 10px 0;
  border-top: 1px solid var(--el-border-color-lighter);
}

.error-count {
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.error-bar {
  display: flex;
  height: 6px;
  border-radius: 3px;
  background: var(--el-fill-color-light);
  overflow: hidden;
}

.error-bar .retry {
  background: var(--el-color-warning);
}

.chip {
  padding: 1px 8px;
  border: 1px solid var(--el-border-color);
  border-radius: 10px;
  background: transparent;
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 11px;
  color: var(--el-text-color-regular);
  cursor: pointer;
}

.chip:hover {
  border-color: var(--el-color-primary);
  color: var(--el-color-primary);
}
</style>
