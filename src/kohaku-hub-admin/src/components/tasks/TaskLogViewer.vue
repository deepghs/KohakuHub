<script setup>
import { nextTick, onBeforeUnmount, onMounted, ref, watch } from "vue";
import dayjs from "dayjs";
import { downloadTaskLogs, getTaskLogs } from "@/utils/api";

// A task's captured log: loads page by page, tails new records while the
// task runs, and downloads the whole log (or one attempt's) as a file.
const props = defineProps({
  token: { type: String, required: true },
  taskId: { type: Number, required: true },
  running: { type: Boolean, default: false },
  attempts: { type: Array, default: () => [] }, // attempt numbers
});
const emit = defineEmits(["error"]);

const TAIL_INTERVAL_MS = 2000;
const PAGE_LIMIT = 1000;
const MAX_PAGES_PER_LOAD = 20;

const lines = ref([]);
const afterId = ref(0);
const attempt = ref("");
const follow = ref(true);
const loading = ref(false);
const downloading = ref(false);
const viewport = ref(null);
let timer = null;
let generation = 0;

async function load() {
  const requested = generation;
  loading.value = true;
  try {
    for (let page = 0; page < MAX_PAGES_PER_LOAD; page += 1) {
      const response = await getTaskLogs(props.token, props.taskId, {
        attempt: attempt.value === "" ? undefined : attempt.value,
        afterId: afterId.value,
        limit: PAGE_LIMIT,
      });
      // The filter changed while this page was in flight.
      if (requested !== generation) return;
      lines.value.push(...response.lines);
      afterId.value = response.next_after_id;
      if (!response.has_more) break;
    }
    if (follow.value) {
      await nextTick();
      if (viewport.value)
        viewport.value.scrollTop = viewport.value.scrollHeight;
    }
  } catch (error) {
    emit("error", error);
  } finally {
    // A superseded load must not clear the flag of the one that replaced it.
    if (requested === generation) loading.value = false;
  }
}

function tail() {
  // Overlapping loads would read the same page twice.
  if (!loading.value) load();
}

function restart() {
  generation += 1;
  lines.value = [];
  afterId.value = 0;
  load();
}

function stopTail() {
  if (timer) {
    clearInterval(timer);
    timer = null;
  }
}

function syncTail() {
  stopTail();
  if (props.running && follow.value) {
    timer = setInterval(tail, TAIL_INTERVAL_MS);
  }
}

async function download() {
  downloading.value = true;
  try {
    const selected = attempt.value === "" ? undefined : attempt.value;
    const blob = await downloadTaskLogs(props.token, props.taskId, selected);
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = `task-${props.taskId}${selected ? `-attempt-${selected}` : ""}.log`;
    link.click();
    URL.revokeObjectURL(url);
  } catch (error) {
    emit("error", error);
  } finally {
    downloading.value = false;
  }
}

function formatTime(value) {
  return dayjs(value).format("HH:mm:ss.SSS");
}

watch(attempt, restart);
watch(() => props.taskId, restart);
watch([() => props.running, follow], syncTail);

onMounted(() => {
  load();
  syncTail();
});
onBeforeUnmount(stopTail);
</script>

<template>
  <div class="log-viewer" data-testid="task-logs">
    <div class="log-toolbar">
      <el-select
        v-model="attempt"
        class="attempt-select"
        placeholder="All attempts"
        data-testid="task-logs-attempt"
      >
        <el-option label="All attempts" value="" />
        <el-option
          v-for="number in attempts"
          :key="number"
          :label="`Attempt ${number}`"
          :value="number"
        />
      </el-select>
      <el-checkbox v-model="follow" data-testid="task-logs-follow">
        Follow{{ running ? " (live)" : "" }}
      </el-checkbox>
      <span class="line-count" data-testid="task-logs-count"
        >{{ lines.length }} line(s)</span
      >
      <el-button
        size="small"
        :loading="loading"
        data-testid="task-logs-refresh"
        @click="load()"
      >
        Load new
      </el-button>
      <el-button
        size="small"
        type="primary"
        :loading="downloading"
        data-testid="task-logs-download"
        @click="download()"
      >
        Download
      </el-button>
    </div>
    <div ref="viewport" class="log-lines" data-testid="task-logs-lines">
      <div v-if="lines.length === 0" class="log-empty">
        {{ loading ? "Loading…" : "No log records captured." }}
      </div>
      <div
        v-for="line in lines"
        :key="line.id"
        class="log-line"
        :class="`level-${line.level.toLowerCase()}`"
      >
        <span class="log-time">{{ formatTime(line.at) }}</span>
        <span class="log-attempt">#{{ line.attempt }}</span>
        <span class="log-level">{{ line.level }}</span>
        <span class="log-message">{{ line.message }}</span>
      </div>
    </div>
  </div>
</template>

<style scoped>
.log-toolbar {
  display: flex;
  align-items: center;
  gap: 12px;
  margin-bottom: 8px;
  flex-wrap: wrap;
}

.attempt-select {
  width: 160px;
}

.line-count {
  margin-left: auto;
  font-size: 12px;
  color: var(--el-text-color-secondary);
}

.log-lines {
  height: 360px;
  overflow: auto;
  padding: 8px 10px;
  border-radius: 6px;
  background: #0f1419;
  color: #d6dde6;
  font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace;
  font-size: 12px;
  line-height: 1.5;
}

.log-empty {
  color: #8a97a6;
}

.log-line {
  display: flex;
  gap: 8px;
  white-space: pre-wrap;
  word-break: break-word;
}

.log-time,
.log-attempt {
  color: #8a97a6;
  flex-shrink: 0;
}

.log-level {
  width: 64px;
  flex-shrink: 0;
  font-weight: 600;
}

.level-info .log-level {
  color: #6cb6ff;
}

.level-warning .log-level {
  color: #e3b341;
}

.level-error .log-level,
.level-critical .log-level {
  color: #ff7b72;
}

.level-error .log-message,
.level-critical .log-message {
  color: #ffa198;
}
</style>
