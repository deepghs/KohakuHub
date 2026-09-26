<script setup>
import { computed } from "vue";
import { formatSeconds } from "./taskFormat.js";

// Progress of one task as reported through TaskContext: a bar when the total
// is known, a running count when it is not, plus stage and ETA.
const props = defineProps({
  task: { type: Object, required: true },
  compact: { type: Boolean, default: false },
});

const progress = computed(() => props.task.progress);

const percent = computed(() => {
  const p = progress.value;
  if (!p || p.done === null || !p.total) return null;
  return Math.min(100, Math.floor((p.done / p.total) * 100));
});

const barStatus = computed(() => {
  if (props.task.status === "succeeded") return "success";
  if (props.task.status === "failed") return "exception";
  if (props.task.stalled || props.task.status === "cancelled") return "warning";
  return "";
});

const countText = computed(() => {
  const p = progress.value;
  if (!p || p.done === null) return null;
  const done = p.done.toLocaleString();
  return p.total ? `${done} / ${p.total.toLocaleString()}` : `${done} done`;
});

const etaText = computed(() => {
  if (props.task.status !== "running") return null;
  const eta = formatSeconds(progress.value?.eta_seconds);
  return eta ? `ETA ${eta}` : null;
});
</script>

<template>
  <div
    v-if="progress"
    class="task-progress"
    :class="{ compact }"
    data-testid="task-progress"
  >
    <el-progress
      v-if="percent !== null"
      :percentage="percent"
      :status="barStatus"
      :stroke-width="compact ? 6 : 10"
      :show-text="false"
      data-testid="task-progress-bar"
    />
    <div class="progress-meta">
      <span v-if="percent !== null" class="percent">{{ percent }}%</span>
      <span v-if="countText" class="count" data-testid="task-progress-count">{{
        countText
      }}</span>
      <span v-if="etaText" class="eta" data-testid="task-progress-eta">{{
        etaText
      }}</span>
    </div>
    <div
      v-if="progress.stage"
      class="stage"
      :title="progress.stage"
      data-testid="task-progress-stage"
    >
      {{ progress.stage }}
    </div>
  </div>
  <span v-else class="no-progress">—</span>
</template>

<style scoped>
.task-progress {
  display: flex;
  flex-direction: column;
  gap: 2px;
  min-width: 0;
}

.progress-meta {
  display: flex;
  gap: 8px;
  font-size: 12px;
  color: var(--el-text-color-secondary);
  flex-wrap: wrap;
}

.percent {
  font-weight: 600;
  color: var(--el-text-color-primary);
}

.eta {
  color: var(--el-color-primary);
}

.stage {
  font-size: 12px;
  color: var(--el-text-color-regular);
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.no-progress {
  color: var(--el-text-color-placeholder);
}
</style>
