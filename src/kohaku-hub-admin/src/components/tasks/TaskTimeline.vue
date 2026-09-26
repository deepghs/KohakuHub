<script setup>
import { computed } from "vue";
import dayjs from "dayjs";

// One bar per time bucket, coloured by how that bucket went:
// green = all succeeded, amber = some failed, red = mostly failed, grey = idle.
const props = defineProps({
  buckets: { type: Array, required: true }, // [{ succeeded, failed }]
  starts: { type: Array, default: () => [] }, // ISO start of each bucket
});

function level(bucket) {
  const total = bucket.succeeded + bucket.failed;
  if (total === 0) return "idle";
  if (bucket.failed === 0) return "ok";
  return bucket.failed / total < 0.5 ? "warn" : "bad";
}

const bars = computed(() =>
  props.buckets.map((bucket, index) => {
    const at = props.starts[index]
      ? `${dayjs(props.starts[index]).format("MM-DD HH:mm")} · `
      : "";
    return {
      level: level(bucket),
      title: `${at}${bucket.succeeded} succeeded, ${bucket.failed} failed`,
    };
  }),
);
</script>

<template>
  <div class="timeline" data-testid="task-timeline">
    <span
      v-for="(bar, index) in bars"
      :key="index"
      class="bar"
      :class="bar.level"
      :title="bar.title"
      :data-level="bar.level"
    />
  </div>
</template>

<style scoped>
.timeline {
  display: flex;
  align-items: stretch;
  gap: 2px;
  height: 20px;
  width: 100%;
}

.bar {
  flex: 1;
  min-width: 2px;
  border-radius: 2px;
}

.bar.idle {
  background: var(--el-fill-color-darker);
  opacity: 0.6;
}

.bar.ok {
  background: var(--el-color-success);
}

.bar.warn {
  background: var(--el-color-warning);
}

.bar.bad {
  background: var(--el-color-danger);
}
</style>
