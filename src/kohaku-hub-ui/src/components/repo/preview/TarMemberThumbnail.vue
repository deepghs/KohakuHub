<!--
  TarMemberThumbnail.vue

  Lazy thumbnail for a single image member inside an indexed-tar
  listing. Shows the placeholder icon until the row scrolls into
  view, then drives the strategy chain in `utils/tar-thumbnail`
  (EXIF probe → small/medium full read → canvas resize) to render
  a ~128 px JPEG thumbnail in place of the icon. Failures stay on
  the placeholder silently — never a thrown render or a toast.

  Stable wrapper: the same <div> is always mounted. The IO target
  doesn't change when state flips, so the observer keeps tracking
  the row through every transition.
-->

<script setup>
import { ref, computed } from "vue";
import { useTarThumbnail, useThumbnailToggle } from "@/utils/tar-thumbnail";

const props = defineProps({
  // Resolve URL for the .tar file (member.offset is taken from the
  // sidecar entry, not the URL).
  tarUrl: { type: String, required: true },
  // Tree-builder node — we only need .name, .offset, .size, .path.
  member: { type: Object, required: true },
  // The Carbon icon class the listing already uses for this entry,
  // shown verbatim while the thumbnail is idle / loading / fallback.
  placeholderIcon: { type: String, required: true },
  // Pixel size of the rendered thumbnail container (square).
  size: { type: Number, default: 24 },
});

const rootRef = ref(null);
const { enabled } = useThumbnailToggle();

// useTarThumbnail manages IntersectionObserver, AbortController,
// pool slot, cache lookup, and state transitions. We only consume
// its reactive output.
const { state, thumbUrl } = useTarThumbnail({
  tarUrl: props.tarUrl,
  member: props.member,
  rootRef,
});

const containerStyle = computed(() => ({
  width: `${props.size}px`,
  height: `${props.size}px`,
}));

// Placeholder is rendered at icon-font-size scale to match the
// existing listing icons. Use the same Carbon mask that the panel
// computed for this entry — this way the thumbnailed and
// non-thumbnailed rows look identical when no preview is loaded.
const showThumbnail = computed(
  () => enabled.value && state.value === "ready" && !!thumbUrl.value,
);
</script>

<template>
  <div
    ref="rootRef"
    class="flex-shrink-0 inline-flex items-center justify-center overflow-hidden rounded bg-gray-100 dark:bg-gray-800"
    :style="containerStyle"
  >
    <img
      v-if="showThumbnail"
      :src="thumbUrl"
      :alt="member.name"
      class="w-full h-full object-cover"
      loading="lazy"
      decoding="async"
      draggable="false"
    />
    <div
      v-else
      :class="placeholderIcon"
      class="text-xl"
    />
  </div>
</template>
