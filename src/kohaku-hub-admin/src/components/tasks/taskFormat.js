// Formatting shared by the task progress, detail and log views.
import dayjs from "dayjs";

/** 45 -> "45s", 200 -> "3m 20s", 3900 -> "1h 5m"; null for no value. */
export function formatSeconds(seconds) {
  if (seconds === null || seconds === undefined) return null;
  const total = Math.round(seconds);
  if (total < 60) return `${total}s`;
  if (total < 3600) return `${Math.floor(total / 60)}m ${total % 60}s`;
  return `${Math.floor(total / 3600)}h ${Math.floor((total % 3600) / 60)}m`;
}

export function formatDate(value) {
  return value ? dayjs(value).format("YYYY-MM-DD HH:mm:ss") : "—";
}

/** Seconds between two ISO timestamps, or up to now when ``end`` is missing. */
export function secondsBetween(start, end) {
  if (!start) return null;
  return (dayjs(end || undefined).valueOf() - dayjs(start).valueOf()) / 1000;
}
