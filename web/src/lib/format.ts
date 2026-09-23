import type { CleanupPolicy } from "@/lib/api/types";

const BYTE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"];
const COUNT_UNITS = ["", "K", "M", "B", "T"];

export type Int64 = string | number;

export function toNumber(value: Int64): number {
  return typeof value === "number" ? value : Number(value);
}

export function formatEnumLabel(value: string) {
  return value
    .split("_")
    .map((part) => part.charAt(0) + part.slice(1).toLowerCase())
    .join(" ");
}

export function formatCleanupPolicy(policy: CleanupPolicy) {
  if (policy === "COMPACT_DELETE") return "compact,delete";
  return policy.toLowerCase();
}

export function isCompactCleanup(policy: CleanupPolicy) {
  return policy === "COMPACT" || policy === "COMPACT_DELETE";
}

export function formatBytes(value: Int64, digits = 1) {
  const bytes = toNumber(value);
  if (bytes === 0) return "0 B";

  const exponent = Math.min(Math.floor(Math.log10(Math.abs(bytes)) / 3), BYTE_UNITS.length - 1);
  const scaled = bytes / 1000 ** exponent;

  return `${scaled.toFixed(exponent === 0 ? 0 : digits)} ${BYTE_UNITS[exponent]}`;
}

export function formatCount(value: Int64, digits = 1) {
  const count = toNumber(value);
  if (Math.abs(count) < 1000) return String(count);

  const exponent = Math.min(Math.floor(Math.log10(Math.abs(count)) / 3), COUNT_UNITS.length - 1);
  const scaled = count / 1000 ** exponent;

  return `${scaled.toFixed(scaled >= 100 ? 0 : digits)}${COUNT_UNITS[exponent]}`;
}

export function formatThroughput(value: number, digits = 1) {
  if (value <= 0) return "0";
  if (value < 10) return value.toFixed(digits);
  if (value < 1000) return String(Math.round(value));
  return formatCount(value, digits);
}

export function formatNumber(value: Int64) {
  if (typeof value === "number") {
    return value.toLocaleString("en-US");
  }

  const sign = value.startsWith("-") ? "-" : "";
  const digits = sign ? value.slice(1) : value;
  if (!/^\d+$/.test(digits)) {
    return value;
  }

  return sign + digits.replace(/\B(?=(\d{3})+(?!\d))/g, ",");
}

export function isZero(value: Int64) {
  return typeof value === "number" ? value === 0 : /^-?0+$/.test(value);
}

export function formatDuration(value: Int64) {
  const ms = toNumber(value);
  if (ms < 0) return "infinite";
  if (ms === 0) return "0";

  const units: Array<[number, string]> = [
    [86_400_000, "d"],
    [3_600_000, "h"],
    [60_000, "m"],
    [1_000, "s"],
  ];

  for (const [size, suffix] of units) {
    if (ms >= size) {
      const value = ms / size;
      return `${Number.isInteger(value) ? value : value.toFixed(1)}${suffix}`;
    }
  }

  return `${ms}ms`;
}

export function toDatetimeLocalValue(date: Date) {
  const pad = (part: number) => String(part).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

export function fromDatetimeLocalValue(value: string) {
  if (!value) return null;
  const date = new Date(value);
  return Number.isFinite(date.getTime()) ? date.toISOString() : null;
}

export function formatTimestamp(value: string | number) {
  return new Date(value).toLocaleString("en-GB", {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });
}

export function formatRelative(value: string | number, now = Date.now()) {
  const delta = now - new Date(value).getTime();
  const absolute = Math.abs(delta);
  const suffix = delta >= 0 ? "ago" : "from now";

  const units: Array<[number, string]> = [
    [86_400_000, "d"],
    [3_600_000, "h"],
    [60_000, "m"],
    [1_000, "s"],
  ];

  for (const [size, unit] of units) {
    if (absolute >= size) {
      return `${Math.floor(absolute / size)}${unit} ${suffix}`;
    }
  }

  return "just now";
}

export function prettyJson(raw: string) {
  try {
    return JSON.stringify(JSON.parse(raw), null, 2);
  } catch {
    return raw;
  }
}

export function isJson(raw: string) {
  try {
    JSON.parse(raw);
    return true;
  } catch {
    return false;
  }
}
