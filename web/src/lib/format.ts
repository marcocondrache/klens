const BYTE_UNITS = ["B", "KB", "MB", "GB", "TB", "PB"]
const COUNT_UNITS = ["", "K", "M", "B", "T"]

export function formatBytes(value: number, digits = 1) {
  if (value === 0) return "0 B"

  const exponent = Math.min(Math.floor(Math.log10(Math.abs(value)) / 3), BYTE_UNITS.length - 1)
  const scaled = value / 1000 ** exponent

  return `${scaled.toFixed(exponent === 0 ? 0 : digits)} ${BYTE_UNITS[exponent]}`
}

export function formatRate(bytesPerSecond: number) {
  return `${formatBytes(bytesPerSecond)}/s`
}

export function formatCount(value: number, digits = 1) {
  if (Math.abs(value) < 1000) return String(value)

  const exponent = Math.min(Math.floor(Math.log10(Math.abs(value)) / 3), COUNT_UNITS.length - 1)
  const scaled = value / 1000 ** exponent

  return `${scaled.toFixed(scaled >= 100 ? 0 : digits)}${COUNT_UNITS[exponent]}`
}

export function formatNumber(value: number) {
  return value.toLocaleString("en-US")
}

export function formatDuration(ms: number) {
  if (ms < 0) return "infinite"
  if (ms === 0) return "0"

  const units: Array<[number, string]> = [
    [86_400_000, "d"],
    [3_600_000, "h"],
    [60_000, "m"],
    [1_000, "s"],
  ]

  for (const [size, suffix] of units) {
    if (ms >= size) {
      const value = ms / size
      return `${Number.isInteger(value) ? value : value.toFixed(1)}${suffix}`
    }
  }

  return `${ms}ms`
}

export function formatTimestamp(ms: number) {
  return new Date(ms).toLocaleString("en-GB", {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  })
}

export function formatTime(ms: number) {
  return new Date(ms).toLocaleTimeString("en-GB", {
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  })
}

export function formatRelative(ms: number) {
  const delta = Date.now() - ms
  const absolute = Math.abs(delta)
  const suffix = delta >= 0 ? "ago" : "from now"

  const units: Array<[number, string]> = [
    [86_400_000, "d"],
    [3_600_000, "h"],
    [60_000, "m"],
    [1_000, "s"],
  ]

  for (const [size, unit] of units) {
    if (absolute >= size) {
      return `${Math.floor(absolute / size)}${unit} ${suffix}`
    }
  }

  return "just now"
}

export function formatPercent(value: number, digits = 1) {
  return `${(value * 100).toFixed(digits)}%`
}

export function prettyJson(raw: string) {
  try {
    return JSON.stringify(JSON.parse(raw), null, 2)
  } catch {
    return raw
  }
}

export function isJson(raw: string) {
  try {
    JSON.parse(raw)
    return true
  } catch {
    return false
  }
}
