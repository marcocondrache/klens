export type Tone = "ok" | "warn" | "error" | "idle" | "brand"

export function lagTone(lag: number): Tone {
  if (lag < 10_000) return "ok"
  if (lag < 250_000) return "warn"
  return "error"
}
