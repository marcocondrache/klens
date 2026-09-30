export type Tone = "ok" | "warn" | "error" | "idle" | "brand";

export function lagTone(lag: number): Tone {
  if (lag < 10_000) return "ok";
  if (lag < 250_000) return "warn";
  return "error";
}

export function diskTone(used: number): Tone {
  if (used < 0.8) return "ok";
  if (used < 0.9) return "warn";
  return "error";
}
