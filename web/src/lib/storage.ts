import type { LaneHealth, LogDir } from "@/lib/api/types";

export function usedShare(dir: LogDir): number | null {
  if (dir.totalBytes == null || dir.usableBytes == null || dir.totalBytes <= 0) return null;
  return (dir.totalBytes - dir.usableBytes) / dir.totalBytes;
}

export function fullestDir(dirs: LogDir[]): { dir: LogDir; used: number } | null {
  let fullest: { dir: LogDir; used: number } | null = null;
  for (const dir of dirs) {
    const used = usedShare(dir);
    if (used != null && (fullest == null || used > fullest.used)) {
      fullest = { dir, used };
    }
  }
  return fullest;
}

export function isLogDirsPending(lane: LaneHealth | undefined): boolean {
  return lane == null || (lane.updatedAt == null && lane.lastError == null);
}
