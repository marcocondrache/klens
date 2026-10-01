import type { LaneHealth, LogDir } from "@/lib/api/types";

/** Share of the directory's volume in use, or null when the broker does not report it. */
export function usedShare(dir: LogDir): number | null {
  if (dir.totalBytes == null || dir.usableBytes == null || dir.totalBytes <= 0) return null;
  return (dir.totalBytes - dir.usableBytes) / dir.totalBytes;
}

/** The directory closest to full: the first one a broker runs out of. */
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

/** True until the log dirs lane commits once, unless that first poll already failed. */
export function isLogDirsPending(lane: LaneHealth | undefined): boolean {
  return lane == null || (lane.updatedAt == null && lane.lastError == null);
}
