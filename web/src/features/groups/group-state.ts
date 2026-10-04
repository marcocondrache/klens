import type { GroupState } from "@/lib/api/types";

export function hasMembers(state: GroupState): boolean {
  return state !== "EMPTY" && state !== "DEAD";
}
