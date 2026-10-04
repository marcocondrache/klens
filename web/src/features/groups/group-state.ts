import type { GroupDetail, GroupState } from "@/lib/api/types";

export function hasMembers(state: GroupState): boolean {
  return state !== "EMPTY" && state !== "DEAD";
}

export function committedTopics(group: GroupDetail): string[] {
  return [
    ...new Set(
      group.offsets.flatMap((offset) => (offset.currentOffset === null ? [] : [offset.topic])),
    ),
  ].sort();
}
