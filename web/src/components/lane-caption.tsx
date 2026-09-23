import { useNow } from "@/hooks/use-now";
import type { LaneHealth } from "@/lib/api/types";
import { laneCaption } from "@/lib/clusters";

/** The ` · Updated 3s ago` suffix. It ticks on its own so the page around it does not. */
export function LaneCaption({ lane }: { lane: LaneHealth | undefined }) {
  const now = useNow();
  const caption = laneCaption(lane, now);

  return caption ? <> · {caption}</> : null;
}
