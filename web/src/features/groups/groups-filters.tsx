import { CircleDashedIcon, TimerIcon } from "lucide-react";

import type { FilterField } from "@/components/data-table/filters";
import { GROUP_TONE, StatusDot } from "@/components/status";
import { formatEnumLabel } from "@/lib/format";
import type { GroupRow } from "@/lib/api/types";

import { GROUP_STATES, type GroupFilter } from "./search";

export function groupMatches(group: GroupRow, needle: string) {
  return group.id.toLowerCase().includes(needle);
}

export const GROUP_FILTERS: Array<FilterField<GroupRow, GroupFilter>> = [
  {
    id: "state",
    label: "State",
    plural: "states",
    icon: CircleDashedIcon,
    options: GROUP_STATES.map((state) => ({
      value: state,
      label: formatEnumLabel(state),
      icon: <StatusDot tone={GROUP_TONE[state]} />,
    })),
    accessor: (group) => group.state,
  },
  {
    id: "lag",
    label: "Lag",
    plural: "states",
    icon: TimerIcon,
    options: [
      { value: "lagging", label: "Lagging", icon: <StatusDot tone="warn" /> },
      { value: "caught-up", label: "Caught up", icon: <StatusDot tone="ok" /> },
    ],
    accessor: (group) =>
      group.totalLag === null ? [] : group.totalLag > 0 ? "lagging" : "caught-up",
  },
];
