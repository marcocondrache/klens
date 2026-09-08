import { useMemo } from "react";
import { SearchIcon } from "lucide-react";
import { useNavigate, useSearchParams } from "react-router";

import { InputGroup, InputGroupAddon, InputGroupInput } from "@/components/ui/input-group";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { DataTable, type Column } from "@/components/data-table";
import { PageHeader } from "@/components/page-header";
import { GroupStateBadge, Pill } from "@/components/status";
import { lagTone } from "@/lib/tone";
import { useConsumerGroups } from "@/lib/api/queries";
import { clusterPath, useClusterName } from "@/lib/clusters";
import { formatCount, formatEnumLabel, formatNumber } from "@/lib/format";
import type { ConsumerGroup, ConsumerGroupState } from "@/lib/api/types";

const STATES: ConsumerGroupState[] = [
  "STABLE",
  "EMPTY",
  "PREPARING_REBALANCE",
  "COMPLETING_REBALANCE",
  "DEAD",
];

export function ConsumerGroupsPage() {
  const cluster = useClusterName();
  const navigate = useNavigate();
  const [params, setParams] = useSearchParams();

  const term = params.get("q") ?? "";
  const state = params.get("state") ?? "all";

  const { data: groups = [], isPending, isError, error } = useConsumerGroups(cluster);

  function update(key: string, value: string | null) {
    const next = new URLSearchParams(params);
    if (value === null || value === "" || value === "all") {
      next.delete(key);
    } else {
      next.set(key, value);
    }
    setParams(next, { replace: true });
  }

  const rows = useMemo(() => {
    const needle = term.trim().toLowerCase();

    return groups.filter((group) => {
      if (state !== "all" && group.state !== state) return false;
      if (needle && !group.id.toLowerCase().includes(needle)) return false;
      return true;
    });
  }, [groups, term, state]);

  const totalLag = rows.reduce((sum, group) => sum + group.lag, 0);

  const columns: Array<Column<ConsumerGroup>> = [
    {
      id: "id",
      header: "Group",
      sortValue: (group) => group.id,
      cell: (group) => <span className="font-mono text-[0.8rem]">{group.id}</span>,
    },
    {
      id: "state",
      header: "State",
      sortValue: (group) => group.state,
      cell: (group) => <GroupStateBadge state={group.state} />,
    },
    {
      id: "members",
      header: "Members",
      align: "right",
      sortValue: (group) => group.members.length,
      cell: (group) => group.members.length,
    },
    {
      id: "topics",
      header: "Topics",
      sortValue: (group) => group.topics.length,
      cell: (group) => (
        <span className="flex flex-wrap gap-1">
          {group.topics.slice(0, 2).map((topic) => (
            <Pill key={topic} className="font-mono">
              {topic}
            </Pill>
          ))}
          {group.topics.length > 2 ? <Pill>+{group.topics.length - 2}</Pill> : null}
        </span>
      ),
    },
    {
      id: "partitions",
      header: "Assigned",
      align: "right",
      sortValue: (group) => group.offsets.length,
      cell: (group) => group.offsets.length,
    },
    {
      id: "lag",
      header: "Lag",
      align: "right",
      sortValue: (group) => group.lag,
      cell: (group) => (
        <Pill tone={lagTone(group.lag)} className="numeric font-mono">
          {formatNumber(group.lag)}
        </Pill>
      ),
    },
    {
      id: "coordinator",
      header: "Coordinator",
      align: "right",
      sortValue: (group) => group.coordinator,
      cell: (group) => (
        <span className="numeric font-mono text-muted-foreground">broker {group.coordinator}</span>
      ),
    },
  ];

  return (
    <div className="space-y-5">
      <PageHeader
        title="Consumer groups"
        description={`${rows.length} groups · ${formatCount(totalLag)} messages of lag`}
      />

      <div className="flex flex-wrap items-center gap-3">
        <InputGroup className="w-full max-w-sm">
          <InputGroupAddon>
            <SearchIcon />
          </InputGroupAddon>
          <InputGroupInput
            value={term}
            onChange={(event) => update("q", event.target.value)}
            placeholder="Search consumer groups…"
          />
        </InputGroup>

        <Select value={state} onValueChange={(value) => update("state", String(value))}>
          <SelectTrigger size="sm" className="w-48">
            <SelectValue placeholder="State" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All states</SelectItem>
            {STATES.map((value) => (
              <SelectItem key={value} value={value}>
                {formatEnumLabel(value)}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <DataTable
        columns={columns}
        rows={rows}
        rowKey={(group) => group.id}
        loading={isPending}
        error={
          isError
            ? error instanceof Error
              ? error.message
              : "Failed to load consumer groups."
            : undefined
        }
        defaultSort={{ id: "lag", direction: "desc" }}
        onRowClick={(group) => navigate(clusterPath(cluster, "groups", group.id))}
      />
    </div>
  );
}
