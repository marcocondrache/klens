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
import { DataTable } from "@/components/data-table";
import { PageHeader } from "@/components/page-header";
import { GroupStateBadge, Pill } from "@/components/status";
import { lagTone } from "@/lib/tone";
import { useConsumerGroups } from "@/lib/api/queries";
import { clusterPath, useClusterName } from "@/lib/clusters";
import { formatCount, formatEnumLabel, formatNumber } from "@/lib/format";
import type { ConsumerGroup, ConsumerGroupState } from "@/lib/api/types";
import { createAppColumnHelper } from "@/lib/table";

const STATES: ConsumerGroupState[] = [
  "STABLE",
  "EMPTY",
  "PREPARING_REBALANCE",
  "COMPLETING_REBALANCE",
  "DEAD",
];

const columnHelper = createAppColumnHelper<ConsumerGroup>();

const columns = columnHelper.columns([
  columnHelper.accessor("id", {
    header: "Group",
    cell: ({ getValue }) => <span className="font-mono text-sm">{getValue()}</span>,
  }),
  columnHelper.accessor("state", {
    header: "State",
    cell: ({ getValue }) => <GroupStateBadge state={getValue()} />,
  }),
  columnHelper.accessor((group) => group.members.length, {
    id: "members",
    header: "Members",
    meta: { align: "right" },
    cell: ({ getValue }) => getValue(),
  }),
  columnHelper.accessor((group) => group.topics.length, {
    id: "topics",
    header: "Topics",
    cell: ({ row }) => (
      <span className="flex flex-wrap gap-1">
        {row.original.topics.slice(0, 2).map((topic) => (
          <Pill key={topic} className="font-mono">
            {topic}
          </Pill>
        ))}
        {row.original.topics.length > 2 ? <Pill>+{row.original.topics.length - 2}</Pill> : null}
      </span>
    ),
  }),
  columnHelper.accessor((group) => group.offsets.length, {
    id: "partitions",
    header: "Assigned",
    meta: { align: "right" },
    cell: ({ getValue }) => getValue(),
  }),
  columnHelper.accessor("lag", {
    header: "Lag",
    meta: { align: "right" },
    cell: ({ getValue }) => (
      <Pill tone={lagTone(getValue())} className="numeric font-mono">
        {formatNumber(getValue())}
      </Pill>
    ),
  }),
  columnHelper.accessor("coordinator", {
    header: "Coordinator",
    meta: { align: "right" },
    cell: ({ getValue }) => <span className="numeric font-mono">broker {getValue()}</span>,
  }),
]);

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

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
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
        data={rows}
        getRowId={(group) => group.id}
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
        fill
      />
    </div>
  );
}
