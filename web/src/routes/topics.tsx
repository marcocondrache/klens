import { useMemo, type ReactNode } from "react";
import { AlertTriangleIcon, SearchIcon } from "lucide-react";
import { useNavigate, useSearchParams } from "react-router";

import { InputGroup, InputGroupAddon, InputGroupInput } from "@/components/ui/input-group";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { DataTable } from "@/components/data-table";
import { PageHeader } from "@/components/page-header";
import { Pill } from "@/components/status";
import { useTopics } from "@/lib/api/queries";
import { clusterPath, useClusterName } from "@/lib/clusters";
import {
  formatBytes,
  formatCleanupPolicy,
  formatDuration,
  formatNumber,
  formatRate,
  formatThroughput,
  isCompactCleanup,
} from "@/lib/format";
import type { Topic } from "@/lib/api/types";
import { createAppColumnHelper } from "@/lib/table";

function emptyMetric(value: number, display: ReactNode) {
  if (value === 0) {
    return <span className="text-muted-foreground">—</span>;
  }

  return display;
}

const columnHelper = createAppColumnHelper<Topic>();

const columns = columnHelper.columns([
  columnHelper.accessor("name", {
    header: "Topic",
    cell: ({ row }) => {
      const topic = row.original;

      return (
        <span className="flex items-center gap-2">
          <span className="font-mono text-sm">{topic.name}</span>
          {topic.internal ? <Pill>internal</Pill> : null}
          {topic.underReplicated ? (
            <Pill tone="warn">
              <AlertTriangleIcon className="size-3" />
              under-replicated
            </Pill>
          ) : null}
        </span>
      );
    },
  }),
  columnHelper.accessor((topic) => topic.partitions.length, {
    id: "partitions",
    header: "Parts",
    meta: { align: "right" },
    cell: ({ getValue }) => getValue(),
  }),
  columnHelper.accessor("replicationFactor", {
    id: "replication",
    header: "RF",
    meta: { align: "right" },
  }),
  columnHelper.accessor("messageCount", {
    id: "messages",
    header: "Messages",
    meta: { align: "right" },
    cell: ({ getValue }) => emptyMetric(getValue(), formatNumber(getValue())),
  }),
  columnHelper.accessor("sizeBytes", {
    id: "size",
    header: "Size",
    meta: { align: "right" },
    cell: ({ getValue }) => emptyMetric(getValue(), formatBytes(getValue())),
  }),
  columnHelper.accessor("messagesPerSec", {
    id: "rate",
    header: "Msg/s",
    meta: { align: "right" },
    cell: ({ getValue }) => emptyMetric(getValue(), formatThroughput(getValue())),
  }),
  columnHelper.accessor("bytesInPerSec", {
    id: "in",
    header: "Bytes in",
    meta: { align: "right" },
    cell: ({ getValue }) => emptyMetric(getValue(), formatRate(getValue())),
  }),
  columnHelper.accessor("retentionMs", {
    id: "retention",
    header: "Retention",
    meta: { align: "right" },
    cell: ({ getValue }) => <span>{formatDuration(getValue())}</span>,
  }),
  columnHelper.accessor("cleanupPolicy", {
    id: "policy",
    header: "Policy",
    meta: { align: "right" },
    cell: ({ getValue }) => (
      <Pill tone={isCompactCleanup(getValue()) ? "brand" : "idle"}>
        {formatCleanupPolicy(getValue())}
      </Pill>
    ),
  }),
  columnHelper.accessor((topic) => topic.consumerGroups.length, {
    id: "groups",
    header: "Groups",
    meta: { align: "right" },
    cell: ({ getValue }) => emptyMetric(getValue(), getValue()),
  }),
]);

export function TopicsPage() {
  const cluster = useClusterName();
  const navigate = useNavigate();
  const [params, setParams] = useSearchParams();

  const term = params.get("q") ?? "";
  const showInternal = params.get("internal") === "1";
  const policy = params.get("policy") ?? "all";

  const { data: topics = [], isPending, isError, error } = useTopics(cluster);

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

    return topics.filter((topic) => {
      if (!showInternal && topic.internal) return false;
      if (policy !== "all" && !formatCleanupPolicy(topic.cleanupPolicy).includes(policy))
        return false;
      if (needle && !topic.name.toLowerCase().includes(needle)) return false;
      return true;
    });
  }, [topics, term, showInternal, policy]);

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
      <PageHeader title="Topics" description={`${rows.length} of ${topics.length} topics`} />

      <div className="flex flex-wrap items-center gap-3">
        <InputGroup className="w-full max-w-sm">
          <InputGroupAddon>
            <SearchIcon />
          </InputGroupAddon>
          <InputGroupInput
            value={term}
            onChange={(event) => update("q", event.target.value)}
            placeholder="Search topics…"
          />
        </InputGroup>

        <Select value={policy} onValueChange={(value) => update("policy", String(value))}>
          <SelectTrigger size="sm" className="w-40">
            <SelectValue placeholder="Cleanup policy" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All policies</SelectItem>
            <SelectItem value="delete">delete</SelectItem>
            <SelectItem value="compact">compact</SelectItem>
          </SelectContent>
        </Select>

        <Label className="flex items-center gap-2 text-sm text-muted-foreground">
          <Switch
            size="sm"
            checked={showInternal}
            onCheckedChange={(checked) => update("internal", checked ? "1" : null)}
          />
          Show internal
        </Label>
      </div>

      <DataTable
        columns={columns}
        data={rows}
        getRowId={(topic) => topic.name}
        loading={isPending}
        error={
          isError ? (error instanceof Error ? error.message : "Failed to load topics.") : undefined
        }
        defaultSort={{ id: "name", direction: "asc" }}
        onRowClick={(topic) => navigate(clusterPath(cluster, "topics", topic.name))}
        fill
      />
    </div>
  );
}
