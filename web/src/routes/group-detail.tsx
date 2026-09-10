import { ActivityIcon, LayersIcon, NetworkIcon, UsersRoundIcon } from "lucide-react";
import { Link, useNavigate, useParams, useSearchParams } from "react-router";

import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Sparkline } from "@/components/charts";
import { CopyButton } from "@/components/copy-button";
import { DataTable } from "@/components/data-table";
import { PageHeader } from "@/components/page-header";
import { Stat, StatGrid } from "@/components/stat";
import { GroupStateBadge, Pill } from "@/components/status";
import { useConsumerGroup, useConsumerGroupLag, useGroupLagHistory } from "@/lib/api/queries";
import { clusterPath, useClusterName } from "@/lib/clusters";
import { formatCount, formatNumber } from "@/lib/format";
import type { ConsumerGroupMember, GroupOffset } from "@/lib/api/types";
import { createAppColumnHelper } from "@/lib/table";

const TABS = ["offsets", "members"];

const offsetColumnHelper = createAppColumnHelper<GroupOffset>();
const memberColumnHelper = createAppColumnHelper<ConsumerGroupMember>();

const memberColumns = memberColumnHelper.columns([
  memberColumnHelper.accessor("clientId", {
    header: "Client ID",
    cell: ({ getValue }) => <span className="font-mono text-sm">{getValue()}</span>,
  }),
  memberColumnHelper.display({
    id: "id",
    header: "Member ID",
    cell: ({ row }) => (
      <span className="flex items-center gap-1">
        <span className="max-w-72 truncate font-mono text-sm">{row.original.id}</span>
        <CopyButton value={row.original.id} label="Copy member ID" />
      </span>
    ),
  }),
  memberColumnHelper.accessor("host", {
    header: "Host",
    cell: ({ getValue }) => <span className="font-mono text-sm">{getValue()}</span>,
  }),
  memberColumnHelper.display({
    id: "assignments",
    header: "Assignments",
    cell: ({ row }) => (
      <span className="flex flex-wrap gap-1">
        {row.original.assignments.map((assignment) => (
          <Pill key={assignment.topic} className="font-mono">
            {assignment.topic}
            <span className="text-muted-foreground">[{assignment.partitions.length}]</span>
          </Pill>
        ))}
      </span>
    ),
  }),
  memberColumnHelper.accessor(
    (member) =>
      member.assignments.reduce((sum, assignment) => sum + assignment.partitions.length, 0),
    {
      id: "partitions",
      header: "Partitions",
      meta: { align: "right" },
      cell: ({ getValue }) => getValue(),
    },
  ),
]);

export function ConsumerGroupPage() {
  const cluster = useClusterName();
  const navigate = useNavigate();
  const { group: groupParam } = useParams<{ group: string }>();
  const groupId = decodeURIComponent(groupParam ?? "");
  const [params, setParams] = useSearchParams();

  const tab = TABS.includes(params.get("tab") ?? "") ? params.get("tab")! : "offsets";
  const { data: group, isPending, isError } = useConsumerGroup(cluster, groupId);
  const { data: lagHistory = [] } = useGroupLagHistory(cluster, groupId);
  useConsumerGroupLag(cluster, groupId);

  function selectTab(value: string) {
    const next = new URLSearchParams(params);
    if (value === "offsets") {
      next.delete("tab");
    } else {
      next.set("tab", value);
    }
    setParams(next, { replace: true });
  }

  if (isError) {
    return (
      <PageHeader
        title={groupId}
        mono
        description="This consumer group does not exist in the selected cluster."
      />
    );
  }

  const maxLag = Math.max(1, ...(group?.offsets ?? []).map((offset) => offset.lag));
  const memberLabels = new Map(
    (group?.members ?? []).map((member) => [member.id, member.clientId] as const),
  );

  const offsetColumns = offsetColumnHelper.columns([
    offsetColumnHelper.accessor("topic", {
      header: "Topic",
      cell: ({ getValue }) => (
        <Link
          to={clusterPath(cluster, "topics", getValue())}
          className="font-mono text-sm hover:text-brand hover:underline"
          onClick={(event) => event.stopPropagation()}
        >
          {getValue()}
        </Link>
      ),
    }),
    offsetColumnHelper.accessor("partition", {
      header: "Partition",
      meta: { align: "right" },
      cell: ({ getValue }) => <span className="numeric font-mono">{getValue()}</span>,
    }),
    offsetColumnHelper.accessor("currentOffset", {
      id: "current",
      header: "Committed",
      meta: { align: "right" },
      cell: ({ getValue }) => formatNumber(getValue()),
    }),
    offsetColumnHelper.accessor("endOffset", {
      id: "end",
      header: "End offset",
      meta: { align: "right" },
      cell: ({ getValue }) => formatNumber(getValue()),
    }),
    offsetColumnHelper.accessor("lag", {
      header: "Lag",
      meta: { align: "right" },
      cell: ({ getValue }) => {
        const lag = getValue();

        return (
          <span className="flex items-center justify-end gap-2">
            <span className="h-1.5 w-16 overflow-hidden rounded-full bg-muted">
              <span
                className={
                  lag === 0
                    ? "block h-full bg-emerald-500/70"
                    : lag > maxLag / 2
                      ? "block h-full bg-rose-500/70"
                      : "block h-full bg-amber-500/70"
                }
                style={{
                  width: `${Math.max(lag === 0 ? 0 : 4, (lag / maxLag) * 100)}%`,
                }}
              />
            </span>
            <span className="numeric w-16 font-mono">{formatNumber(lag)}</span>
          </span>
        );
      },
    }),
    offsetColumnHelper.accessor((offset) => offset.memberId ?? "", {
      id: "member",
      header: "Member",
      meta: { align: "right" },
      cell: ({ row }) =>
        row.original.memberId ? (
          <span className="font-mono text-sm">
            {memberLabels.get(row.original.memberId) ?? row.original.memberId}
          </span>
        ) : (
          <Pill tone="idle">unassigned</Pill>
        ),
    }),
  ]);

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
      <PageHeader
        title={
          <span className="flex items-center gap-1">
            {groupId}
            <CopyButton value={groupId} label="Copy group ID" size="icon-sm" />
          </span>
        }
        mono
        badges={
          group ? (
            <>
              <GroupStateBadge state={group.state} />
              {group.protocol ? <Pill>{group.protocol}</Pill> : null}
              <Pill>coordinator {group.coordinator}</Pill>
            </>
          ) : null
        }
        description={
          group
            ? `${group.topics.length} topics · ${group.offsets.length} assigned partitions`
            : null
        }
      />

      <StatGrid>
        <Stat
          label="Total lag"
          value={formatCount(group?.lag ?? 0)}
          icon={<ActivityIcon />}
          loading={isPending}
          accent
        >
          <Sparkline data={lagHistory} />
        </Stat>
        <Stat
          label="Members"
          value={group?.members.length ?? 0}
          hint={group?.state === "EMPTY" ? "no active consumers" : "active consumers"}
          icon={<UsersRoundIcon />}
          loading={isPending}
        />
        <Stat
          label="Topics"
          value={group?.topics.length ?? 0}
          icon={<LayersIcon />}
          loading={isPending}
        />
        <Stat
          label="Partitions"
          value={group?.offsets.length ?? 0}
          hint="with committed offsets"
          icon={<NetworkIcon />}
          loading={isPending}
        />
      </StatGrid>

      <Tabs
        value={tab}
        onValueChange={(value) => selectTab(String(value))}
        className="min-h-0 flex-1"
      >
        <TabsList variant="line" className="shrink-0">
          <TabsTrigger value="offsets">
            Offsets
            <span className="numeric ml-1.5 text-muted-foreground">
              {group?.offsets.length ?? 0}
            </span>
          </TabsTrigger>
          <TabsTrigger value="members">
            Members
            <span className="numeric ml-1.5 text-muted-foreground">
              {group?.members.length ?? 0}
            </span>
          </TabsTrigger>
        </TabsList>

        <TabsContent value="offsets" className="mt-4 flex min-h-0 flex-col">
          <DataTable
            columns={offsetColumns}
            data={group?.offsets ?? []}
            getRowId={(offset) => `${offset.topic}-${offset.partition}`}
            loading={isPending}
            pageSize={25}
            defaultSort={{ id: "lag", direction: "desc" }}
            onRowClick={(offset) => navigate(clusterPath(cluster, "topics", offset.topic))}
            fill
          />
        </TabsContent>

        <TabsContent value="members" className="mt-4 flex min-h-0 flex-col">
          <DataTable
            columns={memberColumns}
            data={group?.members ?? []}
            getRowId={(member) => member.id}
            loading={isPending}
            emptyState={
              <p className="py-10 text-center text-sm text-muted-foreground">
                This group has no active members.
              </p>
            }
            fill
          />
        </TabsContent>
      </Tabs>
    </div>
  );
}
