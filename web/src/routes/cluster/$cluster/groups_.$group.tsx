import { Link, createFileRoute } from "@tanstack/react-router";
import { createColumnHelper } from "@tanstack/react-table";

import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { CopyButton } from "@/components/copy-button";
import { DataTableColumnHeader } from "@/components/data-table/column-header";
import { DataTable } from "@/components/data-table/data-table";
import { type DataTableFeatures } from "@/components/data-table/features";
import { PageHeader } from "@/components/page-header";
import { GroupStateBadge, Pill } from "@/components/status";
import { useGroup } from "@/lib/api/catalog";
import { catalogLookupMessage } from "@/lib/catalog-lookup";
import { useClusterName } from "@/lib/clusters";
import { formatCount, formatNumber, toNumber } from "@/lib/format";
import type { GroupDetail, GroupMember, GroupOffset } from "@/lib/api/types";
import { parseGroupDetailSearch } from "@/lib/route-search";

export const Route = createFileRoute("/cluster/$cluster/groups_/$group")({
  validateSearch: parseGroupDetailSearch,
  component: ConsumerGroupPage,
});

const offsetColumnHelper = createColumnHelper<DataTableFeatures, GroupOffset>();
const memberColumnHelper = createColumnHelper<DataTableFeatures, GroupMember>();

const memberColumns = memberColumnHelper.columns([
  memberColumnHelper.accessor("clientId", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Client ID" />,
    meta: { label: "Client ID" },
    cell: ({ getValue }) => <span className="font-mono text-sm">{getValue()}</span>,
  }),
  memberColumnHelper.accessor("id", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Member ID" />,
    meta: { label: "Member ID" },
    cell: ({ row }) => (
      <span className="flex items-center gap-1">
        <span className="max-w-72 truncate font-mono text-sm">{row.original.id}</span>
        <CopyButton value={row.original.id} label="Copy member ID" />
      </span>
    ),
  }),
  memberColumnHelper.accessor("host", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Host" />,
    meta: { label: "Host" },
    cell: ({ getValue }) => <span className="font-mono text-sm">{getValue()}</span>,
  }),
  memberColumnHelper.accessor("assignments", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Assignments" />,
    meta: { label: "Assignments" },
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
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="Partitions" className="justify-end" />
      ),
      meta: { align: "right", label: "Partitions" },
      cell: ({ getValue }) => getValue(),
    },
  ),
]);

function GroupFacts({
  group,
  members,
  topicCount,
  partitions,
}: {
  group: GroupDetail;
  members: number;
  topicCount: number;
  partitions: number;
}) {
  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
      <span className="numeric">{members} members</span>
      <span className="numeric">{topicCount} topics</span>
      <span className="numeric">{partitions} assigned partitions</span>
      <span className="numeric text-brand">
        {group.lagComplete ? "" : "≥ "}
        {formatCount(group.totalLag)} lag
      </span>
    </div>
  );
}

function ConsumerGroupPage() {
  const cluster = useClusterName();
  const navigate = Route.useNavigate();
  const { group: groupId } = Route.useParams();
  const { tab: tabParam } = Route.useSearch();
  const tab = tabParam ?? "offsets";
  const { data: group, isPending, isError, error } = useGroup(cluster, groupId);

  function selectTab(value: string) {
    void navigate({
      to: ".",
      search: (prev) => {
        const next = { ...prev };
        if (value === "members") {
          next.tab = "members";
        } else {
          delete next.tab;
        }
        return next;
      },
      replace: true,
      resetScroll: false,
    });
  }

  const lookup = catalogLookupMessage({
    isPending,
    isError,
    error,
    data: group,
    missing: "This consumer group does not exist in the selected cluster.",
    failed: "Failed to load this consumer group.",
  });
  if (lookup) {
    return <PageHeader title={groupId} mono description={lookup} />;
  }

  const offsets = group?.offsets ?? [];
  const members = group?.members ?? [];
  const maxLag = Math.max(1, ...offsets.map((offset) => toNumber(offset.lag)));
  const memberLabels = new Map(members.map((member) => [member.id, member.clientId] as const));
  const topicCount = new Set([
    ...offsets.map((offset) => offset.topic),
    ...members.flatMap((member) => member.assignments.map((assignment) => assignment.topic)),
  ]).size;

  const offsetColumns = offsetColumnHelper.columns([
    offsetColumnHelper.accessor("topic", {
      header: ({ column }) => <DataTableColumnHeader column={column} title="Topic" />,
      meta: { label: "Topic" },
      cell: ({ getValue }) => (
        <Link
          to="/cluster/$cluster/topics/$topic"
          params={{ cluster, topic: getValue() }}
          className="font-mono text-sm hover:text-brand hover:underline"
          onClick={(event) => event.stopPropagation()}
        >
          {getValue()}
        </Link>
      ),
    }),
    offsetColumnHelper.accessor("partition", {
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="Partition" className="justify-end" />
      ),
      meta: { align: "right", label: "Partition" },
      cell: ({ getValue }) => <span className="numeric font-mono">{getValue()}</span>,
    }),
    offsetColumnHelper.accessor((offset) => toNumber(offset.currentOffset), {
      id: "current",
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="Committed" className="justify-end" />
      ),
      meta: { align: "right", label: "Committed" },
      cell: ({ row }) => formatNumber(row.original.currentOffset),
    }),
    offsetColumnHelper.accessor((offset) => toNumber(offset.endOffset), {
      id: "end",
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="End offset" className="justify-end" />
      ),
      meta: { align: "right", label: "End offset" },
      cell: ({ row }) => formatNumber(row.original.endOffset),
    }),
    offsetColumnHelper.accessor((offset) => toNumber(offset.lag), {
      id: "lag",
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="Lag" className="justify-end" />
      ),
      meta: { align: "right", label: "Lag" },
      cell: ({ getValue, row }) => {
        const lag = getValue();

        return (
          <span className="flex items-center justify-end gap-2">
            <span className="h-1.5 w-16 overflow-hidden rounded-full bg-muted">
              <span
                className={
                  lag === 0
                    ? "block h-full bg-ok/70"
                    : lag > maxLag / 2
                      ? "block h-full bg-destructive/70"
                      : "block h-full bg-warn/70"
                }
                style={{
                  width: `${Math.max(lag === 0 ? 0 : 4, (lag / maxLag) * 100)}%`,
                }}
              />
            </span>
            <span className="numeric w-16 font-mono">{formatNumber(row.original.lag)}</span>
          </span>
        );
      },
    }),
    offsetColumnHelper.accessor((offset) => offset.memberId ?? "", {
      id: "member",
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="Member" className="justify-end" />
      ),
      meta: { align: "right", label: "Member" },
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
              <Pill>coordinator {group.coordinatorId}</Pill>
            </>
          ) : null
        }
        description={
          group ? (
            <GroupFacts
              group={group}
              members={members.length}
              topicCount={topicCount}
              partitions={offsets.length}
            />
          ) : null
        }
      />

      <Tabs
        value={tab}
        onValueChange={(value) => selectTab(String(value))}
        className="min-h-0 flex-1"
      >
        <TabsList variant="line" className="shrink-0">
          <TabsTrigger value="offsets">
            Offsets
            <span className="numeric ml-1.5 text-muted-foreground">{offsets.length}</span>
          </TabsTrigger>
          <TabsTrigger value="members">
            Members
            <span className="numeric ml-1.5 text-muted-foreground">{members.length}</span>
          </TabsTrigger>
        </TabsList>

        <TabsContent value="offsets" className="mt-4 flex min-h-0 flex-col">
          <DataTable
            columns={offsetColumns}
            data={offsets}
            getRowId={(offset) => `${offset.topic}-${offset.partition}`}
            loading={isPending}
            defaultSort={{ id: "lag", direction: "desc" }}
            onRowClick={(offset) => {
              void navigate({
                to: "/cluster/$cluster/topics/$topic",
                params: { cluster, topic: offset.topic },
              });
            }}
            fill
          />
        </TabsContent>

        <TabsContent value="members" className="mt-4 flex min-h-0 flex-col">
          <DataTable
            columns={memberColumns}
            data={members}
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
