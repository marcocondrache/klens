import { Link, createFileRoute, stripSearchParams } from "@tanstack/react-router";
import { createColumnHelper } from "@tanstack/react-table";

import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { CopyButton } from "@/components/copy-button";
import { DataTableColumnHeader } from "@/components/data-table/column-header";
import { DataTable } from "@/components/data-table/data-table";
import { type DataTableFeatures } from "@/components/data-table/features";
import { PageHeader } from "@/components/page-header";
import { Facts } from "@/components/facts";
import { TabCount } from "@/components/tab-count";
import { GroupStateBadge, Pill, TONE_TEXT } from "@/components/status";
import { lagTone } from "@/lib/tone";
import { cn } from "@/lib/utils";
import { useGroup } from "@/lib/api/catalog";
import { catalogLookupMessage } from "@/lib/catalog-lookup";
import { useClusterName } from "@/lib/clusters";
import { formatCount, formatNumber, toNumber } from "@/lib/format";
import type { GroupDetail, GroupMember, GroupOffset } from "@/lib/api/types";
import { groupDetailSearch, groupTab, searchDefaults } from "@/lib/route-search";

export const Route = createFileRoute("/cluster/$cluster/groups_/$group")({
  validateSearch: groupDetailSearch,
  search: { middlewares: [stripSearchParams(searchDefaults(groupDetailSearch))] },
  component: ConsumerGroupPage,
});

const offsetColumnHelper = createColumnHelper<DataTableFeatures, GroupOffset>();
const memberColumnHelper = createColumnHelper<DataTableFeatures, GroupMember>();

const memberColumns = memberColumnHelper.columns([
  memberColumnHelper.accessor("clientId", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Client ID" />,
    cell: ({ getValue }) => <span className="font-mono">{getValue()}</span>,
  }),
  memberColumnHelper.accessor("id", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Member ID" />,
    meta: { width: "20rem" },
    cell: ({ row }) => (
      <span className="flex items-center gap-1">
        <span className="max-w-72 truncate font-mono text-muted-foreground">{row.original.id}</span>
        <CopyButton value={row.original.id} label="Copy member ID" reveal />
      </span>
    ),
  }),
  memberColumnHelper.accessor("host", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Host" />,
    meta: { width: "9rem" },
    cell: ({ getValue }) => <span className="font-mono text-muted-foreground">{getValue()}</span>,
  }),
  memberColumnHelper.accessor("assignments", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Assignments" />,
    cell: ({ row }) => (
      <span className="flex flex-wrap gap-1">
        {row.original.assignments.map((assignment) => (
          <Pill key={assignment.topic} className="max-w-full font-mono font-normal text-foreground">
            <span className="truncate">{assignment.topic}</span>
            <span className="shrink-0 text-muted-foreground">×{assignment.partitions.length}</span>
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
      meta: { align: "right", width: "7rem" },
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
    <Facts>
      <span>{members} members</span>
      <span>{topicCount} topics</span>
      <span>{partitions} partitions</span>
      <span className={cn("text-foreground", TONE_TEXT[lagTone(toNumber(group.totalLag))])}>
        {group.lagComplete ? "" : "≥ "}
        {formatCount(group.totalLag)} lag
      </span>
    </Facts>
  );
}

function ConsumerGroupPage() {
  const cluster = useClusterName();
  const navigate = Route.useNavigate();
  const { group: groupId } = Route.useParams();
  const { tab } = Route.useSearch();
  const { data: group, isPending, isError, error } = useGroup(cluster, groupId);

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
      cell: ({ getValue }) => (
        <Link
          to="/cluster/$cluster/topics/$topic"
          params={{ cluster, topic: getValue() }}
          className="font-mono outline-none"
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
      meta: { align: "right", width: "6rem" },
      cell: ({ getValue }) => <span className="numeric">{getValue()}</span>,
    }),
    offsetColumnHelper.accessor((offset) => toNumber(offset.currentOffset), {
      id: "current",
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="Committed" className="justify-end" />
      ),
      meta: { align: "right", width: "9rem" },
      cell: ({ row }) => (
        <span className="text-muted-foreground">{formatNumber(row.original.currentOffset)}</span>
      ),
    }),
    offsetColumnHelper.accessor((offset) => toNumber(offset.endOffset), {
      id: "end",
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="End offset" className="justify-end" />
      ),
      meta: { align: "right", width: "9rem" },
      cell: ({ row }) => (
        <span className="text-muted-foreground">{formatNumber(row.original.endOffset)}</span>
      ),
    }),
    offsetColumnHelper.accessor((offset) => toNumber(offset.lag), {
      id: "lag",
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="Lag" className="justify-end" />
      ),
      meta: { align: "right", width: "12rem" },
      cell: ({ getValue, row }) => {
        const lag = getValue();

        return (
          <span className="flex items-center justify-end gap-3">
            <span className="h-1 w-20 overflow-hidden rounded-full bg-muted">
              <span
                className={cn(
                  "block h-full rounded-full",
                  lag > maxLag / 2 ? "bg-destructive/80" : "bg-foreground/35",
                )}
                style={{
                  width: `${Math.max(lag === 0 ? 0 : 3, (lag / maxLag) * 100)}%`,
                }}
              />
            </span>
            <span className={cn("numeric w-16", lag === 0 && "text-muted-foreground")}>
              {formatNumber(row.original.lag)}
            </span>
          </span>
        );
      },
    }),
    offsetColumnHelper.accessor((offset) => offset.memberId ?? "", {
      id: "member",
      header: ({ column }) => (
        <DataTableColumnHeader column={column} title="Member" className="justify-end" />
      ),
      meta: { align: "right", width: "14rem" },
      cell: ({ row }) =>
        row.original.memberId ? (
          <span className="font-mono text-muted-foreground">
            {memberLabels.get(row.original.memberId) ?? row.original.memberId}
          </span>
        ) : (
          <span className="text-muted-foreground/70">unassigned</span>
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
              <Pill className="gap-1.5 text-foreground">
                <GroupStateBadge state={group.state} />
              </Pill>
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
        onValueChange={(value) =>
          void navigate({ search: { tab: groupTab(value) }, replace: true })
        }
        className="min-h-0 flex-1"
      >
        <TabsList
          variant="line"
          className="w-full shrink-0 justify-start gap-3 border-b [&>[data-slot=tabs-trigger]]:flex-none [&>[data-slot=tabs-trigger]]:after:bg-brand"
        >
          <TabsTrigger value="offsets">
            Offsets
            <TabCount value={isPending ? undefined : offsets.length} />
          </TabsTrigger>
          <TabsTrigger value="members">
            Members
            <TabCount value={isPending ? undefined : members.length} />
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
