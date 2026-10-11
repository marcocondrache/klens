import { getRouteApi } from "@tanstack/react-router";
import { ListXIcon, Trash2Icon } from "lucide-react";

import { DropdownMenuItem, DropdownMenuSeparator } from "@/components/ui/dropdown-menu";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ActionsMenu, useOpenDialog } from "@/components/actions-menu";
import { ConfirmDelete } from "@/components/confirm-delete";
import { CopyButton } from "@/components/copy-button";
import { DataTable } from "@/components/data-table/data-table";
import { Facts } from "@/components/facts";
import { PageHeader } from "@/components/page-header";
import { TabCount } from "@/components/tab-count";
import { GroupStateBadge, PendingValue, Pill, TONE_TEXT } from "@/components/status";
import { useAccess } from "@/hooks/use-access";
import { useGroup } from "@/lib/api/catalog";
import { clusterPathname, del, resourceId } from "@/lib/api/client";
import type { GroupDetail } from "@/lib/api/types";
import { catalogLookupMessage } from "@/lib/catalog-lookup";
import { useClusterName } from "@/lib/clusters";
import { formatCount } from "@/lib/format";
import { lagTone } from "@/lib/tone";

import { DeleteOffsetsDialog } from "./delete-offsets";
import { committedTopics, hasMembers } from "./group-state";
import { memberColumns } from "./member-columns";
import { offsetColumns } from "./offset-columns";
import { ResetOffsetsSheet } from "./reset-offsets";
import { groupTab } from "./search";

const route = getRouteApi("/cluster/$cluster/groups_/$group");

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
      {group.totalLag === null ? (
        <PendingValue label="Fetching committed offsets" />
      ) : (
        <span className={TONE_TEXT[lagTone(group.totalLag)]}>
          {group.lagComplete ? "" : "≥ "}
          {formatCount(group.totalLag)} lag
        </span>
      )}
    </Facts>
  );
}

export function GroupPage() {
  const cluster = useClusterName();
  const navigate = route.useNavigate();
  const { group: groupId } = route.useParams();
  const { tab } = route.useSearch();
  const { canChange } = useAccess();
  const canReset = canChange(cluster, "RESET_OFFSETS");
  const canDelete = canChange(cluster, "DELETE_GROUPS");
  const { data: group, isPending, isError, error } = useGroup(cluster, groupId);
  const dialogs = useOpenDialog<"offsets" | "delete">();

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
  const canDeleteOffsets =
    group != null && canChange(cluster, "DELETE_OFFSETS") && committedTopics(group).length > 0;
  const maxLag = Math.max(
    1,
    ...offsets.flatMap((offset) => (offset.lag === null ? [] : [offset.lag])),
  );
  const memberLabels = new Map(members.map((member) => [member.id, member.clientId] as const));
  const topicCount = new Set([
    ...offsets.map((offset) => offset.topic),
    ...members.flatMap((member) => member.assignments.map((assignment) => assignment.topic)),
  ]).size;

  const columns = offsetColumns({ cluster, maxLag, memberLabels });

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
            </>
          ) : null
        }
        actions={
          group && (canReset || canDelete || canDeleteOffsets) ? (
            <>
              {canReset ? <ResetOffsetsSheet cluster={cluster} group={group} /> : null}
              {canDelete || canDeleteOffsets ? (
                <ActionsMenu label="Group actions">
                  {canDeleteOffsets ? (
                    <DropdownMenuItem onClick={() => dialogs.show("offsets")}>
                      <ListXIcon />
                      Delete offsets
                    </DropdownMenuItem>
                  ) : null}
                  {canDelete ? (
                    <>
                      {canDeleteOffsets ? <DropdownMenuSeparator /> : null}
                      <DropdownMenuItem
                        variant="destructive"
                        onClick={() => dialogs.show("delete")}
                      >
                        <Trash2Icon />
                        Delete group
                      </DropdownMenuItem>
                    </>
                  ) : null}
                </ActionsMenu>
              ) : null}
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

      {group && canDeleteOffsets ? (
        <DeleteOffsetsDialog cluster={cluster} group={group} {...dialogs.props("offsets")} />
      ) : null}
      {group && canDelete ? (
        <ConfirmDelete
          {...dialogs.props("delete")}
          noun="group"
          name={groupId}
          consequence={
            hasMembers(group.state) ? (
              <>
                Kafka only deletes a group without members. Stop the consumers of{" "}
                <span className="font-mono">{groupId}</span> first.
              </>
            ) : (
              <>
                This deletes <span className="font-mono">{groupId}</span> and its committed offsets,
                and cannot be undone. Its consumers start from their reset policy when they rejoin.
              </>
            )
          }
          onDelete={() => del(clusterPathname(cluster, "groups", resourceId(groupId)))}
          onDeleted={() => void navigate({ to: "/cluster/$cluster/groups", params: { cluster } })}
        />
      ) : null}

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
            columns={columns}
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
          />
        </TabsContent>

        <TabsContent value="members" className="mt-4 flex min-h-0 flex-col">
          <DataTable
            columns={memberColumns}
            data={members}
            getRowId={(member) => member.id}
            loading={isPending}
            emptyState="This group has no active members."
          />
        </TabsContent>
      </Tabs>
    </div>
  );
}
