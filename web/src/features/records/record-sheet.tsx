import { useRef, useState } from "react";
import { CopyPlusIcon, ListXIcon, SearchXIcon } from "lucide-react";
import { Link } from "@tanstack/react-router";

import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty";
import { Spinner } from "@/components/ui/spinner";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { CopyButton } from "@/components/copy-button";
import { IconButton } from "@/components/icon-button";
import { PayloadView } from "@/components/payload-view";
import { versionWithId } from "@/features/schemas/subject-search";
import { useAccess } from "@/hooks/use-access";
import { useSubjectRows } from "@/lib/api/catalog";
import { ApiError, apiErrorMessage } from "@/lib/api/client";
import { useRecord, type RecordAddress } from "@/lib/api/live";
import { formatBytes, formatRelative, formatTimestamp } from "@/lib/format";
import type { KafkaRecord, TopicDetail } from "@/lib/api/types";
import { cn } from "@/lib/utils";

import { DeleteRecordsDialog } from "./delete-records";
import { ObfuscatedBadge } from "./obfuscated-badge";
import { ProduceRecordSheet, duplicateDraft } from "./produce-record";
import { useRecordAddress } from "./record-address";

export function RecordSheet({
  cluster,
  topic,
  records,
  obfuscated,
  schemaId,
}: {
  cluster: string;
  topic: TopicDetail;
  records: KafkaRecord[];
  obfuscated: boolean;
  schemaId: number | null;
}) {
  const { address, open, link } = useRecordAddress();
  const [expanded, setExpanded] = useState(false);
  const [cutting, setCutting] = useState(false);
  const sheetRef = useRef<HTMLDivElement>(null);

  const listed =
    address == null
      ? null
      : (records.find(
          (record) => record.partition === address.partition && record.offset === address.offset,
        ) ?? null);
  const lookup = useRecord(cluster, topic.name, address, schemaId, listed == null);
  const selectedRecord = listed ?? lookup.data?.record ?? null;
  const selectedObfuscated = listed ? obfuscated : (lookup.data?.obfuscated ?? false);
  const selectedSchemaId =
    selectedRecord?.value == null ? null : (selectedRecord.schemaId ?? schemaId);
  const { canChange } = useAccess();
  const canDuplicate = !topic.internal && canChange(cluster, "PRODUCE");
  const duplicate = canDuplicate && selectedRecord ? duplicateDraft(selectedRecord) : null;
  const canDelete = !topic.internal && canChange(cluster, "DELETE_RECORDS");

  return (
    <Sheet
      open={address !== null}
      onOpenChange={(next) => {
        if (!next) {
          open(null);
          setExpanded(false);
        }
      }}
    >
      <SheetContent
        ref={sheetRef}
        initialFocus={sheetRef}
        side="right"
        className={cn(
          "w-full gap-0 outline-none data-[side=right]:w-full",
          expanded
            ? "data-[side=right]:sm:max-w-[min(90vw,56rem)]"
            : "data-[side=right]:sm:max-w-2xl",
        )}
      >
        {address ? (
          <SheetHeader className="gap-1 border-b px-5 py-4 pr-12">
            <div className="flex min-w-0 items-center gap-2">
              <SheetTitle className="min-w-0 truncate font-mono text-sm font-medium">
                <span className="text-muted-foreground">{topic.name}</span>
                <span className="text-muted-foreground/60"> / </span>
                {address.partition}
                <span className="text-muted-foreground/60"> @ </span>
                {address.offset}
              </SheetTitle>
              <CopyButton value={link(address)} label="Copy link to this record" />
              {selectedObfuscated ? <ObfuscatedBadge /> : null}
              {canDuplicate || canDelete ? (
                <div className="ml-auto flex shrink-0 items-center gap-1">
                  {canDuplicate ? (
                    <ProduceRecordSheet
                      cluster={cluster}
                      topic={topic}
                      draft={duplicate ?? undefined}
                      trigger={duplicate ? <IconButton label="Duplicate this record" /> : null}
                      onProduced={open}
                    >
                      <CopyPlusIcon />
                    </ProduceRecordSheet>
                  ) : null}
                  {canDelete ? (
                    <>
                      <IconButton
                        label="Delete the records before this one"
                        onClick={() => setCutting(true)}
                      >
                        <ListXIcon />
                      </IconButton>
                      <DeleteRecordsDialog
                        cluster={cluster}
                        topic={topic}
                        cut={{ partition: address.partition, before: address.offset }}
                        open={cutting}
                        onOpenChange={setCutting}
                      />
                    </>
                  ) : null}
                </div>
              ) : null}
            </div>
            <SheetDescription>
              {selectedRecord
                ? `Produced ${formatRelative(selectedRecord.timestamp)}`
                : lookup.isError
                  ? "Not available"
                  : "Loading…"}
            </SheetDescription>
          </SheetHeader>
        ) : null}
        {address && !selectedRecord ? (
          lookup.isError ? (
            <Empty className="py-10">
              <EmptyHeader>
                <EmptyMedia variant="icon">
                  <SearchXIcon />
                </EmptyMedia>
                <EmptyTitle>Record not found</EmptyTitle>
                <EmptyDescription>{missingRecord(lookup.error, address)}</EmptyDescription>
              </EmptyHeader>
            </Empty>
          ) : (
            <div className="flex flex-1 items-center justify-center">
              <Spinner />
            </div>
          )
        ) : null}
        {selectedRecord ? (
          <>
            <dl className="grid shrink-0 grid-cols-3 gap-x-4 gap-y-3 border-b px-5 py-4">
              <Meta label="Partition" value={selectedRecord.partition} />
              <Meta label="Offset" value={selectedRecord.offset} />
              <Meta label="Size" value={formatBytes(selectedRecord.sizeBytes)} />
              <Meta label="Timestamp" value={formatTimestamp(selectedRecord.timestamp)} />
              <Meta
                label="Schema"
                value={
                  selectedSchemaId != null ? (
                    <SchemaLink cluster={cluster} topic={topic.name} id={selectedSchemaId} />
                  ) : (
                    <span className="text-muted-foreground">—</span>
                  )
                }
              />
            </dl>

            <div className="flex min-h-0 flex-1 flex-col gap-5 overflow-hidden px-5 py-4">
              <PayloadView
                key={`key-${selectedRecord.partition}-${selectedRecord.offset}`}
                label="Key"
                source={selectedRecord.key ?? "null"}
                copyLabel="Copy key"
                showCopy={Boolean(selectedRecord.key)}
              />

              <PayloadView
                key={`value-${selectedRecord.partition}-${selectedRecord.offset}`}
                label="Value"
                source={selectedRecord.value ?? "null"}
                filename={`${topic.name}-${selectedRecord.partition}-${selectedRecord.offset}.json`}
                copyLabel="Copy value"
                showCopy={Boolean(selectedRecord.value)}
                showDownload
                showExpand
                expanded={expanded}
                onExpandedChange={setExpanded}
                fill
              />

              <section className="shrink-0 space-y-2">
                <h3 className="flex items-center gap-1.5 text-xs font-medium text-muted-foreground">
                  Headers
                  <span className="numeric text-muted-foreground/60">
                    {selectedRecord.headers.length}
                  </span>
                </h3>
                {selectedRecord.headers.length === 0 ? (
                  <p className="text-sm text-muted-foreground">No headers.</p>
                ) : (
                  <dl className="grid max-h-40 grid-cols-[minmax(0,auto)_minmax(0,1fr)] gap-x-6 overflow-y-auto rounded-lg border bg-subtle px-3 py-2 font-mono text-sm">
                    {selectedRecord.headers.map((header) => (
                      <div key={header.key} className="contents">
                        <dt className="truncate py-1 text-muted-foreground">{header.key}</dt>
                        <dd className="py-1 break-all">{header.value}</dd>
                      </div>
                    ))}
                  </dl>
                )}
              </section>
            </div>
          </>
        ) : null}
      </SheetContent>
    </Sheet>
  );
}

function missingRecord(error: unknown, { partition, offset }: RecordAddress) {
  if (error instanceof ApiError && error.code === "UNKNOWN_OFFSET") {
    return `Partition ${partition} has no record at offset ${offset}. Retention or compaction may have removed it, or the offset may hold a transaction marker.`;
  }
  if (error instanceof ApiError && error.code === "UNKNOWN_PARTITION") {
    return `This topic has no partition ${partition}.`;
  }
  return apiErrorMessage(error, "Failed to load this record.");
}

function SchemaLink({ cluster, topic, id }: { cluster: string; topic: string; id: number }) {
  const { data } = useSubjectRows(cluster);
  const matches = data?.rows.filter((row) => versionWithId(row, id) != null) ?? [];
  const subject = matches.find((row) => row.subject === `${topic}-value`) ?? matches[0];
  const version = subject && versionWithId(subject, id);

  if (subject == null || version == null) {
    return id;
  }

  return (
    <Tooltip>
      <TooltipTrigger
        render={
          <Link
            to="/cluster/$cluster/schemas"
            params={{ cluster }}
            search={{ subject: subject.subject, version }}
            className="text-primary underline-offset-4 outline-none hover:underline focus-visible:underline"
          />
        }
      >
        {id}
      </TooltipTrigger>
      <TooltipContent>
        <span className="font-mono">{subject.subject}</span> · v{version}
      </TooltipContent>
    </Tooltip>
  );
}

function Meta({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="min-w-0 space-y-0.5">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="numeric truncate text-sm">{value}</dd>
    </div>
  );
}
