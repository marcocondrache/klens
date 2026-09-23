import { useMemo, useState } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { createColumnHelper } from "@tanstack/react-table";
import { useQueryStates } from "nuqs";

import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Skeleton } from "@/components/ui/skeleton";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { DataTableColumnHeader } from "@/components/data-table/column-header";
import { DataTable } from "@/components/data-table/data-table";
import { type DataTableFeatures } from "@/components/data-table/features";
import { PageHeader } from "@/components/page-header";
import { PayloadView } from "@/components/payload-view";
import { SearchField } from "@/components/search-field";
import { Pill } from "@/components/status";
import { useAccess } from "@/hooks/use-access";
import { useNow } from "@/hooks/use-now";
import { apiErrorMessage } from "@/lib/api/client";
import { useSubjectRows } from "@/lib/api/catalog";
import { useSubject } from "@/lib/api/live";
import type { SubjectRow } from "@/lib/api/types";
import { laneCaption, useClusterName } from "@/lib/clusters";
import { formatEnumLabel, isJson } from "@/lib/format";
import { schemasSearch } from "@/lib/route-search";
import { cn } from "@/lib/utils";

export const Route = createFileRoute("/cluster/$cluster/schemas")({
  component: SchemasPage,
});

const EMPTY_SUBJECTS: SubjectRow[] = [];

const columnHelper = createColumnHelper<DataTableFeatures, SubjectRow>();

const columns = columnHelper.columns([
  columnHelper.accessor("subject", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Subject" />,
    cell: ({ getValue }) => <span className="font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor("id", {
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="ID" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ getValue }) => <span className="numeric text-muted-foreground">{getValue()}</span>,
  }),
  columnHelper.accessor("type", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Type" />,
    cell: ({ getValue }) => <Pill>{formatEnumLabel(getValue())}</Pill>,
  }),
  columnHelper.accessor("latestVersion", {
    id: "version",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Latest version" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ getValue }) => <span className="numeric">v{getValue()}</span>,
  }),
  columnHelper.accessor((subject) => subject.versions.length, {
    id: "versions",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Versions" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ getValue }) => getValue(),
  }),
  columnHelper.accessor("compatibility", {
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Compatibility" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ getValue }) => (
      <span className={getValue() === "NONE" ? "text-warn" : "text-muted-foreground"}>
        {formatEnumLabel(getValue())}
      </span>
    ),
  }),
]);

const SCHEMA_SKELETON: Array<[indent: number, width: number]> = [
  [0, 4],
  [1, 38],
  [1, 52],
  [1, 30],
  [1, 18],
  [2, 4],
  [3, 46],
  [3, 58],
  [2, 12],
  [3, 40],
  [3, 64],
  [2, 6],
  [1, 4],
  [0, 4],
];

function SchemaLoading() {
  return (
    <div className="flex min-h-0 flex-1 flex-col gap-2" role="status" aria-live="polite">
      <div className="flex h-7 items-center">
        <Skeleton className="h-3 w-12 rounded-sm" />
      </div>
      <div
        aria-hidden
        className="min-h-0 flex-1 space-y-3 overflow-hidden rounded-lg border bg-subtle px-3 py-3.5"
      >
        {SCHEMA_SKELETON.map(([indent, width], index) => (
          <Skeleton
            key={index}
            className="h-3 rounded-sm"
            style={{
              marginLeft: `${indent * 1.25}rem`,
              width: `${width}%`,
              animationDelay: `${index * 50}ms`,
            }}
          />
        ))}
      </div>
      <span className="sr-only">Loading schema…</span>
    </div>
  );
}

function schemaFilename(subject: string, version: number, schema: string) {
  const base = subject.replaceAll("/", "-");
  return `${base}-v${version}.${isJson(schema) ? "json" : "txt"}`;
}

function SchemasPage() {
  const cluster = useClusterName();
  const [{ q: term }, setSearch] = useQueryStates(schemasSearch);
  const [selected, setSelected] = useState<SubjectRow | null>(null);
  const [version, setVersion] = useState<number | null>(null);
  const [expanded, setExpanded] = useState(false);
  const { can } = useAccess();
  const canSchemaText = can(cluster, "SCHEMA_TEXT");
  const { data, isPending, isError, error } = useSubjectRows(cluster);
  const subjects = data?.rows ?? EMPTY_SUBJECTS;
  const now = useNow();
  const caption = laneCaption(data?.sourceHealth, now);

  const {
    data: detail,
    isPending: detailPending,
    isError: detailIsError,
    error: detailError,
  } = useSubject(cluster, selected?.subject ?? null, version, canSchemaText);

  function open(subject: SubjectRow) {
    setSelected(subject);
    setVersion(null);
  }

  const rows = useMemo(() => {
    const needle = term.trim().toLowerCase();
    if (!needle) return subjects;
    return subjects.filter((subject) => subject.subject.toLowerCase().includes(needle));
  }, [subjects, term]);
  const shownVersion = version ?? selected?.latestVersion;

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
      <PageHeader
        title="Schema registry"
        description={`${rows.length} subjects registered${caption ? ` · ${caption}` : ""}`}
      />

      <DataTable
        columns={columns}
        data={rows}
        getRowId={(subject) => subject.subject}
        toolbar={
          <SearchField
            className="shrink-0"
            value={term}
            onChange={(event) => void setSearch({ q: event.target.value })}
            placeholder="Search subjects…"
          />
        }
        loading={isPending}
        error={isError ? apiErrorMessage(error, "Failed to load schemas.") : undefined}
        defaultSort={{ id: "subject", direction: "asc" }}
        onRowClick={open}
        fill
      />

      <Sheet
        open={selected !== null}
        onOpenChange={(isOpen) => {
          if (!isOpen) {
            setSelected(null);
            setExpanded(false);
          }
        }}
      >
        <SheetContent
          side="right"
          className={cn(
            "w-full gap-0 data-[side=right]:w-full",
            expanded
              ? "data-[side=right]:sm:max-w-[min(90vw,56rem)]"
              : "data-[side=right]:sm:max-w-2xl",
          )}
        >
          {selected ? (
            <>
              <SheetHeader className="gap-1 border-b px-5 py-4 pr-12">
                <SheetTitle className="truncate font-mono text-sm font-medium">
                  {selected.subject}
                </SheetTitle>
                <SheetDescription>
                  {formatEnumLabel(selected.type)} · version {shownVersion} ·{" "}
                  {formatEnumLabel(selected.compatibility)} compatibility
                </SheetDescription>
              </SheetHeader>

              <div className="flex min-h-0 flex-1 flex-col gap-5 overflow-hidden px-5 py-4">
                {!canSchemaText ? (
                  <p className="min-h-0 flex-1 text-sm text-muted-foreground">
                    Schema text is not available for your role.
                  </p>
                ) : detail ? (
                  <PayloadView
                    key={`${selected.subject}@${detail.version}`}
                    label="Schema"
                    source={detail.schema}
                    filename={schemaFilename(selected.subject, detail.version, detail.schema)}
                    copyLabel="Copy schema"
                    showDownload
                    showExpand
                    expanded={expanded}
                    onExpandedChange={setExpanded}
                    fill
                  />
                ) : detailPending ? (
                  <SchemaLoading />
                ) : detailIsError ? (
                  <p className="min-h-0 flex-1 text-sm text-muted-foreground">
                    {apiErrorMessage(detailError, "Failed to load schema.")}
                  </p>
                ) : null}

                <section className="shrink-0 space-y-2">
                  <h3 className="text-xs font-medium text-muted-foreground">Versions</h3>
                  <div className="max-h-32 overflow-y-auto">
                    <ToggleGroup
                      value={shownVersion != null ? [String(shownVersion)] : []}
                      onValueChange={(next) => {
                        const picked = next[0];
                        if (picked != null) setVersion(Number(picked));
                      }}
                      variant="outline"
                      size="sm"
                      spacing={0}
                      className="flex-wrap"
                      aria-label="Schema version"
                    >
                      {selected.versions.map((entry) => (
                        <ToggleGroupItem
                          key={entry}
                          value={String(entry)}
                          className="numeric font-mono"
                        >
                          v{entry}
                        </ToggleGroupItem>
                      ))}
                    </ToggleGroup>
                  </div>
                </section>
              </div>
            </>
          ) : null}
        </SheetContent>
      </Sheet>
    </div>
  );
}
