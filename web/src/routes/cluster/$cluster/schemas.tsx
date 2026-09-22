import { useMemo, useState } from "react";
import { createFileRoute } from "@tanstack/react-router";
import { createColumnHelper } from "@tanstack/react-table";

import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Spinner } from "@/components/ui/spinner";
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
import { isJson } from "@/lib/format";
import { parseSchemasSearch } from "@/lib/route-search";
import { cn } from "@/lib/utils";

export const Route = createFileRoute("/cluster/$cluster/schemas")({
  validateSearch: parseSchemasSearch,
  component: SchemasPage,
});

const EMPTY_SUBJECTS: SubjectRow[] = [];

const columnHelper = createColumnHelper<DataTableFeatures, SubjectRow>();

const columns = columnHelper.columns([
  columnHelper.accessor("subject", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Subject" />,
    cell: ({ getValue }) => <span className="font-mono text-sm">{getValue()}</span>,
  }),
  columnHelper.accessor("id", {
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="ID" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ getValue }) => <span className="numeric font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor("type", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Type" />,
    cell: ({ getValue }) => <Pill tone="brand">{getValue()}</Pill>,
  }),
  columnHelper.accessor("latestVersion", {
    id: "version",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Latest version" className="justify-end" />
    ),
    meta: { align: "right" },
    cell: ({ getValue }) => <span className="numeric font-mono">v{getValue()}</span>,
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
      <Pill tone={getValue() === "NONE" ? "warn" : "idle"}>{getValue()}</Pill>
    ),
  }),
]);

function SchemaLoading() {
  return (
    <div
      className="flex min-h-0 flex-1 items-center justify-center"
      role="status"
      aria-live="polite"
    >
      <Spinner className="size-6" aria-hidden />
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
  const navigate = Route.useNavigate();
  const { q: term = "" } = Route.useSearch();
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
            onChange={(event) => {
              const value = event.target.value;
              void navigate({
                to: ".",
                search: value ? { q: value } : {},
                replace: true,
                resetScroll: false,
              });
            }}
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
              <SheetHeader className="border-b">
                <SheetTitle className="font-mono text-sm">{selected.subject}</SheetTitle>
                <SheetDescription>
                  {selected.type} · version {shownVersion} · {selected.compatibility}
                </SheetDescription>
              </SheetHeader>

              <div className="flex min-h-0 flex-1 flex-col gap-5 overflow-hidden p-4">
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
                  <h3 className="text-sm font-medium text-muted-foreground">Versions</h3>
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
