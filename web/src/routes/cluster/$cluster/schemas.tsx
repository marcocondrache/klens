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
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { CopyButton } from "@/components/copy-button";
import { DataTableColumnHeader } from "@/components/data-table/column-header";
import { DataTable } from "@/components/data-table/data-table";
import { type DataTableFeatures } from "@/components/data-table/features";
import { JsonBlock } from "@/components/json-block";
import { PageHeader } from "@/components/page-header";
import { SearchField } from "@/components/search-field";
import { Pill } from "@/components/status";
import { useNow } from "@/hooks/use-now";
import { useSubjectRows } from "@/lib/api/catalog";
import { useSubject } from "@/lib/api/live";
import { laneCaption, useClusterName } from "@/lib/clusters";
import { apiErrorMessage } from "@/lib/api/client";
import { prettyJson } from "@/lib/format";
import type { SubjectRow } from "@/lib/api/types";
import { parseSchemasSearch } from "@/lib/route-search";
import { useAccess } from "@/hooks/use-access";

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

function SchemasPage() {
  const cluster = useClusterName();
  const navigate = Route.useNavigate();
  const { q: term = "" } = Route.useSearch();
  const [selected, setSelected] = useState<SubjectRow | null>(null);
  const [version, setVersion] = useState<number | null>(null);
  const { can } = useAccess();
  const canSchemaText = can(cluster, "SCHEMA_TEXT");
  const { data, isPending, isError, error } = useSubjectRows(cluster);
  const subjects = data?.rows ?? EMPTY_SUBJECTS;
  const now = useNow();
  const caption = laneCaption(data?.sourceHealth, now);

  const { data: detail, isPending: detailPending } = useSubject(
    cluster,
    selected?.subject ?? null,
    version,
    canSchemaText,
  );

  function open(subject: SubjectRow) {
    setSelected(subject);
    setVersion(null);
  }

  const rows = useMemo(() => {
    const needle = term.trim().toLowerCase();
    if (!needle) return subjects;
    return subjects.filter((subject) => subject.subject.toLowerCase().includes(needle));
  }, [subjects, term]);
  const schemaText = detail ? prettyJson(detail.schema) : "";
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

      <Sheet open={selected !== null} onOpenChange={(isOpen) => !isOpen && setSelected(null)}>
        <SheetContent side="right" className="w-full gap-0 sm:max-w-lg">
          {selected ? (
            <>
              <SheetHeader className="border-b">
                <SheetTitle className="font-mono text-sm">{selected.subject}</SheetTitle>
                <SheetDescription>
                  {selected.type} · version {shownVersion} · {selected.compatibility}
                </SheetDescription>
              </SheetHeader>

              <div className="flex-1 space-y-4 overflow-y-auto p-4">
                <div className="flex items-center justify-between">
                  <h3 className="text-sm font-medium text-muted-foreground">Schema</h3>
                  {canSchemaText && schemaText ? (
                    <CopyButton value={schemaText} label="Copy schema" />
                  ) : null}
                </div>
                {!canSchemaText ? (
                  <p className="text-sm text-muted-foreground">
                    Schema text is not available for your role.
                  </p>
                ) : detailPending ? (
                  <p className="text-sm text-muted-foreground">Loading schema…</p>
                ) : (
                  <JsonBlock source={schemaText} />
                )}

                <div className="space-y-2">
                  <h3 className="text-sm font-medium text-muted-foreground">Versions</h3>
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
              </div>
            </>
          ) : null}
        </SheetContent>
      </Sheet>
    </div>
  );
}
