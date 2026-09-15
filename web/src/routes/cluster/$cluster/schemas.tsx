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
import { CopyButton } from "@/components/copy-button";
import { DataTableColumnHeader } from "@/components/data-table/column-header";
import { DataTable } from "@/components/data-table/data-table";
import { type DataTableFeatures } from "@/components/data-table/features";
import { JsonBlock } from "@/components/json-block";
import { PageHeader } from "@/components/page-header";
import { SearchField } from "@/components/search-field";
import { Pill } from "@/components/status";
import { useNow } from "@/hooks/use-now";
import { useCatalogHealth, useSchemaSubjects } from "@/lib/api/catalog";
import { useClusterName } from "@/lib/clusters";
import { catalogHealthCaption } from "@/lib/catalog-health";
import { prettyJson } from "@/lib/format";
import type { SchemaSubject } from "@/lib/api/types";
import { parseSchemasSearch } from "@/lib/route-search";
import { useAccess } from "@/hooks/use-access";

export const Route = createFileRoute("/cluster/$cluster/schemas")({
  validateSearch: parseSchemasSearch,
  component: SchemasPage,
});

const columnHelper = createColumnHelper<DataTableFeatures, SchemaSubject>();

const columns = columnHelper.columns([
  columnHelper.accessor("subject", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Subject" />,
    meta: { label: "Subject" },
    cell: ({ getValue }) => <span className="font-mono text-sm">{getValue()}</span>,
  }),
  columnHelper.accessor("id", {
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="ID" className="justify-end" />
    ),
    meta: { align: "right", label: "ID" },
    cell: ({ getValue }) => <span className="numeric font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor("type", {
    header: ({ column }) => <DataTableColumnHeader column={column} title="Type" />,
    meta: { label: "Type" },
    cell: ({ getValue }) => <Pill tone="brand">{getValue()}</Pill>,
  }),
  columnHelper.accessor("latestVersion", {
    id: "version",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Latest version" className="justify-end" />
    ),
    meta: { align: "right", label: "Latest version" },
    cell: ({ getValue }) => <span className="numeric font-mono">v{getValue()}</span>,
  }),
  columnHelper.accessor((subject) => subject.versions.length, {
    id: "versions",
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Versions" className="justify-end" />
    ),
    meta: { align: "right", label: "Versions" },
    cell: ({ getValue }) => getValue(),
  }),
  columnHelper.accessor("compatibility", {
    header: ({ column }) => (
      <DataTableColumnHeader column={column} title="Compatibility" className="justify-end" />
    ),
    meta: { align: "right", label: "Compatibility" },
    cell: ({ getValue }) => (
      <Pill tone={getValue() === "NONE" ? "warn" : "idle"}>{getValue()}</Pill>
    ),
  }),
]);

function SchemasPage() {
  const cluster = useClusterName();
  const navigate = Route.useNavigate();
  const { q: term = "" } = Route.useSearch();
  const [selected, setSelected] = useState<SchemaSubject | null>(null);
  const { can } = useAccess();
  const canSchemaText = can(cluster, "schemaText");
  const { data: subjects = [], isPending, isError, error } = useSchemaSubjects(cluster);
  const { data: health } = useCatalogHealth(cluster);
  const now = useNow();
  const caption = catalogHealthCaption({
    updatedAt: health?.subjectsUpdatedAt,
    lastError: health?.lastError,
    now,
  });

  const rows = useMemo(() => {
    const needle = term.trim().toLowerCase();
    if (!needle) return subjects;
    return subjects.filter((subject) => subject.subject.toLowerCase().includes(needle));
  }, [subjects, term]);
  const schemaText = selected ? prettyJson(selected.schema) : "";

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
        error={
          isError ? (error instanceof Error ? error.message : "Failed to load schemas.") : undefined
        }
        defaultSort={{ id: "subject", direction: "asc" }}
        onRowClick={setSelected}
        fill
      />

      <Sheet open={selected !== null} onOpenChange={(open) => !open && setSelected(null)}>
        <SheetContent side="right" className="w-full gap-0 sm:max-w-lg">
          {selected ? (
            <>
              <SheetHeader className="border-b">
                <SheetTitle className="font-mono text-sm">{selected.subject}</SheetTitle>
                <SheetDescription>
                  {selected.type} · version {selected.latestVersion} · {selected.compatibility}
                </SheetDescription>
              </SheetHeader>

              <div className="flex-1 space-y-4 overflow-y-auto p-4">
                <div className="flex items-center justify-between">
                  <h3 className="text-sm font-medium text-muted-foreground">Schema</h3>
                  {canSchemaText ? <CopyButton value={schemaText} label="Copy schema" /> : null}
                </div>
                {canSchemaText ? (
                  <JsonBlock source={schemaText} />
                ) : (
                  <p className="text-sm text-muted-foreground">
                    Schema text is not available for your role.
                  </p>
                )}

                <div className="space-y-2">
                  <h3 className="text-sm font-medium text-muted-foreground">Versions</h3>
                  <div className="flex flex-wrap gap-1.5">
                    {selected.versions.map((version) => (
                      <Pill
                        key={version}
                        tone={version === selected.latestVersion ? "brand" : "idle"}
                        className="numeric font-mono"
                      >
                        v{version}
                      </Pill>
                    ))}
                  </div>
                </div>
              </div>
            </>
          ) : null}
        </SheetContent>
      </Sheet>
    </div>
  );
}
