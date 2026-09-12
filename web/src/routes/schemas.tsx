import { useMemo, useState } from "react";
import { useSearchParams } from "react-router";

import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { CopyButton } from "@/components/copy-button";
import { DataTable } from "@/components/data-table";
import { JsonBlock } from "@/components/json-block";
import { PageHeader } from "@/components/page-header";
import { SearchField } from "@/components/search-field";
import { Pill } from "@/components/status";
import { useNow } from "@/hooks/use-now";
import { useCatalogHealth, useSchemaSubjects } from "@/lib/api/catalog";
import { useClusterName } from "@/lib/clusters";
import { catalogHealthCaption } from "@/lib/catalog-health";
import type { SchemaSubject } from "@/lib/api/types";
import { createAppColumnHelper } from "@/lib/table";

const columnHelper = createAppColumnHelper<SchemaSubject>();

const columns = columnHelper.columns([
  columnHelper.accessor("subject", {
    header: "Subject",
    cell: ({ getValue }) => <span className="font-mono text-sm">{getValue()}</span>,
  }),
  columnHelper.accessor("id", {
    header: "ID",
    meta: { align: "right" },
    cell: ({ getValue }) => <span className="numeric font-mono">{getValue()}</span>,
  }),
  columnHelper.accessor("type", {
    header: "Type",
    cell: ({ getValue }) => <Pill tone="brand">{getValue()}</Pill>,
  }),
  columnHelper.accessor("latestVersion", {
    id: "version",
    header: "Latest version",
    meta: { align: "right" },
    cell: ({ getValue }) => <span className="numeric font-mono">v{getValue()}</span>,
  }),
  columnHelper.accessor((subject) => subject.versions.length, {
    id: "versions",
    header: "Versions",
    meta: { align: "right" },
    cell: ({ getValue }) => getValue(),
  }),
  columnHelper.accessor("compatibility", {
    header: "Compatibility",
    meta: { align: "right" },
    cell: ({ getValue }) => (
      <Pill tone={getValue() === "NONE" ? "warn" : "idle"}>{getValue()}</Pill>
    ),
  }),
]);

export function SchemasPage() {
  const cluster = useClusterName();
  const [params, setParams] = useSearchParams();
  const [selected, setSelected] = useState<SchemaSubject | null>(null);

  const term = params.get("q") ?? "";
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

  return (
    <div className="flex min-h-0 flex-1 flex-col gap-5">
      <PageHeader
        title="Schema registry"
        description={`${rows.length} subjects registered${caption ? ` · ${caption}` : ""}`}
      />

      <SearchField
        className="shrink-0"
        value={term}
        onChange={(event) => {
          const next = new URLSearchParams(params);
          if (event.target.value) {
            next.set("q", event.target.value);
          } else {
            next.delete("q");
          }
          setParams(next, { replace: true });
        }}
        placeholder="Search subjects…"
      />

      <DataTable
        columns={columns}
        data={rows}
        getRowId={(subject) => subject.subject}
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
                  <h3 className="text-sm font-medium tracking-wide text-muted-foreground">
                    Schema
                  </h3>
                  <CopyButton value={selected.schema} label="Copy schema" />
                </div>
                <JsonBlock source={selected.schema} />

                <div className="space-y-2">
                  <h3 className="text-sm font-medium tracking-wide text-muted-foreground">
                    Versions
                  </h3>
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
