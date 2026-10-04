import { useMemo, useState } from "react";
import { getRouteApi } from "@tanstack/react-router";
import { FilePlusIcon, PlusIcon } from "lucide-react";

import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Button } from "@/components/ui/button";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { DataTable } from "@/components/data-table/data-table";
import { LaneCaption } from "@/components/lane-caption";
import { PageHeader } from "@/components/page-header";
import { PayloadView } from "@/components/payload-view";
import { SearchField } from "@/components/search-field";
import { useAccess } from "@/hooks/use-access";
import { useSearchDraft } from "@/hooks/use-search-draft";
import { apiErrorMessage } from "@/lib/api/client";
import { useSubjectRows } from "@/lib/api/catalog";
import { useSubject } from "@/lib/api/live";
import type { RegisteredVersion, SubjectRow } from "@/lib/api/types";
import { useClusterName } from "@/lib/clusters";
import { formatEnumLabel, isJson } from "@/lib/format";
import { cn } from "@/lib/utils";

import { DeleteSchemaDialog } from "./delete-schema";
import { NEW_SUBJECT, RegisterSchemaDialog } from "./register-schema";
import { SchemaLoading } from "./schema-loading";
import { subjectColumns } from "./schemas-columns";

const route = getRouteApi("/cluster/$cluster/schemas");

const EMPTY_SUBJECTS: SubjectRow[] = [];

function schemaFilename(subject: string, version: number, schema: string) {
  const base = subject.replaceAll("/", "-");
  return `${base}-v${version}.${isJson(schema) ? "json" : "txt"}`;
}

export function SchemasPage() {
  const cluster = useClusterName();
  const navigate = route.useNavigate();
  const { q: term, subject, version } = route.useSearch();
  const searchInput = useSearchDraft(term, (q) => {
    void navigate({ search: (prev) => ({ ...prev, q }), replace: true });
  });
  const [expanded, setExpanded] = useState(false);
  const { can, canChange } = useAccess();
  const canSchemaText = can(cluster, "SCHEMA_TEXT");
  const { data, isPending, isError, error } = useSubjectRows(cluster);
  const canManage = data?.hasRegistry === true && canChange(cluster, "MANAGE_SCHEMAS");
  const subjects = data?.rows ?? EMPTY_SUBJECTS;
  const selected = subject ? (subjects.find((row) => row.subject === subject) ?? null) : null;

  const {
    data: detail,
    isPending: detailPending,
    isError: detailIsError,
    error: detailError,
  } = useSubject(cluster, selected?.subject ?? null, version ?? null, canSchemaText);

  function open(row: SubjectRow) {
    void navigate({
      search: (prev) => ({ ...prev, subject: row.subject, version: undefined }),
      replace: true,
    });
  }

  function show(subject: string, registered: RegisteredVersion) {
    void navigate({
      search: (prev) => ({ ...prev, subject, version: registered.version }),
      replace: true,
    });
  }

  function showRemaining(remaining: number | null) {
    if (remaining === null) {
      close();
      return;
    }
    void navigate({
      search: (prev) => ({ ...prev, version: remaining }),
      replace: true,
    });
  }

  function close() {
    setExpanded(false);
    void navigate({
      search: (prev) => ({ ...prev, subject: undefined, version: undefined }),
      replace: true,
    });
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
        description={
          <>
            {rows.length} subjects
            <LaneCaption lane={data?.sourceHealth} />
          </>
        }
        actions={
          canManage ? (
            <RegisterSchemaDialog
              cluster={cluster}
              draft={NEW_SUBJECT}
              trigger={<Button />}
              onRegistered={show}
            >
              <PlusIcon data-icon="inline-start" />
              Register schema
            </RegisterSchemaDialog>
          ) : null
        }
      />

      <DataTable
        columns={subjectColumns}
        data={rows}
        getRowId={(subject) => subject.subject}
        toolbar={
          <SearchField className="shrink-0" {...searchInput} placeholder="Search subjects…" />
        }
        loading={isPending}
        error={isError ? apiErrorMessage(error, "Failed to load schemas.") : undefined}
        defaultSort={{ id: "subject", direction: "asc" }}
        onRowClick={open}
      />

      <Sheet
        open={selected !== null}
        onOpenChange={(isOpen) => {
          if (!isOpen) close();
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
                    Your role cannot view schema text.
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
                  <div className="flex items-center justify-between gap-2">
                    <h3 className="text-xs font-medium text-muted-foreground">Versions</h3>
                    {canManage ? (
                      <div className="flex items-center gap-2">
                        <RegisterSchemaDialog
                          cluster={cluster}
                          draft={{
                            subject: selected.subject,
                            type: detail?.type ?? selected.type,
                            schema: detail?.schema ?? "",
                            references: detail?.references ?? [],
                          }}
                          trigger={<Button variant="outline" size="sm" />}
                          onRegistered={show}
                        >
                          <FilePlusIcon data-icon="inline-start" />
                          New version
                        </RegisterSchemaDialog>
                        {shownVersion != null ? (
                          <DeleteSchemaDialog
                            cluster={cluster}
                            subject={selected}
                            version={shownVersion}
                            onDeleted={showRemaining}
                          />
                        ) : null}
                      </div>
                    ) : null}
                  </div>
                  <div className="max-h-32 overflow-y-auto">
                    <ToggleGroup
                      value={shownVersion != null ? [String(shownVersion)] : []}
                      onValueChange={(next) => {
                        const picked = next[0];
                        if (picked == null) return;
                        void navigate({
                          search: (prev) => ({ ...prev, version: Number(picked) }),
                          replace: true,
                        });
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
