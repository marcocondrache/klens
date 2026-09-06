import { useMemo, useState } from "react"
import { SearchIcon } from "lucide-react"
import { useSearchParams } from "react-router"

import {
  InputGroup,
  InputGroupAddon,
  InputGroupInput,
} from "@/components/ui/input-group"
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet"
import { CopyButton } from "@/components/copy-button"
import { DataTable, type Column } from "@/components/data-table"
import { JsonBlock } from "@/components/json-block"
import { PageHeader } from "@/components/page-header"
import { Pill } from "@/components/status"
import { useSchemaSubjects } from "@/lib/api/queries"
import { useClusterName } from "@/lib/clusters"
import type { SchemaSubject } from "@/lib/api/types"

export function SchemasPage() {
  const cluster = useClusterName()
  const [params, setParams] = useSearchParams()
  const [selected, setSelected] = useState<SchemaSubject | null>(null)

  const term = params.get("q") ?? ""
  const { data: subjects = [], isPending } = useSchemaSubjects(cluster)

  const rows = useMemo(() => {
    const needle = term.trim().toLowerCase()
    if (!needle) return subjects
    return subjects.filter((subject) => subject.subject.toLowerCase().includes(needle))
  }, [subjects, term])

  const columns: Array<Column<SchemaSubject>> = [
    {
      id: "subject",
      header: "Subject",
      sortValue: (subject) => subject.subject,
      cell: (subject) => <span className="font-mono text-[0.8rem]">{subject.subject}</span>,
    },
    {
      id: "id",
      header: "ID",
      align: "right",
      sortValue: (subject) => subject.id,
      cell: (subject) => <span className="numeric font-mono">{subject.id}</span>,
    },
    {
      id: "type",
      header: "Type",
      sortValue: (subject) => subject.type,
      cell: (subject) => <Pill tone="brand">{subject.type}</Pill>,
    },
    {
      id: "version",
      header: "Latest version",
      align: "right",
      sortValue: (subject) => subject.latestVersion,
      cell: (subject) => (
        <span className="numeric font-mono">v{subject.latestVersion}</span>
      ),
    },
    {
      id: "versions",
      header: "Versions",
      align: "right",
      sortValue: (subject) => subject.versions.length,
      cell: (subject) => subject.versions.length,
    },
    {
      id: "compatibility",
      header: "Compatibility",
      align: "right",
      sortValue: (subject) => subject.compatibility,
      cell: (subject) => (
        <Pill tone={subject.compatibility === "NONE" ? "warn" : "idle"}>
          {subject.compatibility}
        </Pill>
      ),
    },
  ]

  return (
    <div className="space-y-5">
      <PageHeader
        title="Schema registry"
        description={`${rows.length} subjects registered`}
      />

      <InputGroup className="w-full max-w-sm">
        <InputGroupAddon>
          <SearchIcon />
        </InputGroupAddon>
        <InputGroupInput
          value={term}
          onChange={(event) => {
            const next = new URLSearchParams(params)
            if (event.target.value) {
              next.set("q", event.target.value)
            } else {
              next.delete("q")
            }
            setParams(next, { replace: true })
          }}
          placeholder="Search subjects…"
        />
      </InputGroup>

      <DataTable
        columns={columns}
        rows={rows}
        rowKey={(subject) => subject.subject}
        loading={isPending}
        defaultSort={{ id: "subject", direction: "asc" }}
        onRowClick={setSelected}
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
                  <h3 className="text-xs font-medium tracking-wider text-muted-foreground uppercase">
                    Schema
                  </h3>
                  <CopyButton value={selected.schema} label="Copy schema" />
                </div>
                <JsonBlock source={selected.schema} />

                <div className="space-y-2">
                  <h3 className="text-xs font-medium tracking-wider text-muted-foreground uppercase">
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
  )
}
