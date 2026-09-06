import { useState } from "react"
import { ClockIcon, DownloadIcon, SearchIcon } from "lucide-react"

import { Button } from "@/components/ui/button"
import {
  Empty,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "@/components/ui/empty"
import {
  InputGroup,
  InputGroupAddon,
  InputGroupInput,
} from "@/components/ui/input-group"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet"
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip"
import { CopyButton } from "@/components/copy-button"
import { DataTable, type Column } from "@/components/data-table"
import { JsonBlock } from "@/components/json-block"
import { Pill } from "@/components/status"
import { useRecords } from "@/lib/api/queries"
import { formatBytes, formatRelative, formatTimestamp } from "@/lib/format"
import type { RecordOrder, Topic, TopicRecord } from "@/lib/api/types"

const LIMITS = ["25", "50", "100"]

function preview(value: string | null) {
  if (!value) return "—"
  return value.replace(/\s+/g, " ").trim()
}

export function RecordBrowser({ cluster, topic }: { cluster: string; topic: Topic }) {
  const [partition, setPartition] = useState<string>("all")
  const [term, setTerm] = useState("")
  const [limit, setLimit] = useState("50")
  const [order, setOrder] = useState<RecordOrder>("NEWEST")
  const [selected, setSelected] = useState<TopicRecord | null>(null)

  const { data: records = [], isFetching } = useRecords({
    cluster,
    topic: topic.name,
    partition: partition === "all" ? null : Number(partition),
    search: term,
    limit: Number(limit),
    order,
  })

  const columns: Array<Column<TopicRecord>> = [
    {
      id: "partition",
      header: "Part",
      align: "right",
      sortValue: (record) => record.partition,
      cell: (record) => <span className="numeric font-mono text-xs">{record.partition}</span>,
      headerClassName: "w-16",
    },
    {
      id: "offset",
      header: "Offset",
      align: "right",
      sortValue: (record) => record.offset,
      cell: (record) => <span className="numeric font-mono text-xs">{record.offset}</span>,
      headerClassName: "w-28",
    },
    {
      id: "key",
      header: "Key",
      sortValue: (record) => record.key ?? "",
      cell: (record) => (
        <span className="block max-w-40 truncate font-mono text-xs text-brand">
          {record.key ?? "null"}
        </span>
      ),
    },
    {
      id: "value",
      header: "Value",
      cell: (record) => (
        <span className="block max-w-md truncate font-mono text-xs text-muted-foreground lg:max-w-xl">
          {preview(record.value)}
        </span>
      ),
    },
    {
      id: "size",
      header: "Size",
      align: "right",
      sortValue: (record) => record.sizeBytes,
      cell: (record) => (
        <span className="text-xs text-muted-foreground">{formatBytes(record.sizeBytes)}</span>
      ),
    },
    {
      id: "timestamp",
      header: "Timestamp",
      align: "right",
      sortValue: (record) => record.timestamp,
      cell: (record) => (
        <Tooltip>
          <TooltipTrigger
            render={
              <span className="numeric cursor-default text-xs whitespace-nowrap text-muted-foreground" />
            }
          >
            {formatRelative(record.timestamp)}
          </TooltipTrigger>
          <TooltipContent>{formatTimestamp(record.timestamp)}</TooltipContent>
        </Tooltip>
      ),
    },
  ]

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap items-center gap-3">
        <InputGroup className="w-full max-w-sm">
          <InputGroupAddon>
            <SearchIcon />
          </InputGroupAddon>
          <InputGroupInput
            value={term}
            onChange={(event) => setTerm(event.target.value)}
            placeholder="Search key or value…"
          />
        </InputGroup>

        <Select value={partition} onValueChange={(value) => setPartition(String(value))}>
          <SelectTrigger size="sm" className="w-36">
            <SelectValue placeholder="Partition" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All partitions</SelectItem>
            {topic.partitions.map((part) => (
              <SelectItem key={part.id} value={String(part.id)}>
                Partition {part.id}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        <Select value={order} onValueChange={(value) => setOrder(value as RecordOrder)}>
          <SelectTrigger size="sm" className="w-36">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="NEWEST">Newest first</SelectItem>
            <SelectItem value="OLDEST">Oldest first</SelectItem>
          </SelectContent>
        </Select>

        <Select value={limit} onValueChange={(value) => setLimit(String(value))}>
          <SelectTrigger size="sm" className="w-28">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {LIMITS.map((value) => (
              <SelectItem key={value} value={value}>
                {value} rows
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        <span className="ml-auto text-xs text-muted-foreground">
          {isFetching ? "polling…" : `${records.length} records`}
        </span>
      </div>

      <DataTable
        columns={columns}
        rows={records}
        rowKey={(record) => `${record.partition}-${record.offset}`}
        loading={isFetching && records.length === 0}
        pageSize={Number(limit)}
        onRowClick={setSelected}
        emptyState={
          <Empty className="py-10">
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <ClockIcon />
              </EmptyMedia>
              <EmptyTitle>No records</EmptyTitle>
              <EmptyDescription>
                {term
                  ? "Nothing matched your search in the scanned offset window."
                  : "This topic has no records in the selected range."}
              </EmptyDescription>
            </EmptyHeader>
          </Empty>
        }
      />

      <Sheet open={selected !== null} onOpenChange={(open) => !open && setSelected(null)}>
        <SheetContent side="right" className="w-full gap-0 sm:max-w-xl">
          {selected ? (
            <>
              <SheetHeader className="border-b">
                <SheetTitle className="font-mono text-sm">
                  {topic.name}[{selected.partition}]@{selected.offset}
                </SheetTitle>
                <SheetDescription>
                  {formatTimestamp(selected.timestamp)} · {formatBytes(selected.sizeBytes)} ·{" "}
                  {selected.compression.toLowerCase()}
                </SheetDescription>
              </SheetHeader>

              <div className="flex-1 space-y-5 overflow-y-auto p-4">
                <section className="space-y-2">
                  <div className="flex items-center justify-between">
                    <h3 className="text-xs font-medium tracking-wider text-muted-foreground uppercase">
                      Key
                    </h3>
                    {selected.key ? <CopyButton value={selected.key} label="Copy key" /> : null}
                  </div>
                  <p className="rounded-lg border bg-muted/30 p-3 font-mono text-xs break-all">
                    {selected.key ?? "null"}
                  </p>
                </section>

                <section className="space-y-2">
                  <div className="flex items-center justify-between">
                    <h3 className="text-xs font-medium tracking-wider text-muted-foreground uppercase">
                      Value
                    </h3>
                    <div className="flex items-center gap-1">
                      {selected.value ? (
                        <CopyButton value={selected.value} label="Copy value" />
                      ) : null}
                      <Button
                        variant="ghost"
                        size="icon-xs"
                        aria-label="Download value"
                        className="text-muted-foreground"
                        onClick={() => {
                          const blob = new Blob([selected.value ?? ""], { type: "application/json" })
                          const url = URL.createObjectURL(blob)
                          const anchor = document.createElement("a")
                          anchor.href = url
                          anchor.download = `${topic.name}-${selected.partition}-${selected.offset}.json`
                          anchor.click()
                          URL.revokeObjectURL(url)
                        }}
                      >
                        <DownloadIcon />
                      </Button>
                    </div>
                  </div>
                  <JsonBlock source={selected.value ?? "null"} className="max-h-96" />
                </section>

                <section className="space-y-2">
                  <h3 className="text-xs font-medium tracking-wider text-muted-foreground uppercase">
                    Headers
                  </h3>
                  {selected.headers.length === 0 ? (
                    <p className="text-xs text-muted-foreground">No headers.</p>
                  ) : (
                    <div className="divide-y overflow-hidden rounded-lg border">
                      {selected.headers.map((header) => (
                        <div
                          key={header.key}
                          className="flex items-start justify-between gap-3 px-3 py-2"
                        >
                          <span className="font-mono text-xs text-brand">{header.key}</span>
                          <span className="max-w-[60%] font-mono text-xs break-all text-muted-foreground">
                            {header.value}
                          </span>
                        </div>
                      ))}
                    </div>
                  )}
                </section>

                <section className="space-y-2">
                  <h3 className="text-xs font-medium tracking-wider text-muted-foreground uppercase">
                    Metadata
                  </h3>
                  <div className="grid grid-cols-2 gap-2 text-xs">
                    <Meta label="Partition" value={String(selected.partition)} />
                    <Meta label="Offset" value={String(selected.offset)} />
                    <Meta label="Timestamp" value={String(selected.timestamp)} />
                    <Meta label="Age" value={formatRelative(selected.timestamp)} />
                    <Meta label="Size" value={formatBytes(selected.sizeBytes)} />
                    <Meta
                      label="Compression"
                      value={<Pill>{selected.compression.toLowerCase()}</Pill>}
                    />
                  </div>
                </section>
              </div>
            </>
          ) : null}
        </SheetContent>
      </Sheet>
    </div>
  )
}

function Meta({ label, value }: { label: string; value: React.ReactNode }) {
  return (
    <div className="rounded-lg border bg-muted/20 px-3 py-2">
      <p className="text-[0.7rem] tracking-wider text-muted-foreground uppercase">{label}</p>
      <p className="numeric mt-0.5 font-mono text-xs">{value}</p>
    </div>
  )
}
