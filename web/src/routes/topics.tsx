import { useMemo } from "react"
import { AlertTriangleIcon, SearchIcon } from "lucide-react"
import { useNavigate, useSearchParams } from "react-router"

import {
  InputGroup,
  InputGroupAddon,
  InputGroupInput,
} from "@/components/ui/input-group"
import { Label } from "@/components/ui/label"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import { Switch } from "@/components/ui/switch"
import { DataTable, type Column } from "@/components/data-table"
import { PageHeader } from "@/components/page-header"
import { Pill } from "@/components/status"
import { useTopics } from "@/lib/api/queries"
import { clusterPath, useClusterName } from "@/lib/clusters"
import {
  formatBytes,
  formatCleanupPolicy,
  formatCount,
  formatDuration,
  formatNumber,
  formatRate,
  isCompactCleanup,
} from "@/lib/format"
import type { Topic } from "@/lib/api/types"

export function TopicsPage() {
  const cluster = useClusterName()
  const navigate = useNavigate()
  const [params, setParams] = useSearchParams()

  const term = params.get("q") ?? ""
  const showInternal = params.get("internal") === "1"
  const policy = params.get("policy") ?? "all"

  const { data: topics = [], isPending } = useTopics(cluster)

  function update(key: string, value: string | null) {
    const next = new URLSearchParams(params)
    if (value === null || value === "" || value === "all") {
      next.delete(key)
    } else {
      next.set(key, value)
    }
    setParams(next, { replace: true })
  }

  const rows = useMemo(() => {
    const needle = term.trim().toLowerCase()

    return topics.filter((topic) => {
      if (!showInternal && topic.internal) return false
      if (policy !== "all" && !formatCleanupPolicy(topic.cleanupPolicy).includes(policy)) return false
      if (needle && !topic.name.toLowerCase().includes(needle)) return false
      return true
    })
  }, [topics, term, showInternal, policy])

  const columns: Array<Column<Topic>> = [
    {
      id: "name",
      header: "Topic",
      sortValue: (topic) => topic.name,
      cell: (topic) => (
        <span className="flex items-center gap-2">
          <span className="font-mono text-[0.8rem]">{topic.name}</span>
          {topic.internal ? <Pill>internal</Pill> : null}
          {topic.underReplicated ? (
            <Pill tone="warn">
              <AlertTriangleIcon className="size-3" />
              under-replicated
            </Pill>
          ) : null}
        </span>
      ),
    },
    {
      id: "partitions",
      header: "Parts",
      align: "right",
      sortValue: (topic) => topic.partitions.length,
      cell: (topic) => topic.partitions.length,
    },
    {
      id: "replication",
      header: "RF",
      align: "right",
      sortValue: (topic) => topic.replicationFactor,
      cell: (topic) => topic.replicationFactor,
    },
    {
      id: "messages",
      header: "Messages",
      align: "right",
      sortValue: (topic) => topic.messageCount,
      cell: (topic) => formatNumber(topic.messageCount),
    },
    {
      id: "size",
      header: "Size",
      align: "right",
      sortValue: (topic) => topic.sizeBytes,
      cell: (topic) => formatBytes(topic.sizeBytes),
    },
    {
      id: "rate",
      header: "Msg/s",
      align: "right",
      sortValue: (topic) => topic.messagesPerSec,
      cell: (topic) => formatCount(topic.messagesPerSec),
    },
    {
      id: "in",
      header: "Bytes in",
      align: "right",
      sortValue: (topic) => topic.bytesInPerSec,
      cell: (topic) => formatRate(topic.bytesInPerSec),
    },
    {
      id: "retention",
      header: "Retention",
      align: "right",
      sortValue: (topic) => topic.retentionMs,
      cell: (topic) => (
        <span className="text-muted-foreground">{formatDuration(topic.retentionMs)}</span>
      ),
    },
    {
      id: "policy",
      header: "Policy",
      align: "right",
      sortValue: (topic) => topic.cleanupPolicy,
      cell: (topic) => (
        <Pill tone={isCompactCleanup(topic.cleanupPolicy) ? "brand" : "idle"}>
          {formatCleanupPolicy(topic.cleanupPolicy)}
        </Pill>
      ),
    },
    {
      id: "groups",
      header: "Groups",
      align: "right",
      sortValue: (topic) => topic.consumerGroups.length,
      cell: (topic) =>
        topic.consumerGroups.length ? (
          topic.consumerGroups.length
        ) : (
          <span className="text-muted-foreground">—</span>
        ),
    },
  ]

  return (
    <div className="space-y-5">
      <PageHeader
        title="Topics"
        description={`${rows.length} of ${topics.length} topics`}
      />

      <div className="flex flex-wrap items-center gap-3">
        <InputGroup className="w-full max-w-sm">
          <InputGroupAddon>
            <SearchIcon />
          </InputGroupAddon>
          <InputGroupInput
            value={term}
            onChange={(event) => update("q", event.target.value)}
            placeholder="Search topics…"
          />
        </InputGroup>

        <Select value={policy} onValueChange={(value) => update("policy", String(value))}>
          <SelectTrigger size="sm" className="w-40">
            <SelectValue placeholder="Cleanup policy" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All policies</SelectItem>
            <SelectItem value="delete">delete</SelectItem>
            <SelectItem value="compact">compact</SelectItem>
          </SelectContent>
        </Select>

        <Label className="flex items-center gap-2 text-xs text-muted-foreground">
          <Switch
            size="sm"
            checked={showInternal}
            onCheckedChange={(checked) => update("internal", checked ? "1" : null)}
          />
          Show internal
        </Label>
      </div>

      <DataTable
        columns={columns}
        rows={rows}
        rowKey={(topic) => topic.name}
        loading={isPending}
        defaultSort={{ id: "name", direction: "asc" }}
        onRowClick={(topic) => navigate(clusterPath(cluster, "topics", topic.name))}
      />
    </div>
  )
}
