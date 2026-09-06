import { useState } from "react"
import {
  FileJsonIcon,
  GaugeIcon,
  HardDriveIcon,
  LayersIcon,
  ServerIcon,
  ShieldCheckIcon,
  UsersRoundIcon,
} from "lucide-react"
import { useLocation, useNavigate } from "react-router"

import {
  Command,
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
} from "@/components/ui/command"
import { StatusDot } from "@/components/status"
import { useClusters, useSearch } from "@/lib/api/queries"
import { clusterPath, useClusterName } from "@/lib/clusters"
import type { ClusterStatus } from "@/lib/api/types"

const NAV = [
  { label: "Overview", segment: "", icon: GaugeIcon },
  { label: "Nodes", segment: "nodes", icon: HardDriveIcon },
  { label: "Topics", segment: "topics", icon: LayersIcon },
  { label: "Consumer groups", segment: "groups", icon: UsersRoundIcon },
  { label: "Schema registry", segment: "schemas", icon: FileJsonIcon },
  { label: "ACLs", segment: "acls", icon: ShieldCheckIcon },
]

const RESULT_ICON = {
  topic: LayersIcon,
  group: UsersRoundIcon,
  node: HardDriveIcon,
  subject: FileJsonIcon,
}

const STATUS_TONE: Record<ClusterStatus, "ok" | "warn" | "error"> = {
  healthy: "ok",
  degraded: "warn",
  offline: "error",
}

const SECTIONS = new Set(["topics", "groups", "nodes", "schemas", "acls"])

export function CommandPalette({
  open,
  onOpenChange,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
}) {
  const cluster = useClusterName()
  const navigate = useNavigate()
  const location = useLocation()
  const [term, setTerm] = useState("")

  const { data: clusters = [] } = useClusters()
  const { data: results = [], isFetching } = useSearch(cluster, term)

  function changeOpen(next: boolean) {
    if (!next) setTerm("")
    onOpenChange(next)
  }

  function run(action: () => void) {
    changeOpen(false)
    action()
  }

  const topics = results.filter((result) => result.kind === "topic")
  const groups = results.filter((result) => result.kind === "group")
  const nodes = results.filter((result) => result.kind === "node")

  return (
    <CommandDialog
      open={open}
      onOpenChange={changeOpen}
      title="Search klens"
      description="Jump to a topic, consumer group, node or section"
    >
      <Command shouldFilter={false}>
        <CommandInput
          value={term}
          onValueChange={setTerm}
          placeholder="Search topics, groups and nodes…"
        />
        <CommandList>
          {term && !isFetching && results.length === 0 ? (
            <CommandEmpty>No matches in {cluster}.</CommandEmpty>
          ) : null}

          {topics.length ? (
            <CommandGroup heading="Topics">
              {topics.map((result) => {
                const Icon = RESULT_ICON[result.kind]
                return (
                  <CommandItem
                    key={result.href}
                    value={result.href}
                    onSelect={() => run(() => navigate(result.href))}
                  >
                    <Icon className="text-muted-foreground" />
                    <span className="font-mono text-[0.8rem]">{result.label}</span>
                    <span className="ml-auto text-xs text-muted-foreground">{result.detail}</span>
                  </CommandItem>
                )
              })}
            </CommandGroup>
          ) : null}

          {groups.length ? (
            <CommandGroup heading="Consumer groups">
              {groups.map((result) => (
                <CommandItem
                  key={result.href}
                  value={result.href}
                  onSelect={() => run(() => navigate(result.href))}
                >
                  <UsersRoundIcon className="text-muted-foreground" />
                  <span className="font-mono text-[0.8rem]">{result.label}</span>
                  <span className="ml-auto text-xs text-muted-foreground">{result.detail}</span>
                </CommandItem>
              ))}
            </CommandGroup>
          ) : null}

          {nodes.length ? (
            <CommandGroup heading="Nodes">
              {nodes.map((result) => (
                <CommandItem
                  key={result.href}
                  value={result.href}
                  onSelect={() => run(() => navigate(result.href))}
                >
                  <HardDriveIcon className="text-muted-foreground" />
                  <span>{result.label}</span>
                  <span className="ml-auto font-mono text-xs text-muted-foreground">
                    {result.detail}
                  </span>
                </CommandItem>
              ))}
            </CommandGroup>
          ) : null}

          {results.length ? <CommandSeparator /> : null}

          <CommandGroup heading="Go to">
            {NAV.map((item) => (
              <CommandItem
                key={item.label}
                value={`nav:${item.label}`}
                onSelect={() => run(() => navigate(clusterPath(cluster, item.segment)))}
              >
                <item.icon className="text-muted-foreground" />
                <span>{item.label}</span>
              </CommandItem>
            ))}
          </CommandGroup>

          <CommandGroup heading="Switch cluster">
            {clusters.map((entry) => (
              <CommandItem
                key={entry.name}
                value={`cluster:${entry.name}`}
                onSelect={() =>
                  run(() => {
                    const [, , , section] = location.pathname.split("/")
                    navigate(
                      section && SECTIONS.has(section)
                        ? `/cluster/${entry.name}/${section}`
                        : `/cluster/${entry.name}`,
                    )
                  })
                }
              >
                <ServerIcon className="text-muted-foreground" />
                <span>{entry.label}</span>
                <span className="ml-auto flex items-center gap-1.5 text-xs text-muted-foreground">
                  <StatusDot tone={STATUS_TONE[entry.status]} />
                  {entry.brokerCount} brokers
                </span>
              </CommandItem>
            ))}
          </CommandGroup>
        </CommandList>
      </Command>
    </CommandDialog>
  )
}
