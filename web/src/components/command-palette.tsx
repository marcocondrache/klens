import { useState } from "react"
import {
  FileJsonIcon,
  HardDriveIcon,
  LayersIcon,
  ServerIcon,
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
import { SECTIONS, findSection } from "@/lib/sections"
import type { ClusterStatus } from "@/lib/api/types"

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
      description="Jump to a topic, consumer group, broker or section"
    >
      <Command shouldFilter={false}>
        <CommandInput
          value={term}
          onValueChange={setTerm}
          placeholder="Search topics, groups and brokers…"
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
            <CommandGroup heading="Brokers">
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
            {SECTIONS.map((section) => (
              <CommandItem
                key={section.segment}
                value={`nav:${section.label}`}
                onSelect={() => run(() => navigate(clusterPath(cluster, section.segment)))}
              >
                <section.icon className="text-muted-foreground" />
                <span>{section.label}</span>
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
                    const section = findSection(location.pathname.split("/")[3])
                    navigate(
                      section
                        ? `/cluster/${entry.name}/${section.segment}`
                        : `/cluster/${entry.name}`,
                    )
                  })
                }
              >
                <ServerIcon className="text-muted-foreground" />
                <span>{entry.label}</span>
                <span className="ml-auto flex items-center gap-1.5 text-xs text-muted-foreground">
                  <StatusDot tone={STATUS_TONE[entry.status]} />
                  {entry.brokerCount} {entry.brokerCount === 1 ? "broker" : "brokers"}
                </span>
              </CommandItem>
            ))}
          </CommandGroup>
        </CommandList>
      </Command>
    </CommandDialog>
  )
}
