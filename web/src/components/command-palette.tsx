import { useState } from "react";
import { FileJsonIcon, HardDriveIcon, LayersIcon, ServerIcon, UsersRoundIcon } from "lucide-react";
import { useLocation, useNavigate } from "react-router";

import {
  Command,
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator,
  CommandShortcut,
} from "@/components/ui/command";
import { StatusDot } from "@/components/status";
import { useClusters, useSearch } from "@/lib/api/queries";
import { clusterPath, useClusterName } from "@/lib/clusters";
import { SECTIONS, findSection } from "@/lib/sections";
import type { ClusterStatus } from "@/lib/api/types";

const RESULT_ICON = {
  TOPIC: LayersIcon,
  GROUP: UsersRoundIcon,
  NODE: HardDriveIcon,
  SUBJECT: FileJsonIcon,
};

const STATUS_TONE: Record<ClusterStatus, "ok" | "warn" | "error"> = {
  HEALTHY: "ok",
  DEGRADED: "warn",
  OFFLINE: "error",
};

export function CommandPalette({
  open,
  onOpenChange,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const cluster = useClusterName();
  const navigate = useNavigate();
  const location = useLocation();
  const [term, setTerm] = useState("");

  const { data: clusters = [] } = useClusters();
  const { data: results = [], isFetching } = useSearch(cluster, term);

  function changeOpen(next: boolean) {
    if (!next) setTerm("");
    onOpenChange(next);
  }

  function run(action: () => void) {
    changeOpen(false);
    action();
  }

  const topics = results.filter((result) => result.kind === "TOPIC");
  const groups = results.filter((result) => result.kind === "GROUP");
  const nodes = results.filter((result) => result.kind === "NODE");
  const subjects = results.filter((result) => result.kind === "SUBJECT");

  return (
    <CommandDialog
      open={open}
      onOpenChange={changeOpen}
      title="Search klens"
      description="Jump to a topic, consumer group, broker, schema or section"
      className="sm:max-w-xl"
    >
      <Command shouldFilter={false}>
        <CommandInput
          value={term}
          onValueChange={setTerm}
          placeholder="Search topics, groups, brokers and schemas…"
        />
        <CommandList className="max-h-[min(24rem,50vh)]">
          {term && !isFetching && results.length === 0 ? (
            <CommandEmpty>No matches in {cluster}.</CommandEmpty>
          ) : null}

          {topics.length ? (
            <CommandGroup heading="Topics">
              {topics.map((result) => {
                const Icon = RESULT_ICON[result.kind];
                return (
                  <CommandItem
                    key={result.href}
                    value={result.href}
                    onSelect={() => run(() => navigate(result.href))}
                    className="min-w-0"
                  >
                    <Icon className="text-muted-foreground" />
                    <span
                      className="min-w-0 flex-1 truncate font-mono text-sm"
                      title={result.label}
                    >
                      {result.label}
                    </span>
                    <CommandShortcut className="shrink-0 tracking-normal">
                      {result.detail}
                    </CommandShortcut>
                  </CommandItem>
                );
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
                  className="min-w-0"
                >
                  <UsersRoundIcon className="text-muted-foreground" />
                  <span className="min-w-0 flex-1 truncate font-mono text-sm" title={result.label}>
                    {result.label}
                  </span>
                  <CommandShortcut className="shrink-0 tracking-normal">
                    {result.detail}
                  </CommandShortcut>
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
                  className="min-w-0"
                >
                  <HardDriveIcon className="text-muted-foreground" />
                  <span className="min-w-0 flex-1 truncate">{result.label}</span>
                  <CommandShortcut className="shrink-0 font-mono tracking-normal">
                    {result.detail}
                  </CommandShortcut>
                </CommandItem>
              ))}
            </CommandGroup>
          ) : null}

          {subjects.length ? (
            <CommandGroup heading="Schemas">
              {subjects.map((result) => (
                <CommandItem
                  key={result.href}
                  value={result.href}
                  onSelect={() => run(() => navigate(result.href))}
                  className="min-w-0"
                >
                  <FileJsonIcon className="text-muted-foreground" />
                  <span className="min-w-0 flex-1 truncate font-mono text-sm" title={result.label}>
                    {result.label}
                  </span>
                  <CommandShortcut className="shrink-0 tracking-normal">
                    {result.detail}
                  </CommandShortcut>
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
                className="min-w-0"
                onSelect={() =>
                  run(() => {
                    const section = findSection(location.pathname.split("/")[3]);
                    navigate(
                      section
                        ? `/cluster/${entry.name}/${section.segment}`
                        : `/cluster/${entry.name}`,
                    );
                  })
                }
              >
                <ServerIcon className="text-muted-foreground" />
                <span className="min-w-0 flex-1 truncate">{entry.label}</span>
                <CommandShortcut className="flex shrink-0 items-center gap-1.5 tracking-normal">
                  <StatusDot tone={STATUS_TONE[entry.status]} />
                  {entry.brokerCount} {entry.brokerCount === 1 ? "broker" : "brokers"}
                </CommandShortcut>
              </CommandItem>
            ))}
          </CommandGroup>
        </CommandList>
      </Command>
    </CommandDialog>
  );
}
