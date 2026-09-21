import { useState } from "react";
import { FileJsonIcon, HardDriveIcon, LayersIcon, ServerIcon, UsersRoundIcon } from "lucide-react";
import { useNavigate } from "@tanstack/react-router";

import {
  Command,
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandShortcut,
} from "@/components/ui/command";
import { StatusDot } from "@/components/status";
import { useClusters, useSearch } from "@/lib/api/catalog";
import type { SearchHit } from "@/lib/api/types";
import { clusterTone, useClusterName } from "@/lib/clusters";
import { apiErrorMessage } from "@/lib/api/client";
import { useAccess } from "@/hooks/use-access";
import { clusterSectionTo, useActiveSection, visibleSections } from "@/lib/sections";
import { cn } from "@/lib/utils";

const RESULT_ICON = {
  TOPIC: LayersIcon,
  GROUP: UsersRoundIcon,
  NODE: HardDriveIcon,
  SUBJECT: FileJsonIcon,
};

const RESULT_GROUPS: Array<{ kind: SearchHit["kind"]; heading: string }> = [
  { kind: "TOPIC", heading: "Topics" },
  { kind: "GROUP", heading: "Consumer groups" },
  { kind: "NODE", heading: "Brokers" },
  { kind: "SUBJECT", heading: "Schemas" },
];

export function CommandPalette({
  open,
  onOpenChange,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const cluster = useClusterName();
  const navigate = useNavigate();
  const section = useActiveSection();
  const { can } = useAccess();
  const sections = visibleSections(can(cluster, "ACLS"));
  const [term, setTerm] = useState("");
  const searching = term.trim().length > 0;

  function goHref(href: string) {
    void navigate({ href });
  }

  const { data: clusters = [] } = useClusters();
  const { data: results = [], isFetching, isError, error } = useSearch(cluster, term);

  function changeOpen(next: boolean) {
    if (!next) setTerm("");
    onOpenChange(next);
  }

  function run(action: () => void) {
    changeOpen(false);
    action();
  }

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
          {searching && isError ? (
            <CommandEmpty>{apiErrorMessage(error, "Search failed.")}</CommandEmpty>
          ) : null}

          {searching && !isFetching && !isError && results.length === 0 ? (
            <CommandEmpty>No matches in {cluster}.</CommandEmpty>
          ) : null}

          {RESULT_GROUPS.map(({ kind, heading }) => {
            const items = results.filter((result) => result.kind === kind);
            if (items.length === 0) return null;

            const Icon = RESULT_ICON[kind];
            const broker = kind === "NODE";

            return (
              <CommandGroup key={kind} heading={heading}>
                {items.map((result) => (
                  <CommandItem
                    key={result.href}
                    value={result.href}
                    onSelect={() => run(() => goHref(result.href))}
                    className="min-w-0"
                  >
                    <Icon className="text-muted-foreground" />
                    <span
                      className={cn("min-w-0 flex-1 truncate", !broker && "font-mono text-sm")}
                      title={broker ? undefined : result.label}
                    >
                      {result.label}
                    </span>
                    <CommandShortcut
                      className={cn("shrink-0 tracking-normal", broker && "font-mono")}
                    >
                      {result.detail}
                    </CommandShortcut>
                  </CommandItem>
                ))}
              </CommandGroup>
            );
          })}

          {searching ? null : (
            <CommandGroup heading="Go to">
              {sections.map((section) => (
                <CommandItem
                  key={section.segment}
                  value={`nav:${section.label}`}
                  onSelect={() =>
                    run(() => {
                      void navigate({
                        to: clusterSectionTo(section.segment),
                        params: { cluster },
                      });
                    })
                  }
                >
                  <section.icon className="text-muted-foreground" />
                  <span>{section.label}</span>
                </CommandItem>
              ))}
            </CommandGroup>
          )}

          {searching ? null : (
            <CommandGroup heading="Switch cluster">
              {clusters.map((entry) => (
                <CommandItem
                  key={entry.cluster}
                  value={`cluster:${entry.cluster}`}
                  className="min-w-0"
                  onSelect={() =>
                    run(() => {
                      void navigate({
                        to: section ? clusterSectionTo(section.segment) : "/cluster/$cluster",
                        params: { cluster: entry.cluster },
                      });
                    })
                  }
                >
                  <ServerIcon className="text-muted-foreground" />
                  <span className="min-w-0 flex-1 truncate">{entry.cluster}</span>
                  <CommandShortcut className="flex shrink-0 items-center gap-1.5 tracking-normal">
                    <StatusDot tone={clusterTone(entry)} />
                  </CommandShortcut>
                </CommandItem>
              ))}
            </CommandGroup>
          )}
        </CommandList>
      </Command>
    </CommandDialog>
  );
}
