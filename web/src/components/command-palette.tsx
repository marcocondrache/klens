import { useLayoutEffect, useState } from "react";
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
import { clusterTone, useClusterName } from "@/lib/clusters";
import { useAccess } from "@/hooks/use-access";
import { clusterSectionTo, useActiveSection, visibleSections } from "@/lib/sections";

const RESULT_ICON = {
  TOPIC: LayersIcon,
  GROUP: UsersRoundIcon,
  NODE: HardDriveIcon,
  SUBJECT: FileJsonIcon,
};

function matchesQuery(label: string, term: string) {
  const query = term.trim().toLowerCase();
  return query.length === 0 || label.toLowerCase().includes(query);
}

function firstValue(...values: Array<string | undefined>) {
  return values.find((value) => value != null && value.length > 0) ?? "";
}

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
  const [selected, setSelected] = useState("");

  function goHref(href: string) {
    void navigate({ href });
  }

  const { data: clusters = [] } = useClusters();
  const { data: results = [], isFetching, isError, error } = useSearch(cluster, term);

  function changeOpen(next: boolean) {
    if (!next) {
      setTerm("");
      setSelected("");
    }
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
  const goto =
    results.length > 0 ? [] : sections.filter((section) => matchesQuery(section.label, term));
  const clusterHits =
    results.length > 0 ? [] : clusters.filter((entry) => matchesQuery(entry.cluster, term));
  const highlight = firstValue(
    topics[0]?.href,
    groups[0]?.href,
    nodes[0]?.href,
    subjects[0]?.href,
    goto[0] ? `nav:${goto[0].label}` : undefined,
    clusterHits[0] ? `cluster:${clusterHits[0].cluster}` : undefined,
  );
  const searching = term.trim().length > 0;
  const showEmpty =
    searching &&
    !isFetching &&
    !isError &&
    results.length === 0 &&
    goto.length === 0 &&
    clusterHits.length === 0;

  useLayoutEffect(() => {
    setSelected(highlight);
  }, [highlight]);

  return (
    <CommandDialog
      open={open}
      onOpenChange={changeOpen}
      title="Search klens"
      description="Jump to a topic, consumer group, broker, schema or section"
      className="sm:max-w-xl"
    >
      <Command shouldFilter={false} value={selected} onValueChange={setSelected}>
        <CommandInput
          value={term}
          onValueChange={setTerm}
          placeholder="Search topics, groups, brokers and schemas…"
        />
        <CommandList className="max-h-[min(24rem,50vh)]">
          {searching && isError ? (
            <p className="px-2 py-3 text-center text-sm text-destructive">
              {error instanceof Error ? error.message : "Search failed."}
            </p>
          ) : null}

          {showEmpty ? <CommandEmpty>No matches in {cluster}.</CommandEmpty> : null}

          {topics.length ? (
            <CommandGroup heading="Topics">
              {topics.map((result) => {
                const Icon = RESULT_ICON[result.kind];
                return (
                  <CommandItem
                    key={result.href}
                    value={result.href}
                    onSelect={() => run(() => goHref(result.href))}
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
                  onSelect={() => run(() => goHref(result.href))}
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
                  onSelect={() => run(() => goHref(result.href))}
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
                  onSelect={() => run(() => goHref(result.href))}
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

          {goto.length ? (
            <CommandGroup heading="Go to">
              {goto.map((section) => (
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
          ) : null}

          {clusterHits.length ? (
            <CommandGroup heading="Switch cluster">
              {clusterHits.map((entry) => (
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
          ) : null}
        </CommandList>
      </Command>
    </CommandDialog>
  );
}
