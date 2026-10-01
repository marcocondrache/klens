import { useState, type ReactNode } from "react";
import { Autocomplete } from "@base-ui/react/autocomplete";
import {
  FileJsonIcon,
  HardDriveIcon,
  LayersIcon,
  SearchIcon,
  ServerIcon,
  UsersRoundIcon,
  type LucideIcon,
} from "lucide-react";
import { useNavigate } from "@tanstack/react-router";

import { Dialog, DialogContent, DialogDescription, DialogTitle } from "@/components/ui/dialog";
import { InputGroup, InputGroupAddon } from "@/components/ui/input-group";
import { StatusDot } from "@/components/status";
import { useClusters, useSearch } from "@/lib/api/catalog";
import type { SearchHit } from "@/lib/api/types";
import { clusterTone, useClusterName } from "@/lib/clusters";
import { apiErrorMessage } from "@/lib/api/client";
import { useAccess } from "@/hooks/use-access";
import { clusterSectionTo, useSwitchCluster, visibleSections } from "@/lib/sections";
import { cn } from "@/lib/utils";

const RESULT_ICON = {
  TOPIC: LayersIcon,
  GROUP: UsersRoundIcon,
  NODE: HardDriveIcon,
  SUBJECT: FileJsonIcon,
};

const RESULT_HEADING: Record<SearchHit["kind"], string> = {
  TOPIC: "Topics",
  GROUP: "Consumer groups",
  NODE: "Brokers",
  SUBJECT: "Schemas",
};

interface PaletteItem {
  id: string;
  label: string;
  icon: LucideIcon;
  mono?: boolean;
  detail?: ReactNode;
  detailMono?: boolean;
  select: () => void;
}

interface PaletteGroup {
  value: string;
  items: PaletteItem[];
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
  const switchCluster = useSwitchCluster();
  const { can } = useAccess();
  const sections = visibleSections((privilege) => can(cluster, privilege));
  const [term, setTerm] = useState("");
  const searching = term.trim().length > 0;

  const { data: clusters = [] } = useClusters();
  const { data: results = [], isPending, isError, error } = useSearch(cluster, term);
  const showNavigation = !searching || isPending;
  const hits = searching && !isPending ? results : [];
  const kinds = [...new Set(hits.map((hit) => hit.kind))];

  const groups: PaletteGroup[] = kinds.map((kind) => ({
    value: RESULT_HEADING[kind],
    items: hits
      .filter((hit) => hit.kind === kind)
      .map((hit) => ({
        id: hit.href,
        label: hit.label,
        icon: RESULT_ICON[kind],
        mono: kind !== "NODE",
        detail: hit.detail,
        detailMono: kind === "NODE",
        select: () => void navigate({ href: hit.href }),
      })),
  }));

  if (showNavigation) {
    groups.push(
      {
        value: "Go to",
        items: sections.map((section) => ({
          id: `nav:${section.segment}`,
          label: section.label,
          icon: section.icon,
          select: () =>
            void navigate({ to: clusterSectionTo(section.segment), params: { cluster } }),
        })),
      },
      {
        value: "Switch cluster",
        items: clusters.map((entry) => ({
          id: `cluster:${entry.cluster}`,
          label: entry.cluster,
          icon: ServerIcon,
          detail: <StatusDot tone={clusterTone(entry)} />,
          select: () => switchCluster(entry.cluster),
        })),
      },
    );
  }

  function changeOpen(next: boolean) {
    if (!next) setTerm("");
    onOpenChange(next);
  }

  function run(item: PaletteItem) {
    changeOpen(false);
    item.select();
  }

  return (
    <Dialog open={open} onOpenChange={changeOpen}>
      <DialogContent
        showCloseButton={false}
        className="top-1/3 translate-y-0 gap-0 overflow-hidden p-1 sm:max-w-xl"
      >
        <DialogTitle className="sr-only">Search klens</DialogTitle>
        <DialogDescription className="sr-only">
          Jump to a topic, consumer group, broker, schema or section
        </DialogDescription>
        <Autocomplete.Root
          open
          inline
          mode="none"
          items={groups}
          value={term}
          onValueChange={setTerm}
          autoHighlight="always"
          keepHighlight
        >
          <div className="p-1 pb-0">
            <InputGroup className="h-8 border-input/30 bg-input/30 shadow-none">
              <Autocomplete.Input
                className="w-full text-sm outline-hidden"
                placeholder="Search topics, groups, brokers and schemas…"
              />
              <InputGroupAddon className="pl-2">
                <SearchIcon className="size-4 shrink-0 opacity-50" />
              </InputGroupAddon>
            </InputGroup>
          </div>
          <Autocomplete.Empty className="py-6 text-center text-sm empty:hidden">
            {isError
              ? apiErrorMessage(error, "Search failed.")
              : searching
                ? `No matches in ${cluster}.`
                : null}
          </Autocomplete.Empty>
          <Autocomplete.List className="no-scrollbar max-h-[min(24rem,50vh)] scroll-py-1 overflow-x-hidden overflow-y-auto outline-none">
            {(group: PaletteGroup) => (
              <Autocomplete.Group key={group.value} items={group.items} className="p-1">
                <Autocomplete.GroupLabel className="px-2 py-1.5 text-xs font-medium text-muted-foreground">
                  {group.value}
                </Autocomplete.GroupLabel>
                <Autocomplete.Collection>
                  {(item: PaletteItem) => (
                    <Autocomplete.Item
                      key={item.id}
                      value={item}
                      onClick={() => run(item)}
                      className="group/item flex min-w-0 cursor-default items-center gap-2 rounded-lg px-2 py-1.5 text-sm outline-hidden select-none data-highlighted:bg-muted"
                    >
                      <item.icon className="size-4 shrink-0 text-muted-foreground" />
                      <span
                        className={cn("min-w-0 flex-1 truncate", item.mono && "font-mono")}
                        title={item.mono ? item.label : undefined}
                      >
                        {item.label}
                      </span>
                      {item.detail != null ? (
                        <span
                          className={cn(
                            "flex shrink-0 items-center text-xs text-muted-foreground group-data-highlighted/item:text-foreground",
                            item.detailMono && "font-mono",
                          )}
                        >
                          {item.detail}
                        </span>
                      ) : null}
                    </Autocomplete.Item>
                  )}
                </Autocomplete.Collection>
              </Autocomplete.Group>
            )}
          </Autocomplete.List>
        </Autocomplete.Root>
      </DialogContent>
    </Dialog>
  );
}
