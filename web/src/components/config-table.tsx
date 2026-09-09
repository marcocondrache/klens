import { useMemo, useState } from "react";
import { EyeOffIcon, LockIcon, SearchIcon } from "lucide-react";

import { InputGroup, InputGroupAddon, InputGroupInput } from "@/components/ui/input-group";
import { Label } from "@/components/ui/label";
import { Switch } from "@/components/ui/switch";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { CopyButton } from "@/components/copy-button";
import { DataTable, type Column } from "@/components/data-table";
import { Pill } from "@/components/status";
import type { ConfigEntry } from "@/lib/api/types";
import { cn } from "@/lib/utils";

const SOURCE_LABEL: Record<ConfigEntry["source"], string> = {
  DYNAMIC_TOPIC_CONFIG: "topic override",
  DYNAMIC_BROKER_CONFIG: "broker override",
  STATIC_BROKER_CONFIG: "static",
  DEFAULT_CONFIG: "default",
};

export function ConfigTable({
  entries,
  loading = false,
  fill = false,
}: {
  entries: ConfigEntry[];
  loading?: boolean;
  fill?: boolean;
}) {
  const [term, setTerm] = useState("");
  const [onlyOverrides, setOnlyOverrides] = useState(false);

  const rows = useMemo(() => {
    const needle = term.trim().toLowerCase();

    return entries.filter((entry) => {
      if (onlyOverrides && entry.source === "DEFAULT_CONFIG") return false;
      if (!needle) return true;
      return (
        entry.name.toLowerCase().includes(needle) ||
        (entry.value ?? "").toLowerCase().includes(needle)
      );
    });
  }, [entries, term, onlyOverrides]);

  const columns: Array<Column<ConfigEntry>> = [
    {
      id: "name",
      header: "Key",
      sortValue: (entry) => entry.name,
      cell: (entry) => (
        <span className="flex items-center gap-1.5">
          <span className="font-mono text-sm">{entry.name}</span>
          {entry.readOnly ? (
            <Tooltip>
              <TooltipTrigger render={<LockIcon className="size-3 text-muted-foreground" />} />
              <TooltipContent>Read-only</TooltipContent>
            </Tooltip>
          ) : null}
        </span>
      ),
    },
    {
      id: "value",
      header: "Value",
      className: "whitespace-normal",
      cell: (entry) =>
        entry.sensitive ? (
          <span className="flex items-center gap-1.5 text-muted-foreground">
            <EyeOffIcon className="size-3" />
            <span className="text-xs">hidden</span>
          </span>
        ) : (
          <span className="flex items-center gap-1">
            <span className="numeric font-mono text-sm break-all">
              {entry.value === "" ? "—" : entry.value}
            </span>
            {entry.value ? <CopyButton value={entry.value} label="Copy value" /> : null}
          </span>
        ),
    },
    {
      id: "source",
      header: "Source",
      align: "right",
      sortValue: (entry) => entry.source,
      cell: (entry) => (
        <Pill tone={entry.source === "DEFAULT_CONFIG" ? "idle" : "brand"}>
          {SOURCE_LABEL[entry.source]}
        </Pill>
      ),
    },
  ];

  return (
    <div className={cn(fill ? "flex min-h-0 flex-1 flex-col gap-3" : "space-y-3")}>
      <div className="flex shrink-0 flex-wrap items-center justify-between gap-3">
        <InputGroup className="w-full max-w-xs">
          <InputGroupAddon>
            <SearchIcon />
          </InputGroupAddon>
          <InputGroupInput
            value={term}
            onChange={(event) => setTerm(event.target.value)}
            placeholder="Filter configuration…"
          />
        </InputGroup>

        <Label className="flex items-center gap-2 text-sm text-muted-foreground">
          <Switch
            size="sm"
            checked={onlyOverrides}
            onCheckedChange={(checked) => setOnlyOverrides(checked)}
          />
          Overrides only
        </Label>
      </div>

      <DataTable
        columns={columns}
        rows={rows}
        rowKey={(entry) => entry.name}
        loading={loading}
        pageSize={50}
        defaultSort={{ id: "name", direction: "asc" }}
        fill={fill}
      />
    </div>
  );
}
