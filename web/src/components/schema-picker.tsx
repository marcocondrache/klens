import { useMemo, useState } from "react";
import { ChevronsUpDownIcon } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import type { SchemaSubject } from "@/lib/api/types";
import { cn } from "@/lib/utils";

export function SchemaPicker({
  subjects,
  topic,
  selectedSubject,
  onSelect,
}: {
  subjects: SchemaSubject[];
  topic: string;
  selectedSubject: string | null;
  onSelect: (subject: string | null) => void;
}) {
  const [open, setOpen] = useState(false);
  const preferred = `${topic}-value`;
  const options = useMemo(() => {
    return {
      pinned: subjects.filter((subject) => subject.subject === preferred),
      rest: subjects.filter((subject) => subject.subject !== preferred),
    };
  }, [preferred, subjects]);
  const all = [...options.pinned, ...options.rest];
  const selected = all.find((subject) => subject.subject === selectedSubject) ?? null;

  if (all.length === 0) {
    return null;
  }

  function choose(subject: string | null) {
    onSelect(subject);
    setOpen(false);
  }

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger
        render={
          <Button
            variant="outline"
            size="sm"
            className="max-w-64 min-w-0 gap-1 font-normal"
            aria-label="Decode value with schema"
          />
        }
      >
        <span className={cn("min-w-0 truncate", selected ? "font-mono" : "text-muted-foreground")}>
          {selected ? selected.subject : "Decode with schema…"}
        </span>
        {selected ? (
          <span className="shrink-0 text-muted-foreground">v{selected.latestVersion}</span>
        ) : null}
        <ChevronsUpDownIcon className="shrink-0 text-muted-foreground" />
      </PopoverTrigger>
      <PopoverContent align="start" side="bottom" className="w-80 gap-0 p-0">
        <Command>
          <CommandInput placeholder="Search subjects…" />
          <CommandList>
            <CommandEmpty>No matching subjects.</CommandEmpty>
            <CommandGroup>
              <CommandItem
                value="raw no schema"
                data-checked={!selected || undefined}
                onSelect={() => choose(null)}
              >
                Raw
              </CommandItem>
            </CommandGroup>
            {options.pinned.length > 0 ? (
              <CommandGroup heading="Suggested">
                {options.pinned.map((subject) => (
                  <SubjectItem
                    key={subject.subject}
                    subject={subject}
                    checked={selectedSubject === subject.subject}
                    onSelect={() => choose(subject.subject)}
                  />
                ))}
              </CommandGroup>
            ) : null}
            {options.rest.length > 0 ? (
              <CommandGroup heading={options.pinned.length > 0 ? "All" : undefined}>
                {options.rest.map((subject) => (
                  <SubjectItem
                    key={subject.subject}
                    subject={subject}
                    checked={selectedSubject === subject.subject}
                    onSelect={() => choose(subject.subject)}
                  />
                ))}
              </CommandGroup>
            ) : null}
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}

function SubjectItem({
  subject,
  checked,
  onSelect,
}: {
  subject: SchemaSubject;
  checked: boolean;
  onSelect: () => void;
}) {
  return (
    <CommandItem
      value={subject.subject}
      data-checked={checked || undefined}
      onSelect={onSelect}
      className="min-w-0"
    >
      <span className="min-w-0 flex-1 truncate font-mono text-sm" title={subject.subject}>
        {subject.subject}
      </span>
      <span className="shrink-0 text-xs text-muted-foreground">v{subject.latestVersion}</span>
    </CommandItem>
  );
}
