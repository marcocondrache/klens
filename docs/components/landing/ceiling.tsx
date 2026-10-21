'use client';

import { type ReactNode, useState } from 'react';
import { cn } from '@/lib/cn';

type Privilege =
  | 'records'
  | 'schema_text'
  | 'acls'
  | 'create_topics'
  | 'produce'
  | 'register_schemas';

const readPrivileges: Privilege[] = ['records', 'schema_text', 'acls'];
const writePrivileges: Privilege[] = ['create_topics', 'produce', 'register_schemas'];

const readTools: { name: string; needs: Privilege }[] = [
  { name: 'klens_records_read', needs: 'records' },
  { name: 'klens_record_get', needs: 'records' },
  { name: 'klens_schema_get', needs: 'schema_text' },
  { name: 'klens_acls_list', needs: 'acls' },
];

const writeTools: { name: string; needs: Privilege }[] = [
  { name: 'klens_topic_create', needs: 'create_topics' },
  { name: 'klens_record_produce', needs: 'produce' },
  { name: 'klens_schema_register', needs: 'register_schemas' },
];

/** Catalog tools need no privilege, so every agent receives them whatever the ceiling grants. */
const CATALOG_TOOLS = 9;
const TOTAL_TOOLS = CATALOG_TOOLS + readTools.length + writeTools.length;

const keyColor = 'text-[#116329] dark:text-[#85e89d]';
const valueColor = 'text-[#032f62] dark:text-[#9ecbff]';
const commentColor = 'text-[#59636e] dark:text-[#8b949e]';

export function McpCeiling() {
  const [granted, setGranted] = useState<ReadonlySet<Privilege>>(
    () => new Set(['schema_text', 'acls']),
  );

  const toggle = (privilege: Privilege) => {
    const next = new Set(granted);
    if (!next.delete(privilege)) next.add(privilege);
    setGranted(next);
  };

  const served =
    CATALOG_TOOLS + [...readTools, ...writeTools].filter((tool) => granted.has(tool.needs)).length;

  return (
    <div className="@container overflow-hidden rounded-xl border bg-fd-popover text-fd-popover-foreground">
      <div className="grid @xl:grid-cols-[minmax(0,5fr)_minmax(0,6fr)]">
        <div className="flex flex-col @max-xl:border-b @xl:border-e">
          <PaneHeader title="config.yaml">
            <span className="text-fd-muted-foreground">Select a privilege to toggle it</span>
          </PaneHeader>
          <pre className="p-2 font-mono text-xs leading-6">
            <code className="grid">
              <YamlLine indent={0} name="mcp" />
              <YamlLine indent={1} name="bind" value="127.0.0.1:8090" />
              <YamlLine indent={1} name="clusters" value="[production, staging]" />
              <YamlLine indent={1} name="privileges" />
              {readPrivileges.map((privilege) => (
                <PrivilegeLine
                  key={privilege}
                  privilege={privilege}
                  granted={granted.has(privilege)}
                  onToggle={toggle}
                />
              ))}
              <span className="h-3" />
              {writePrivileges.map((privilege) => (
                <PrivilegeLine
                  key={privilege}
                  privilege={privilege}
                  granted={granted.has(privilege)}
                  onToggle={toggle}
                />
              ))}
            </code>
          </pre>
        </div>

        <div className="flex flex-col">
          <PaneHeader title="tools/list">
            <span className="text-fd-muted-foreground tabular-nums">
              {served} of {TOTAL_TOOLS} tools
            </span>
          </PaneHeader>
          <ul className="flex flex-col py-2 text-xs">
            {readTools.map((tool) => (
              <ToolRow key={tool.name} {...tool} served={granted.has(tool.needs)} />
            ))}
            <li aria-hidden className="mx-4 my-1.5 border-t border-fd-border/60" />
            {writeTools.map((tool) => (
              <ToolRow key={tool.name} {...tool} served={granted.has(tool.needs)} />
            ))}
          </ul>
          <p className="mt-auto border-t px-4 py-2.5 text-xs text-fd-muted-foreground">
            And {CATALOG_TOOLS} catalog tools that need no privilege.
          </p>
        </div>
      </div>
    </div>
  );
}

function PaneHeader({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="flex items-center gap-3 border-b px-4 py-2.5 text-xs">
      <span className="font-mono font-medium">{title}</span>
      <span className="ms-auto">{children}</span>
    </div>
  );
}

function YamlLine({ indent, name, value }: { indent: number; name: string; value?: string }) {
  return (
    <span className="px-2 whitespace-pre">
      {'  '.repeat(indent)}
      <span className={keyColor}>{name}</span>:
      {value && <span className={valueColor}> {value}</span>}
    </span>
  );
}

function PrivilegeLine({
  privilege,
  granted,
  onToggle,
}: {
  privilege: Privilege;
  granted: boolean;
  onToggle: (privilege: Privilege) => void;
}) {
  return (
    <button
      type="button"
      aria-pressed={granted}
      onClick={() => onToggle(privilege)}
      className={cn(
        'rounded-md px-2 text-start whitespace-pre transition-colors hover:bg-fd-accent',
        'focus-visible:outline-2 focus-visible:outline-brand',
      )}
    >
      {'    '}
      {granted ? (
        <>
          - <span className={valueColor}>{privilege}</span>
        </>
      ) : (
        <span className={commentColor}># - {privilege}</span>
      )}
    </button>
  );
}

function ToolRow({ name, needs, served }: { name: string; needs: Privilege; served: boolean }) {
  return (
    <li className="flex h-8 items-center gap-3 px-4">
      <span
        className={cn(
          'truncate font-mono transition-colors',
          served
            ? 'text-fd-foreground'
            : 'text-fd-muted-foreground line-through decoration-fd-muted-foreground/40',
        )}
      >
        {name}
      </span>
      {served ? (
        <span className="ms-auto shrink-0 rounded-md border border-brand/30 bg-brand/10 px-1.5 py-0.5 font-mono text-[11px]">
          {needs}
        </span>
      ) : (
        <span className="ms-auto shrink-0 text-fd-muted-foreground">
          <span className="@max-sm:hidden">needs </span>
          <span className="font-mono">{needs}</span>
        </span>
      )}
    </li>
  );
}
