import { BotIcon, WrenchIcon } from 'lucide-react';

export function AgentChat() {
  return (
    <div
      aria-hidden
      className="flex select-none flex-col gap-4 rounded-xl border bg-fd-popover p-4 text-fd-popover-foreground"
    >
      <p className="ms-auto max-w-[85%] rounded-2xl rounded-br-md bg-fd-accent px-4 py-2.5">
        Why is fraud-detector falling behind on production?
      </p>

      <div className="flex w-fit items-center gap-2 rounded-lg border bg-fd-background px-3 py-2 font-mono text-xs text-fd-muted-foreground">
        <WrenchIcon className="size-3.5" />
        <span className="text-fd-foreground">klens_group_describe</span>
        <span className="max-sm:hidden">cluster: production, group: fraud-detector</span>
      </div>

      <div className="flex gap-3">
        <BotIcon className="mt-0.5 size-4 shrink-0 text-fd-muted-foreground" />
        <p className="leading-relaxed">
          One member, <code className="font-mono text-xs">fraud-detector-2</code>, holds 91% of the
          lag, on partitions 4 to 7 of{' '}
          <code className="font-mono text-xs">payments.transactions</code>. The other member is
          caught up, so the group is stuck on one consumer rather than slow overall.
        </p>
      </div>
    </div>
  );
}

const tools = [
  { name: 'klens_clusters', description: 'Clusters and their health' },
  { name: 'klens_search', description: 'Topics, groups, brokers, and subjects by name' },
  { name: 'klens_group_describe', description: 'Members, lag per partition, what looks wrong' },
  { name: 'klens_schema_get', description: 'The text of a schema, read from the registry' },
];

export function ToolList() {
  return (
    <div className="@container overflow-hidden rounded-xl border bg-fd-popover text-fd-popover-foreground">
      <ul>
        {tools.map((tool) => (
          <li
            key={tool.name}
            className="flex flex-col gap-1 border-b border-fd-border/60 px-4 py-2.5 last:border-b-0 @md:flex-row @md:items-center @md:gap-3"
          >
            <span className="flex items-center gap-2 font-mono text-xs font-medium text-nowrap">
              <WrenchIcon className="size-3.5 text-brand" />
              {tool.name}
            </span>
            <span className="text-xs text-fd-muted-foreground @md:ms-auto @md:text-end">
              {tool.description}
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}
