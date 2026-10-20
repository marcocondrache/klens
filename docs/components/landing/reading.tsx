import {
  CalendarClockIcon,
  DownloadIcon,
  FileJson2Icon,
  FilterIcon,
  type LucideIcon,
  SearchIcon,
} from 'lucide-react';
import { cn } from '@/lib/cn';
import { InView } from './in-view';
import { order } from './orders';

const matches = Array.from({ length: 30 }, (_, sequence) => order(sequence))
  .filter((record) => record.value.includes('"item":"book"'))
  .reverse()
  .slice(0, 4);

export function RecordSearch() {
  return (
    <InView
      aria-hidden
      className="flex select-none flex-col rounded-xl border bg-fd-popover text-fd-popover-foreground mask-[linear-gradient(to_bottom,white_60%,transparent)]"
    >
      <div className="flex flex-wrap items-center gap-2 border-b p-3">
        <div className="flex min-w-48 flex-1 items-center gap-2 rounded-lg border bg-fd-background px-3 py-2">
          <SearchIcon className="size-4 text-fd-muted-foreground" />
          <span className="font-mono text-xs">book</span>
          <span className="-ms-1.5 h-4 w-px bg-fd-foreground motion-safe:animate-pulse" />
        </div>
        <Chip icon={FilterIcon}>Filter</Chip>
        <Chip icon={CalendarClockIcon}>Last 24 hours</Chip>
        <span className="ms-auto rounded-lg border p-2 text-fd-muted-foreground">
          <DownloadIcon className="size-4" />
        </span>
      </div>

      {matches.map((record) => (
        <div
          key={record.key}
          className="flex flex-col gap-1.5 border-b border-fd-border/60 px-4 py-3"
        >
          <div className="flex items-center gap-2 text-xs text-fd-muted-foreground">
            <FileJson2Icon className="size-3.5" />
            <span className="font-mono text-nowrap">
              orders / {record.partition} @ {record.offset}
            </span>
            <span className="ms-auto truncate font-mono">{record.key}</span>
          </div>
          <p className="truncate font-mono text-xs">{record.value}</p>
        </div>
      ))}
    </InView>
  );
}

function Chip({ icon: Icon, children }: { icon: LucideIcon; children: string }) {
  return (
    <span className="flex items-center gap-1.5 rounded-lg border px-2.5 py-2 text-xs text-fd-muted-foreground">
      <Icon className="size-3.5" />
      {children}
    </span>
  );
}

const groups = [
  { name: 'order-service', state: 'Stable', members: 3, lag: 12 },
  { name: 'payments-ledger', state: 'Stable', members: 6, lag: 0 },
  { name: 'fraud-detector', state: 'Stable', members: 2, lag: 18_402 },
  { name: 'email-sender', state: 'Empty', members: 0, lag: 3_210 },
  { name: 'analytics-sink', state: 'Stable', members: 4, lag: 640 },
] as const;

const maxLag = Math.max(...groups.map((group) => group.lag));

const lagColumns =
  'grid grid-cols-[minmax(0,1fr)_auto_4.5rem] gap-x-3 px-4 @sm:grid-cols-[minmax(0,1fr)_4.5rem_6rem] @md:grid-cols-[minmax(0,1fr)_4.5rem_4rem_7rem]';

export function ConsumerLag() {
  return (
    <div
      aria-hidden
      className="@container flex select-none flex-col overflow-hidden rounded-xl border bg-fd-popover text-fd-popover-foreground"
    >
      <div className={cn(lagColumns, 'border-b py-2.5 text-xs text-fd-muted-foreground')}>
        <span>Group</span>
        <span>
          <span className="@max-sm:hidden">State</span>
        </span>
        <span className="text-end @max-md:hidden">Members</span>
        <span className="text-end">Lag</span>
      </div>
      {groups.map((group) => (
        <div
          key={group.name}
          className={cn(
            lagColumns,
            'h-11 items-center border-b border-fd-border/60 text-xs last:border-b-0',
          )}
        >
          <span className="truncate font-mono">{group.name}</span>
          <span className="flex items-center gap-1.5">
            <span
              className={cn(
                'size-1.5 rounded-full',
                group.state === 'Stable' ? 'bg-fd-success' : 'bg-fd-muted-foreground/50',
              )}
            />
            <span className="@max-sm:hidden">{group.state}</span>
          </span>
          <span className="text-end tabular-nums @max-md:hidden">{group.members}</span>
          <span className="flex flex-col items-end gap-1">
            <span
              className={cn(
                'tabular-nums',
                group.lag > 10_000 && 'font-medium text-amber-700 dark:text-fd-warning',
              )}
            >
              {group.lag.toLocaleString('en-US')}
            </span>
            <span className="h-1 w-full overflow-hidden rounded-full bg-fd-accent">
              <span
                className={cn(
                  'block h-full rounded-full',
                  group.lag > 10_000 ? 'bg-fd-warning' : 'bg-fd-muted-foreground/60',
                )}
                style={{ width: `${(Math.log10(group.lag + 1) / Math.log10(maxLag + 1)) * 100}%` }}
              />
            </span>
          </span>
        </div>
      ))}
    </div>
  );
}
