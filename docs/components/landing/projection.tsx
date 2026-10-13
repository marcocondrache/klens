import type { CSSProperties, ReactNode } from 'react';
import {
  ArrowDownIcon,
  BotIcon,
  BracesIcon,
  DatabaseIcon,
  MonitorIcon,
  ServerIcon,
} from 'lucide-react';
import { cn } from '@/lib/cn';
import { LogoMark } from '@/components/logo';
import { InView } from './in-view';

/** The ingest lanes, with their default periods. Pulses move faster on shorter periods. */
const lanes = [
  { name: 'topology', period: '10s', pulse: '3.2s' },
  { name: 'watermarks', period: '3s · 30s', pulse: '1.8s' },
  { name: 'offsets', period: '2s · 20s', pulse: '1.5s' },
  { name: 'configs', period: '60s', pulse: '7s' },
  { name: 'subjects', period: '30s', pulse: '5s' },
  { name: 'log dirs', period: '60s', pulse: '8s' },
  { name: 'acls · quotas · users', period: '60s', pulse: '9s' },
];

const readers = [
  { icon: MonitorIcon, name: 'Topics page' },
  { icon: MonitorIcon, name: 'Consumer group' },
  { icon: MonitorIcon, name: 'Broker page' },
  { icon: BotIcon, name: 'Agent over MCP' },
  { icon: BracesIcon, name: 'JSON API' },
];

export function Projection() {
  return (
    <InView
      aria-hidden
      className="grid select-none items-center gap-6 text-xs lg:grid-cols-[9rem_minmax(0,1fr)_11rem_minmax(0,0.6fr)_10rem] lg:gap-0"
    >
      <Node label="Fixed cadence">
        <div className="flex items-center gap-2 font-medium text-fd-foreground">
          <DatabaseIcon className="size-4" />
          Kafka
        </div>
        {['broker-1', 'broker-2', 'broker-3'].map((broker) => (
          <div key={broker} className="flex items-center gap-2 font-mono text-fd-muted-foreground">
            <ServerIcon className="size-3.5" />
            {broker}
          </div>
        ))}
      </Node>

      <StackedArrow />

      <div className="flex flex-col gap-3.5 lg:px-4">
        {lanes.map((lane) => (
          <div key={lane.name} className="flex flex-col gap-1">
            <div className="flex justify-between font-mono text-[11px] text-fd-muted-foreground">
              <span>{lane.name}</span>
              <span>{lane.period}</span>
            </div>
            <Rail duration={lane.pulse} className="bg-brand/25">
              <span className="block size-1.5 rounded-full bg-brand shadow-[0_0_10px_2px] shadow-brand/60" />
            </Rail>
          </div>
        ))}
      </div>

      <StackedArrow />

      <Node label="One copy in memory" className="border-brand/50 bg-brand/5">
        <div className="flex items-center gap-2 font-medium text-fd-foreground">
          <LogoMark className="size-4" />
          Projection
        </div>
        {['topics · partitions', 'groups · lag', 'configs · schemas', 'sizes · ACLs'].map(
          (table) => (
            <div
              key={table}
              className="rounded-md border bg-fd-background px-2 py-1 font-mono text-[11px]"
            >
              {table}
            </div>
          ),
        )}
      </Node>

      <StackedArrow />

      <div className="hidden flex-col gap-2 lg:flex">
        <div className="flex flex-col gap-2 border border-transparent p-3 lg:px-4">
          {readers.map((reader, i) => (
            <div key={reader.name} className="flex h-5 items-center">
              <Rail
                duration={`${0.9 + (i % 3) * 0.25}s`}
                className="w-full bg-fd-muted-foreground/20"
              >
                <span className="block size-1 rounded-full bg-fd-muted-foreground" />
              </Rail>
            </div>
          ))}
        </div>
        <span className="invisible text-[11px]">readers</span>
      </div>

      <Node label="Any number of readers">
        {readers.map(({ icon: Icon, name }) => (
          <div key={name} className="flex h-5 items-center gap-2 text-fd-foreground">
            <Icon className="size-3.5 text-fd-muted-foreground" />
            {name}
          </div>
        ))}
      </Node>
    </InView>
  );
}

/** Below lg the diagram stacks, so an arrow carries the flow down between the columns. */
function StackedArrow() {
  return <ArrowDownIcon className="mx-auto -my-3 size-4 text-fd-muted-foreground lg:hidden" />;
}

function Node({
  label,
  className,
  children,
}: {
  label: string;
  className?: string;
  children: ReactNode;
}) {
  return (
    <div className="flex flex-col gap-2">
      <div
        className={cn(
          'flex flex-col gap-2 rounded-xl border bg-fd-popover p-3 shadow-sm',
          className,
        )}
      >
        {children}
      </div>
      <span className="text-center text-[11px] text-fd-muted-foreground">{label}</span>
    </div>
  );
}

function Rail({
  duration,
  className,
  children,
}: {
  duration: string;
  className?: string;
  children: ReactNode;
}) {
  return (
    <div className={cn('relative h-px overflow-x-clip', className)}>
      <span
        className="absolute inset-0 flex items-center motion-safe:animate-lane-pulse motion-reduce:hidden"
        style={{ '--lane-duration': duration } as CSSProperties}
      >
        {children}
      </span>
    </div>
  );
}
