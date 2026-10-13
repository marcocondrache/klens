'use client';

import { useEffect, useRef, useState } from 'react';
import { cn } from '@/lib/cn';
import { useMotionAllowed } from './motion';
import { order } from './orders';
import { useInView } from './use-in-view';

const VISIBLE_ROWS = 7;
const ARRIVAL_MS = 1400;

const columns =
  'grid grid-cols-[2.5rem_3.5rem_minmax(0,1fr)] gap-x-3 px-4 @md:grid-cols-[3rem_3.5rem_6rem_minmax(0,1fr)]';

export function LiveTail() {
  const ref = useRef<HTMLDivElement>(null);
  const inView = useInView(ref);
  const motionAllowed = useMotionAllowed();
  const [next, setNext] = useState(VISIBLE_ROWS);
  const rows = Array.from({ length: VISIBLE_ROWS }, (_, i) => order(next - 1 - i));

  useEffect(() => {
    if (!inView || !motionAllowed) return;

    const timeout = setTimeout(() => setNext(next + 1), ARRIVAL_MS);

    return () => clearTimeout(timeout);
  }, [inView, motionAllowed, next]);

  return (
    <div
      ref={ref}
      aria-hidden
      data-active={inView}
      className="@container flex select-none flex-col overflow-hidden rounded-xl border bg-fd-popover text-fd-popover-foreground"
    >
      <div className="flex items-center gap-3 border-b px-4 py-2.5">
        <span className="font-mono text-sm font-medium">orders</span>
        <div className="ms-auto flex shrink-0 rounded-full border p-0.5 text-xs">
          <span className="px-2.5 py-1 text-fd-muted-foreground">Newest</span>
          <span className="px-2.5 py-1 text-fd-muted-foreground @max-2xs:hidden">Oldest</span>
          <span className="flex items-center gap-1.5 rounded-full bg-fd-accent px-2.5 py-1 font-medium">
            <span className="relative flex size-1.5">
              <span className="absolute inline-flex size-full rounded-full bg-fd-success opacity-75 motion-safe:animate-ping" />
              <span className="relative inline-flex size-1.5 rounded-full bg-fd-success" />
            </span>
            Live
          </span>
        </div>
      </div>

      <div className={cn(columns, 'border-b py-2 text-xs text-fd-muted-foreground')}>
        <span>Part.</span>
        <span className="text-end">Offset</span>
        <span className="@max-md:hidden">Key</span>
        <span>Value</span>
      </div>

      <div className="h-[252px] overflow-hidden mask-[linear-gradient(to_bottom,white_70%,transparent)]">
        {rows.map((row, i) => (
          <div
            key={row.key}
            className={cn(
              columns,
              'h-9 items-center border-b border-fd-border/60 font-mono text-xs',
              i === 0 &&
                next > VISIBLE_ROWS &&
                'relative animate-record-in before:pointer-events-none before:absolute before:inset-0 before:animate-record-flash before:bg-brand/15',
            )}
          >
            <span className="text-fd-muted-foreground">{row.partition}</span>
            <span className="text-end tabular-nums">{row.offset}</span>
            <span className="truncate @max-md:hidden">{row.key}</span>
            <span className="truncate text-fd-muted-foreground">{row.value}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
