'use client';

import { useState } from 'react';
import { cn } from '@/lib/cn';

type View = 'wire' | 'klens';

type Line =
  | { kind: 'open'; indent: number; name: string }
  | { kind: 'close'; indent: number; last?: boolean }
  | {
      kind: 'field';
      indent: number;
      name: string;
      wire: string;
      klens?: string;
      strategy?: 'hash' | 'mask' | 'drop';
      last?: boolean;
    };

const lines: Line[] = [
  { kind: 'field', indent: 1, name: 'order', wire: '1042' },
  { kind: 'open', indent: 1, name: 'card' },
  {
    kind: 'field',
    indent: 2,
    name: 'number',
    wire: '"4111 1111 1111 1111"',
    klens: '"kx:3f9a2c481b7d6e05"',
    strategy: 'hash',
  },
  { kind: 'field', indent: 2, name: 'cvv', wire: '"123"', strategy: 'drop', last: true },
  { kind: 'close', indent: 1 },
  { kind: 'open', indent: 1, name: 'customer' },
  {
    kind: 'field',
    indent: 2,
    name: 'email',
    wire: '"ada@example.com"',
    klens: '"***"',
    strategy: 'mask',
    last: true,
  },
  { kind: 'close', indent: 1, last: true },
];

const keyColor = 'text-blue-700 dark:text-blue-400';
const valueColor = 'text-green-800 dark:text-fd-success';

const views: { value: View; label: string }[] = [
  { value: 'wire', label: 'In Kafka' },
  { value: 'klens', label: 'In klens' },
];

export function ObfuscatedRecord() {
  const [view, setView] = useState<View>('klens');

  return (
    <div className="flex flex-col overflow-hidden rounded-xl border bg-fd-popover text-fd-popover-foreground">
      <div className="flex items-center gap-3 border-b px-4 py-2.5">
        <span className="truncate font-mono text-xs text-fd-muted-foreground max-sm:hidden">
          payments.transactions / 4 @ 88
        </span>
        <div
          role="group"
          aria-label="Record view"
          className="ms-auto flex shrink-0 rounded-full border p-0.5 text-xs"
        >
          {views.map((item) => (
            <button
              key={item.value}
              type="button"
              aria-pressed={view === item.value}
              onClick={() => setView(item.value)}
              className={cn(
                'rounded-full px-2.5 py-1 transition-colors',
                view === item.value
                  ? 'bg-fd-accent font-medium text-fd-accent-foreground'
                  : 'text-fd-muted-foreground hover:text-fd-foreground',
              )}
            >
              {item.label}
            </button>
          ))}
        </div>
      </div>

      <pre className="overflow-x-auto p-4 font-mono text-xs leading-6">
        <code className="grid">
          <span>{'{'}</span>
          {lines.map((line, i) => {
            const pad = '  '.repeat(line.indent);

            if (line.kind === 'open') {
              return (
                <span key={i}>
                  {pad}
                  <span className={keyColor}>&quot;{line.name}&quot;</span>: {'{'}
                </span>
              );
            }

            if (line.kind === 'close') {
              return (
                <span key={i}>
                  {pad}
                  {'}'}
                  {!line.last && ','}
                </span>
              );
            }

            const changed = view === 'klens' && line.strategy !== undefined;
            const dropped = changed && line.strategy === 'drop';
            const next = lines[i + 1];
            const nextDropped =
              view === 'klens' && next?.kind === 'field' && next.strategy === 'drop' && next.last;

            return (
              <span key={i} className="flex whitespace-pre">
                <span>{pad}</span>
                <span className="flex flex-wrap items-center gap-x-3">
                  {!dropped && (
                    <span>
                      <span className={keyColor}>&quot;{line.name}&quot;</span>:{' '}
                      <span className={changed ? 'text-brand' : valueColor}>
                        {view === 'klens' && line.klens ? line.klens : line.wire}
                      </span>
                      {!line.last && !nextDropped && ','}
                    </span>
                  )}
                  {changed && (
                    <span className="rounded-full border border-brand/40 bg-brand/10 px-2 text-[11px] leading-4 text-brand animate-fd-fade-in">
                      {line.strategy}
                    </span>
                  )}
                </span>
              </span>
            );
          })}
          <span>{'}'}</span>
        </code>
      </pre>
    </div>
  );
}
