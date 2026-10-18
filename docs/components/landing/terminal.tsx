'use client';

import { type ReactNode, useEffect, useRef, useState } from 'react';
import { CheckIcon, CopyIcon, TerminalIcon } from 'lucide-react';
import { cn } from '@/lib/cn';
import { useMotionAllowed } from './motion';
import { useInView } from './use-in-view';

export type LogLine = {
  level: 'INFO' | 'WARN';
  message: string;
  fields?: string;
};

const TYPE_MS = 24;
const PAUSE_TICKS = 4;
const LINE_MS = 220;

export function Terminal({
  command,
  output,
  window: browserWindow,
}: {
  command: string;
  output: LogLine[];
  window: ReactNode;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const inView = useInView(ref);
  const motionAllowed = useMotionAllowed();

  const typed = command.length;
  const ran = typed + PAUSE_TICKS;
  const end = ran + output.length + 1;
  const [tick, setTick] = useState(end);
  const [played, setPlayed] = useState(false);
  const [copied, setCopied] = useState(false);

  if (inView && motionAllowed && !played) {
    setPlayed(true);
    setTick(0);
  }

  useEffect(() => {
    if (tick >= end) return;

    const timeout = setTimeout(() => setTick(tick + 1), tick < typed ? TYPE_MS : LINE_MS);

    return () => clearTimeout(timeout);
  }, [tick, typed, end]);

  useEffect(() => {
    if (!copied) return;

    const timeout = setTimeout(() => setCopied(false), 1500);

    return () => clearTimeout(timeout);
  }, [copied]);

  const copy = () => {
    navigator.clipboard.writeText(command).then(
      () => setCopied(true),
      () => setCopied(false),
    );
  };

  const transcript = [
    `$ ${command}`,
    ...output.map((line) => [line.level, line.message, line.fields].filter(Boolean).join(' ')),
  ].join('\n');

  return (
    <div ref={ref} className="relative overflow-hidden rounded-xl border bg-fd-secondary shadow-md">
      <div className="flex items-center gap-2 border-b px-3 py-2 text-fd-muted-foreground">
        <TerminalIcon className="size-4" />
        <span className="text-xs font-medium">Terminal</span>
        <button
          type="button"
          onClick={copy}
          aria-label={copied ? 'Copied' : 'Copy command'}
          className="ms-auto rounded-md p-1 transition-colors hover:bg-fd-accent hover:text-fd-accent-foreground"
        >
          {copied ? <CheckIcon className="size-3.5" /> : <CopyIcon className="size-3.5" />}
        </button>
      </div>

      <pre className="sr-only">{transcript}</pre>

      <pre
        aria-hidden
        className="min-h-[280px] overflow-x-auto p-3 pb-28 font-mono text-[11px] leading-6 text-fd-secondary-foreground/90 sm:p-4 sm:pb-28 sm:text-[13px]"
      >
        <code className="grid">
          <span className="break-all whitespace-pre-wrap">
            <span className="select-none text-brand">$ </span>
            {command.slice(0, tick)}
            {tick < typed && (
              <span className="ms-px inline-block h-4 w-2 translate-y-0.5 animate-pulse bg-fd-foreground" />
            )}
            <span className="invisible">{command.slice(tick)}</span>
          </span>
          {output.map((line, i) => (
            <span
              key={i}
              className={cn(
                'whitespace-pre-wrap ps-[6ch] -indent-[6ch] wrap-anywhere',
                i < tick - ran ? 'animate-fd-fade-in' : 'invisible',
              )}
            >
              <span
                className={
                  line.level === 'INFO'
                    ? 'text-green-800 dark:text-fd-success'
                    : 'text-amber-800 dark:text-fd-warning'
                }
              >
                {line.level.padStart(5)}
              </span>{' '}
              {line.message}
              {line.fields && <span className="text-fd-muted-foreground"> {line.fields}</span>}
            </span>
          ))}
        </code>
      </pre>

      <div
        aria-hidden
        className={cn(
          'absolute right-4 bottom-4 transition-all duration-500',
          tick < end ? 'invisible translate-y-4 opacity-0' : 'visible translate-y-0 opacity-100',
        )}
      >
        {browserWindow}
      </div>
    </div>
  );
}
