'use client';

import {
  type CSSProperties,
  type ReactNode,
  useCallback,
  useEffect,
  useId,
  useLayoutEffect,
  useRef,
  useState,
} from 'react';
import { BotIcon, BracesIcon, DatabaseIcon, MonitorIcon, ServerIcon } from 'lucide-react';
import { cn } from '@/lib/cn';
import { LogoMark } from '@/components/logo';
import { useMotionAllowed } from './motion';
import { type Box, type Geometry, layBeams } from './projection/geometry';
import {
  type BeamKind,
  BINS,
  FlowModel,
  lanes,
  type Mode,
  READER_COLUMNS,
  READER_COUNTS,
  READER_ROWS,
  readersInRow,
  TRAVEL_SECONDS,
  WINDOW_SECONDS,
} from './projection/model';
import { useInView } from './use-in-view';

/** The lengths of a pulse's bright head and faint tail, as shares of its beam. */
const HEAD_LENGTH = 0.025;
const TAIL_LENGTH = 0.16;

/** How many pulses one beam can carry at once. */
const POOL_SIZE: Record<BeamKind, number> = { lane: 2, push: 2, poll: 6, record: 1 };

const BEAM_COUNT: Record<BeamKind, number> = {
  lane: lanes.length,
  push: READER_ROWS,
  poll: READER_ROWS,
  record: 1,
};

/** The reads in one bin that fill the chart. More than this clips at the top. */
const CHART_CEILING = 6;
const CHART_HEIGHT = 40;

const kinds = Object.keys(BEAM_COUNT) as BeamKind[];

type Pools = Record<BeamKind, (SVGGElement | null)[][]>;

/** The elements the animation writes to on every frame, outside React. */
type Targets = {
  pools: Pools;
  kafka: HTMLElement | null;
  laneRows: (HTMLElement | null)[];
  laneBars: (HTMLElement | null)[];
  readerRows: (HTMLElement | null)[];
  loadBars: (SVGRectElement | null)[];
  multiple: HTMLElement | null;
};

export function Projection() {
  const [mode, setMode] = useState<Mode>('projection');
  const [readers, setReaders] = useState<number>(10);
  const [model] = useState(() => {
    const model = new FlowModel('projection', 10);
    model.fastForward(WINDOW_SECONDS);

    return model;
  });
  const [geometry, setGeometry] = useState<Geometry | null>(null);

  const containerRef = useRef<HTMLDivElement>(null);
  const coreRef = useRef<HTMLDivElement>(null);
  const readersRef = useRef<HTMLDivElement>(null);
  const [targets] = useState<Targets>(() => ({
    pools: emptyPools(),
    kafka: null,
    laneRows: [],
    laneBars: [],
    readerRows: [],
    loadBars: [],
    multiple: null,
  }));

  const inView = useInView(containerRef);
  const animate = useMotionAllowed() && inView;
  const glowId = `glow-${useId().replace(/[^\w-]/g, '')}`;

  const measure = useCallback(() => {
    const container = containerRef.current;
    const kafka = targets.kafka;
    const core = coreRef.current;
    const readersNode = readersRef.current;
    if (!container || !kafka || !core || !readersNode) return;

    const origin = container.getBoundingClientRect();
    const box = (element: Element | null): Box => {
      const rect = element?.getBoundingClientRect() ?? origin;

      return {
        left: rect.left - origin.left,
        top: rect.top - origin.top,
        right: rect.right - origin.left,
        bottom: rect.bottom - origin.top,
      };
    };

    setGeometry(
      layBeams(origin.width, origin.height, {
        kafka: box(kafka),
        core: box(core),
        readers: box(readersNode),
        laneRows: targets.laneRows.map(box),
        readerRows: targets.readerRows.map(box),
      }),
    );
  }, [targets]);

  useLayoutEffect(() => {
    const container = containerRef.current;
    if (!container) return;

    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(container);

    return () => observer.disconnect();
  }, [measure]);

  // Mode and readers are dependencies so a paused diagram repaints after either changes.
  useEffect(() => {
    if (!geometry) return;

    paint(model, targets, animate);
    if (!animate) return;

    let last = performance.now();
    let frame = 0;

    const tick = (now: number) => {
      // A frame after a long pause, such as a hidden tab, would land every pulse at once.
      model.advance(Math.min(0.1, (now - last) / 1000));
      last = now;
      paint(model, targets, true);
      frame = requestAnimationFrame(tick);
    };
    frame = requestAnimationFrame(tick);

    return () => cancelAnimationFrame(frame);
  }, [animate, geometry, model, targets, mode, readers]);

  const changeMode = (next: Mode) => {
    model.setMode(next);
    if (!animate) model.fastForward(WINDOW_SECONDS);
    setMode(next);
  };

  const changeReaders = (next: number) => {
    model.setReaders(next);
    if (!animate) model.fastForward(WINDOW_SECONDS);
    setReaders(next);
  };

  const direct = mode === 'direct';
  const readerLabel = readers === 1 ? '1 reader' : `${readers} readers`;

  return (
    <div className="flex flex-col gap-6">
      <div ref={containerRef} aria-hidden className="relative select-none pt-4 text-xs lg:pt-28">
        <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(var(--color-fd-border)_1px,transparent_1px)] bg-size-[18px_18px] mask-[radial-gradient(ellipse_at_center,white_30%,transparent_75%)]" />

        {geometry && (
          <Beams
            geometry={geometry}
            mode={mode}
            activeRows={rowsWithReaders(readers)}
            glowId={glowId}
            pools={targets.pools}
          />
        )}

        {geometry?.record && (
          <span
            className={cn(
              'absolute -translate-x-1/2 -translate-y-full rounded-full border bg-fd-card px-3 py-1 text-[11px] text-nowrap text-fd-muted-foreground shadow-sm transition-opacity duration-500',
              direct && 'opacity-0',
            )}
            style={{ left: geometry.record.apex.x, top: geometry.record.apex.y - 6 }}
          >
            Record reads and live tails go to Kafka when someone asks
          </span>
        )}

        <div className="relative grid grid-cols-1 gap-20 lg:grid-cols-[minmax(0,15rem)_minmax(4rem,1fr)_minmax(0,16rem)_minmax(4rem,1fr)_minmax(0,15rem)] lg:items-center lg:gap-0">
          <Node
            caption={
              direct ? 'Load grows with every reader' : 'Load set by cluster size and lane periods'
            }
          >
            <div
              ref={(element) => {
                targets.kafka = element;
              }}
              className={cn(
                'flex flex-col gap-3 rounded-xl border bg-fd-popover p-3 shadow-lg shadow-black/5 transition-colors duration-500',
                direct && readers === 100 && 'border-fd-warning/60',
              )}
              style={
                direct
                  ? {
                      boxShadow:
                        '0 0 calc(var(--heat, 0) * 32px) color-mix(in oklch, var(--color-fd-warning) calc(var(--heat, 0) * 60%), transparent)',
                    }
                  : undefined
              }
            >
              <div className="flex items-center gap-2 font-medium text-fd-foreground">
                <DatabaseIcon className="size-4" />
                Kafka
              </div>
              {['broker-1', 'broker-2', 'broker-3'].map((broker) => (
                <div
                  key={broker}
                  className="flex items-center gap-2 font-mono text-fd-muted-foreground"
                >
                  <ServerIcon className="size-3.5" />
                  {broker}
                  <span
                    className={cn(
                      'ms-auto size-1.5 rounded-full transition-colors duration-500',
                      heat(mode, readers),
                    )}
                  />
                </div>
              ))}
              <div className="border-t pt-3">
                <div className="mb-2 flex items-baseline justify-between text-[11px] text-fd-muted-foreground">
                  <span>Reads of Kafka</span>
                  <span
                    ref={(element) => {
                      targets.multiple = element;
                    }}
                    className={cn(
                      'font-mono text-sm font-medium tabular-nums',
                      direct ? 'text-amber-700 dark:text-fd-warning' : 'text-fd-foreground',
                    )}
                  />
                </div>
                <svg
                  viewBox={`0 0 ${BINS * 4} ${CHART_HEIGHT}`}
                  preserveAspectRatio="none"
                  className={cn(
                    'h-10 w-full transition-colors duration-500',
                    direct ? 'text-fd-warning' : 'text-brand',
                  )}
                >
                  {Array.from({ length: BINS }, (_, i) => (
                    <rect
                      key={i}
                      ref={(element) => {
                        targets.loadBars[i] = element;
                      }}
                      x={i * 4}
                      width={3}
                      y={CHART_HEIGHT}
                      height={0}
                      rx={1}
                      fill="currentColor"
                    />
                  ))}
                </svg>
              </div>
            </div>
          </Node>

          <div className="max-lg:hidden" />

          <Node caption={direct ? 'Skipped, every page asks Kafka' : 'One copy in memory'}>
            <div
              ref={coreRef}
              className={cn(
                'flex flex-col gap-1 rounded-xl border border-brand/50 bg-fd-popover p-3 shadow-[0_0_0_6px] shadow-brand/5 transition-opacity duration-500',
                direct && 'opacity-35',
              )}
            >
              <div className="mb-1 flex items-center gap-2 font-medium text-fd-foreground">
                <LogoMark className="size-4" />
                klens
                <span className="ms-auto rounded-full border border-brand/30 bg-brand/10 px-2 py-0.5 text-[10px] text-brand">
                  projection
                </span>
              </div>
              {lanes.map((lane, i) => (
                <div
                  key={lane.name}
                  ref={(element) => {
                    targets.laneRows[i] = element;
                  }}
                  className="relative flex flex-col gap-1 overflow-hidden rounded-md px-2 py-1"
                  style={
                    {
                      backgroundColor:
                        'color-mix(in oklch, var(--color-brand) calc(var(--flash, 0) * 22%), transparent)',
                    } as CSSProperties
                  }
                >
                  <span className="flex justify-between gap-2 font-mono text-[11px]">
                    <span className="truncate">{lane.name}</span>
                    <span className="text-fd-muted-foreground">{lane.period}</span>
                  </span>
                  <span className="h-0.5 overflow-hidden rounded-full bg-fd-border/60">
                    <span
                      ref={(element) => {
                        targets.laneBars[i] = element;
                      }}
                      className="block h-full origin-left rounded-full bg-brand"
                    />
                  </span>
                </div>
              ))}
            </div>
          </Node>

          <div className="max-lg:hidden" />

          <Node caption="Pages, API calls, and agents">
            <div
              ref={readersRef}
              className="flex flex-col gap-3 rounded-xl border bg-fd-popover p-3 shadow-lg shadow-black/5"
            >
              <div className="flex items-center justify-between font-medium text-fd-foreground">
                Readers
                <span className="font-mono text-fd-muted-foreground tabular-nums">{readers}</span>
              </div>
              <div className="flex flex-col gap-1">
                {Array.from({ length: READER_ROWS }, (_, row) => (
                  <div
                    key={row}
                    ref={(element) => {
                      targets.readerRows[row] = element;
                    }}
                    className="flex gap-1"
                  >
                    {Array.from({ length: READER_COLUMNS }, (_, column) => (
                      <span
                        key={column}
                        className={cn(
                          'aspect-square flex-1 rounded-[2px] transition-colors duration-300',
                          column < readersInRow(readers, row)
                            ? direct
                              ? 'bg-fd-warning opacity-[calc(0.45+0.55*var(--pulse,0))]'
                              : 'bg-brand opacity-[calc(0.45+0.55*var(--pulse,0))]'
                            : 'bg-fd-border/50',
                        )}
                      />
                    ))}
                  </div>
                ))}
              </div>
              <div className="flex justify-between text-fd-muted-foreground">
                <MonitorIcon className="size-3.5" />
                <BracesIcon className="size-3.5" />
                <BotIcon className="size-3.5" />
              </div>
            </div>
          </Node>
        </div>
      </div>

      <div className="flex flex-col items-center gap-3 rounded-2xl border bg-fd-background/60 p-2 md:flex-row">
        <Segmented
          label="How the UI reads Kafka"
          value={mode}
          onChange={changeMode}
          options={[
            { value: 'projection', label: 'With klens' },
            { value: 'direct', label: 'Each page asks Kafka' },
          ]}
        />
        <p aria-live="polite" className="flex-1 px-2 text-center text-xs text-fd-muted-foreground">
          {direct
            ? `${readerLabel} ask Kafka every few seconds, so each one adds its own reads.`
            : `${readerLabel} read the copy in memory. Kafka sees the same lane reads for 1 or 100.`}
        </p>
        <Segmented
          label="Readers"
          value={readers}
          onChange={changeReaders}
          options={READER_COUNTS.map((count) => ({
            value: count,
            label: count === 1 ? '1 reader' : `${count}`,
          }))}
        />
      </div>
    </div>
  );
}

function Beams({
  geometry,
  mode,
  activeRows,
  glowId,
  pools,
}: {
  geometry: Geometry;
  mode: Mode;
  activeRows: boolean[];
  glowId: string;
  pools: Pools;
}) {
  const direct = mode === 'direct';
  const paths: Record<BeamKind, string[]> = {
    lane: geometry.lanes,
    push: geometry.pushes,
    poll: geometry.polls,
    record: geometry.record ? [geometry.record.path] : [],
  };
  const visible: Record<BeamKind, boolean> = {
    lane: true,
    push: !direct,
    poll: direct,
    record: !direct,
  };

  return (
    <svg
      viewBox={`0 0 ${geometry.width} ${geometry.height}`}
      className="pointer-events-none absolute inset-0 size-full overflow-visible"
    >
      <defs>
        <filter id={glowId} x="-50%" y="-50%" width="200%" height="200%">
          <feGaussianBlur stdDeviation="3" result="blur" />
          <feMerge>
            <feMergeNode in="blur" />
            <feMergeNode in="SourceGraphic" />
          </feMerge>
        </filter>
      </defs>

      {kinds.map((kind) => (
        <g
          key={kind}
          className={cn(
            'transition-opacity duration-500',
            kind === 'poll'
              ? 'text-fd-warning'
              : kind === 'record'
                ? 'text-fd-muted-foreground'
                : 'text-brand',
            !visible[kind] && 'opacity-0',
            kind === 'lane' && direct && 'opacity-15',
          )}
        >
          {paths[kind].map((path, index) => {
            const hidden = (kind === 'push' || kind === 'poll') && !activeRows[index];

            return (
              <g
                key={index}
                className={cn('transition-opacity duration-500', hidden && 'opacity-0')}
              >
                <path
                  d={path}
                  fill="none"
                  stroke="currentColor"
                  strokeOpacity={0.22}
                  strokeWidth={1}
                  strokeDasharray={kind === 'record' ? '4 5' : undefined}
                />
                <g filter={`url(#${glowId})`}>
                  {Array.from({ length: POOL_SIZE[kind] }, (_, slot) => (
                    <g
                      key={slot}
                      ref={(element) => {
                        pools[kind][index][slot] = element;
                      }}
                      opacity={0}
                    >
                      {[
                        { length: TAIL_LENGTH, width: 2, opacity: 0.35 },
                        { length: HEAD_LENGTH, width: kind === 'record' ? 2 : 3, opacity: 1 },
                      ].map((stroke) => (
                        <path
                          key={stroke.length}
                          data-length={stroke.length}
                          d={path}
                          pathLength={1}
                          fill="none"
                          stroke="currentColor"
                          strokeOpacity={stroke.opacity}
                          strokeWidth={stroke.width}
                          strokeLinecap="round"
                          strokeDasharray={`${stroke.length} 2`}
                        />
                      ))}
                    </g>
                  ))}
                </g>
              </g>
            );
          })}
        </g>
      ))}
    </svg>
  );
}

function Node({ caption, children }: { caption: string; children: ReactNode }) {
  return (
    <div className="relative mx-auto flex w-full max-w-sm flex-col gap-2">
      {children}
      <span className="text-center text-[11px] text-fd-muted-foreground">{caption}</span>
    </div>
  );
}

function Segmented<T extends string | number>({
  label,
  value,
  onChange,
  options,
}: {
  label: string;
  value: T;
  onChange: (value: T) => void;
  options: { value: T; label: string }[];
}) {
  return (
    <div
      role="group"
      aria-label={label}
      className="flex shrink-0 rounded-full border bg-fd-card p-0.5 text-xs"
    >
      {options.map((option) => (
        <button
          key={option.value}
          type="button"
          aria-pressed={value === option.value}
          onClick={() => onChange(option.value)}
          className={cn(
            'rounded-full px-3 py-1.5 transition-colors',
            'focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-brand',
            value === option.value
              ? 'bg-brand font-medium text-brand-foreground'
              : 'text-fd-muted-foreground hover:text-fd-foreground',
          )}
        >
          {option.label}
        </button>
      ))}
    </div>
  );
}

/** Brokers warm up as readers pile onto them, and stay cool behind a projection. */
function heat(mode: Mode, readers: number): string {
  if (mode === 'projection' || readers === 1) return 'bg-fd-success';
  if (readers === 10) return 'bg-fd-warning';

  return 'bg-red-500 motion-safe:animate-pulse';
}

function rowsWithReaders(readers: number): boolean[] {
  return Array.from({ length: READER_ROWS }, (_, row) => readersInRow(readers, row) > 0);
}

function emptyPools(): Pools {
  const pools = {} as Pools;
  for (const kind of kinds) {
    pools[kind] = Array.from({ length: BEAM_COUNT[kind] }, () => []);
  }

  return pools;
}

/** Writes one frame of the model to the page. Paused, it draws no pulses, only the state. */
function paint(model: FlowModel, targets: Targets, withPulses: boolean) {
  const used: Record<BeamKind, number[]> = {
    lane: new Array<number>(BEAM_COUNT.lane).fill(0),
    push: new Array<number>(BEAM_COUNT.push).fill(0),
    poll: new Array<number>(BEAM_COUNT.poll).fill(0),
    record: [0],
  };

  if (withPulses) {
    for (const pulse of model.pulses) {
      const progress = (model.time - pulse.start) / TRAVEL_SECONDS[pulse.kind];
      if (progress < 0) continue;

      const slot = used[pulse.kind][pulse.index]++;
      const group = targets.pools[pulse.kind][pulse.index]?.[slot];
      if (!group) continue;

      // A dash of length L at offset L - p ends at p, so both strokes share the head's position.
      for (const stroke of group.children) {
        if (stroke instanceof SVGElement) {
          stroke.style.strokeDashoffset = String(Number(stroke.dataset.length) - progress);
        }
      }
      group.style.opacity = '1';
    }
  }

  for (const kind of kinds) {
    targets.pools[kind].forEach((pool, index) => {
      for (let slot = used[kind][index]; slot < pool.length; slot++) {
        const group = pool[slot];
        if (group) group.style.opacity = '0';
      }
    });
  }

  lanes.forEach((lane, i) => {
    const age = model.time - model.laneRefreshed[i];
    targets.laneBars[i]?.style.setProperty('transform', `scaleX(${clamp(1 - age / lane.seconds)})`);
    targets.laneRows[i]?.style.setProperty(
      '--flash',
      String(withPulses ? clamp(1 - age / 0.7) : 0),
    );
  });

  targets.readerRows.forEach((row, i) => {
    const age = model.time - model.rowFlashed[i];
    row?.style.setProperty('--pulse', String(withPulses ? clamp(1 - age / 0.5) : 0));
  });

  const bins = model.loadBins();
  const recent = (bins[BINS - 1] + bins[BINS - 2]) / 2;
  targets.kafka?.style.setProperty('--heat', String(clamp(recent / CHART_CEILING)));

  bins.forEach((count, i) => {
    const bar = targets.loadBars[i];
    if (!bar) return;

    const height = Math.max(count > 0 ? 3 : 1, clamp(count / CHART_CEILING) * CHART_HEIGHT);
    bar.setAttribute('y', String(CHART_HEIGHT - height));
    bar.setAttribute('height', String(height));
    bar.setAttribute('opacity', count > 0 ? '1' : '0.25');
  });

  if (targets.multiple) {
    const text = `${Math.max(1, Math.round(model.loadMultiple()))}×`;
    if (targets.multiple.textContent !== text) targets.multiple.textContent = text;
  }
}

function clamp(value: number): number {
  return Math.min(1, Math.max(0, value));
}
