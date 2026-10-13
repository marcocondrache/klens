'use client';

import {
  type ReactNode,
  useCallback,
  useEffect,
  useId,
  useLayoutEffect,
  useRef,
  useState,
} from 'react';
import { DatabaseIcon } from 'lucide-react';
import { cn } from '@/lib/cn';
import { Lens } from './lens';
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
const POOL_SIZE: Record<BeamKind, number> = { lane: 2, push: 2, poll: 6 };

const BEAM_COUNT: Record<BeamKind, number> = {
  lane: lanes.length,
  push: READER_ROWS,
  poll: READER_ROWS,
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
  hub: HTMLElement | null;
  readerRows: (HTMLElement | null)[];
  loadBars: (SVGRectElement | null)[];
  multiple: HTMLElement | null;
};

/**
 * One cluster and its readers, drawn as beams across the section below its copy. With klens,
 * the lanes converge on the lens and frames fan out to the readers. Without it, every reader
 * reaches across to Kafka on its own.
 */
export function Projection({ children }: { children: ReactNode }) {
  const [mode, setMode] = useState<Mode>('projection');
  const [readers, setReaders] = useState<number>(10);
  const [model] = useState(() => {
    const model = new FlowModel('projection', 10);
    model.fastForward(WINDOW_SECONDS);

    return model;
  });
  const [geometry, setGeometry] = useState<Geometry | null>(null);
  const [targets] = useState<Targets>(() => ({
    pools: emptyPools(),
    kafka: null,
    hub: null,
    readerRows: [],
    loadBars: [],
    multiple: null,
  }));

  const stageRef = useRef<HTMLDivElement>(null);
  const readersRef = useRef<HTMLDivElement>(null);

  const inView = useInView(stageRef);
  const animate = useMotionAllowed() && inView;
  const glowId = `glow-${useId().replace(/[^\w-]/g, '')}`;

  const measure = useCallback(() => {
    const stage = stageRef.current;
    const readersNode = readersRef.current;
    if (!stage || !targets.kafka || !targets.hub || !readersNode) return;

    const origin = stage.getBoundingClientRect();
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
        kafka: box(targets.kafka),
        hub: box(targets.hub),
        readers: box(readersNode),
        readerRows: targets.readerRows.map(box),
      }),
    );
  }, [targets]);

  useLayoutEffect(() => {
    const stage = stageRef.current;
    if (!stage) return;

    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(stage);

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

  return (
    <div className="relative isolate col-span-full flex flex-col items-center gap-10">
      <div className="pointer-events-none absolute -inset-x-6 -inset-y-16 -z-1 bg-[radial-gradient(var(--color-fd-border)_1px,transparent_1px)] bg-size-[18px_18px] mask-[radial-gradient(ellipse_at_center,white_10%,transparent_70%)] md:-inset-x-12" />

      <div className="max-w-2xl text-center">{children}</div>

      <div ref={stageRef} aria-hidden className="relative w-full select-none text-xs lg:h-[360px]">
        {geometry && (
          <Beams
            geometry={geometry}
            mode={mode}
            activeRows={rowsWithReaders(readers)}
            glowId={glowId}
            pools={targets.pools}
          />
        )}

        <div className="relative flex h-full flex-col items-center gap-14 lg:flex-row lg:justify-between lg:gap-0 lg:px-[7%]">
          <div
            ref={(element) => {
              targets.kafka = element;
            }}
            className={cn(
              'flex w-44 flex-col gap-2 rounded-xl border bg-fd-popover/85 p-3 shadow-xl shadow-black/5 backdrop-blur-md transition-colors duration-500',
              direct && readers === 100 && 'border-fd-warning/60',
            )}
            style={
              direct
                ? {
                    boxShadow:
                      '0 0 calc(var(--heat, 0) * 40px) color-mix(in oklch, var(--color-fd-warning) calc(var(--heat, 0) * 60%), transparent)',
                  }
                : undefined
            }
          >
            <div className="flex items-center gap-2 font-medium text-fd-foreground">
              <DatabaseIcon className="size-4" />
              Kafka
              <span
                ref={(element) => {
                  targets.multiple = element;
                }}
                className={cn(
                  'ms-auto font-mono tabular-nums',
                  direct ? 'text-amber-700 dark:text-fd-warning' : 'text-fd-muted-foreground',
                )}
              />
            </div>
            <svg
              viewBox={`0 0 ${BINS * 4} ${CHART_HEIGHT}`}
              preserveAspectRatio="none"
              className={cn(
                'h-7 w-full transition-colors duration-500',
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

          <div
            ref={(element) => {
              targets.hub = element;
            }}
            className={cn(
              'relative size-44 transition-[opacity,filter] duration-700 lg:size-60',
              direct && 'opacity-20 grayscale',
            )}
          >
            <div
              className="absolute -inset-1/3 rounded-full"
              style={{
                background:
                  'radial-gradient(closest-side, color-mix(in oklch, var(--color-brand-glow) calc(18% + var(--flash, 0) * 30%), transparent), transparent)',
              }}
            />
            <Lens className="relative size-full" />
          </div>

          <div
            ref={readersRef}
            className="flex flex-col gap-2 rounded-xl border bg-fd-popover/85 p-3 shadow-xl shadow-black/5 backdrop-blur-md"
          >
            <div className="flex items-center justify-between gap-4 font-medium text-fd-foreground">
              Readers
              <span className="font-mono text-fd-muted-foreground tabular-nums">{readers}</span>
            </div>
            <div className="flex flex-col gap-0.5">
              {Array.from({ length: READER_ROWS }, (_, row) => (
                <div
                  key={row}
                  ref={(element) => {
                    targets.readerRows[row] = element;
                  }}
                  className="flex gap-0.5"
                >
                  {Array.from({ length: READER_COLUMNS }, (_, column) => (
                    <span
                      key={column}
                      className={cn(
                        'size-2 rounded-[2px] transition-colors duration-300',
                        column < readersInRow(readers, row)
                          ? direct
                            ? 'bg-fd-warning opacity-[calc(0.45+0.55*var(--pulse,0))]'
                            : 'bg-brand opacity-[calc(0.45+0.55*var(--pulse,0))]'
                          : 'bg-fd-border/60',
                      )}
                    />
                  ))}
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>

      <div className="flex flex-wrap items-center justify-center gap-3">
        <Segmented
          label="How the UI reads Kafka"
          value={mode}
          onChange={changeMode}
          options={[
            { value: 'projection', label: 'With klens' },
            { value: 'direct', label: 'Each page asks Kafka' },
          ]}
        />
        <Segmented
          label="Readers"
          value={readers}
          onChange={changeReaders}
          options={READER_COUNTS.map((count) => ({
            value: count,
            label: count === 1 ? '1 reader' : `${count} readers`,
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
  };
  const visible: Record<BeamKind, boolean> = { lane: true, push: !direct, poll: direct };

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

      <g className="text-fd-muted-foreground mask-[linear-gradient(to_right,transparent,white_12%,white_88%,transparent)]">
        {geometry.edges.map((path) => (
          <path key={path} d={path} fill="none" stroke="currentColor" strokeOpacity={0.25} />
        ))}
      </g>

      {kinds.map((kind) => (
        <g
          key={kind}
          className={cn(
            'transition-opacity duration-500',
            kind === 'poll' ? 'text-fd-warning' : 'text-brand',
            !visible[kind] && 'opacity-0',
            kind === 'lane' && direct && 'opacity-15',
          )}
        >
          {paths[kind].map((path, index) => {
            const hidden = kind !== 'lane' && !activeRows[index];

            return (
              <g
                key={index}
                className={cn('transition-opacity duration-500', hidden && 'opacity-0')}
              >
                <path
                  d={path}
                  fill="none"
                  stroke="currentColor"
                  strokeOpacity={0.25}
                  strokeWidth={1}
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
                        { length: HEAD_LENGTH, width: 3, opacity: 1 },
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
      className="flex shrink-0 rounded-full border bg-fd-card/85 p-0.5 text-xs shadow-lg shadow-black/5 backdrop-blur-md"
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

  const freshest = Math.max(...model.laneRefreshed);
  targets.hub?.style.setProperty(
    '--flash',
    String(withPulses ? clamp(1 - (model.time - freshest) / 0.8) : 0),
  );

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
