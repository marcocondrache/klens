import { useId, type ComponentProps } from "react";

import { cn } from "@/lib/utils";

/**
 * The klens mark: focus brackets around one message. The brackets follow the text color and the
 * message keeps the brand color, so the mark sits in either theme.
 */
export function LogoMark({ className, ...props }: ComponentProps<"svg">) {
  return (
    <svg
      viewBox="0 0 64 64"
      fill="none"
      aria-hidden
      className={cn("size-5 shrink-0", className)}
      {...props}
    >
      <g stroke="currentColor" strokeWidth="6" strokeLinecap="round" strokeLinejoin="round">
        <path d="M10 23.6v-4.4a9.2 9.2 0 0 1 9.2-9.2h4.4" />
        <path d="M40.4 10h4.4a9.2 9.2 0 0 1 9.2 9.2v4.4" />
        <path d="M54 40.4v4.4a9.2 9.2 0 0 1-9.2 9.2h-4.4" />
        <path d="M23.6 54h-4.4a9.2 9.2 0 0 1-9.2-9.2v-4.4" />
      </g>
      <line
        x1="23"
        y1="32"
        x2="41"
        y2="32"
        stroke="var(--brand)"
        strokeWidth="8"
        strokeLinecap="round"
      />
    </svg>
  );
}

const W = 320;
const H = 160;
const CX = W / 2;
const CY = H / 2;
/** Stream period in viewBox units; each row repeats every PERIOD so the loop is seamless. */
const PERIOD = 96;
const ROW_GAP = 19;
const STROKE = 6.5;
/** Half the bracket frame's side. */
const FRAME = 36;
const LENS = FRAME - 4;
const ZOOM = 1.28;
const FOCUS = 12;

/** One period of messages per partition, as `[start, length]`. */
const PARTITIONS = [
  {
    y: CY - ROW_GAP,
    seconds: 9,
    messages: [
      [0, 18],
      [26, 10],
      [44, 30],
      [82, 8],
    ],
  },
  {
    y: CY,
    seconds: 7,
    messages: [
      [0, 26],
      [34, 14],
      [56, 8],
      [72, 16],
    ],
  },
  {
    y: CY + ROW_GAP,
    seconds: 11,
    messages: [
      [0, 10],
      [18, 22],
      [48, 12],
      [68, 20],
    ],
  },
] as const;

const REPEATS = Array.from(
  { length: Math.ceil(W / PERIOD) + 3 },
  (_, index) => (index - 2) * PERIOD,
);

function Stream() {
  return PARTITIONS.map((partition) => (
    <g
      key={partition.y}
      className="logo-stream-row"
      style={{ animationDuration: `${partition.seconds}s` }}
      strokeWidth={STROKE}
      strokeLinecap="round"
    >
      {REPEATS.flatMap((offset) =>
        partition.messages.map(([start, length]) => (
          <line
            key={`${offset}-${start}`}
            x1={offset + start}
            y1={partition.y}
            x2={offset + start + length}
            y2={partition.y}
          />
        )),
      )}
    </g>
  ));
}

function bracketPaths(side: number) {
  const a = CX - side;
  const b = CX + side;
  const t = CY - side;
  const u = CY + side;
  const arm = side * 0.62;
  const r = side * 0.42;

  return [
    `M${a} ${t + arm}V${t + r}a${r} ${r} 0 0 1 ${r}-${r}H${a + arm}`,
    `M${b - arm} ${t}H${b - r}a${r} ${r} 0 0 1 ${r} ${r}V${t + arm}`,
    `M${b} ${u - arm}V${u - r}a${r} ${r} 0 0 1-${r} ${r}H${b - arm}`,
    `M${a + arm} ${u}H${a + r}a${r} ${r} 0 0 1-${r}-${r}V${u - arm}`,
  ];
}

/**
 * The mark as a scene: partitions stream through the brackets, which magnify what passes under
 * them, while the message in focus stays lit. Motion stops under `prefers-reduced-motion`.
 */
export function LogoStream({
  glow = false,
  className,
  ...props
}: ComponentProps<"svg"> & { glow?: boolean }) {
  // `useId` may contain characters that break `url(#…)` references.
  const id = `logo${useId().replace(/[^\w-]/g, "")}`;
  const fade = `${id}-fade`;
  const outside = `${id}-outside`;
  const glass = `${id}-glass`;
  const lens = `${id}-lens`;
  const halo = `${id}-halo`;

  return (
    <svg
      viewBox={`0 0 ${W} ${H}`}
      fill="none"
      aria-hidden
      className={cn("logo-stream overflow-visible", className)}
      {...props}
    >
      <defs>
        <linearGradient id={fade} x1="0" x2={W} gradientUnits="userSpaceOnUse">
          <stop offset="0" stopColor="#fff" stopOpacity="0" />
          <stop offset="0.28" stopColor="#fff" />
          <stop offset="0.72" stopColor="#fff" />
          <stop offset="1" stopColor="#fff" stopOpacity="0" />
        </linearGradient>
        <mask id={outside}>
          <rect width={W} height={H} fill={`url(#${fade})`} />
          <rect
            x={CX - FRAME - 7}
            y={CY - FRAME - 7}
            width={2 * FRAME + 14}
            height={2 * FRAME + 14}
            rx={FRAME * 0.5}
            fill="#000"
          />
        </mask>
        <radialGradient id={glass} cx={CX} cy={CY} r={LENS} gradientUnits="userSpaceOnUse">
          <stop offset="0.55" stopColor="#fff" />
          <stop offset="1" stopColor="#fff" stopOpacity="0" />
        </radialGradient>
        <radialGradient id={halo} cx={CX} cy={CY} r={FRAME * 2.2} gradientUnits="userSpaceOnUse">
          <stop offset="0" stopColor="var(--brand)" stopOpacity="0.28" />
          <stop offset="1" stopColor="var(--brand)" stopOpacity="0" />
        </radialGradient>
        <mask id={lens}>
          <rect
            x={CX - LENS}
            y={CY - LENS}
            width={2 * LENS}
            height={2 * LENS}
            rx={LENS * 0.4}
            fill={`url(#${glass})`}
          />
          <line
            x1={CX - FOCUS}
            y1={CY}
            x2={CX + FOCUS}
            y2={CY}
            stroke="#000"
            strokeWidth={STROKE * 1.6 + 8}
            strokeLinecap="round"
          />
        </mask>
      </defs>

      {glow ? <circle cx={CX} cy={CY} r={FRAME * 2.2} fill={`url(#${halo})`} /> : null}

      <g mask={`url(#${outside})`} className="stroke-foreground/22">
        <Stream />
      </g>
      <g mask={`url(#${lens})`}>
        <g
          transform={`translate(${CX} ${CY}) scale(${ZOOM}) translate(${-CX} ${-CY})`}
          className="stroke-foreground/55"
        >
          <Stream />
        </g>
      </g>

      <g stroke="currentColor" strokeWidth={STROKE} strokeLinecap="round" strokeLinejoin="round">
        {bracketPaths(FRAME).map((d) => (
          <path key={d} d={d} />
        ))}
      </g>
      <line
        x1={CX - FOCUS}
        y1={CY}
        x2={CX + FOCUS}
        y2={CY}
        stroke="var(--brand)"
        strokeWidth={STROKE * 1.6}
        strokeLinecap="round"
        className="drop-shadow-[0_0_10px_color-mix(in_oklab,var(--brand)_70%,transparent)]"
      />
    </svg>
  );
}
