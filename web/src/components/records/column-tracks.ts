const CELL_PAD = 16;

const HEADER_FONT = '500 14px "DM Sans Variable", sans-serif';
const MONO_FONT = '400 14px "Geist Mono Variable", ui-monospace, monospace';
const SANS_FONT = '400 14px "DM Sans Variable", sans-serif';

export type ColumnFit = {
  min: number;
  max?: number;
  flex?: boolean;
  font: "mono" | "sans";
};

export const RECORD_COLUMN_FIT: Record<string, ColumnFit> = {
  partition: { min: 40, max: 56, font: "mono" },
  offset: { min: 56, max: 128, font: "mono" },
  key: { min: 80, max: 200, font: "mono" },
  value: { min: 128, flex: true, font: "mono" },
  size: { min: 48, max: 80, font: "sans" },
  timestamp: { min: 140, max: 180, font: "sans" },
};

const DEFAULT_FIT: ColumnFit = { min: 80, flex: true, font: "sans" };

let measureCtx: CanvasRenderingContext2D | null | undefined;

function context(): CanvasRenderingContext2D | null {
  if (measureCtx !== undefined) return measureCtx;
  if (typeof document === "undefined") {
    measureCtx = null;
    return null;
  }
  const canvas = document.createElement("canvas");
  measureCtx = canvas.getContext("2d");
  return measureCtx;
}

function fallbackWidth(text: string, font: string) {
  const px = font === HEADER_FONT ? 7.4 : font === MONO_FONT ? 8.4 : 7.2;
  return text.length * px;
}

export function measureText(text: string, font: string) {
  const ctx = context();
  if (!ctx) return fallbackWidth(text, font);
  ctx.font = font;
  return ctx.measureText(text).width;
}

export function fitColumnWidth(label: string, samples: string[], fit: ColumnFit) {
  const cellFont = fit.font === "mono" ? MONO_FONT : SANS_FONT;
  let width = measureText(label, HEADER_FONT) + CELL_PAD;
  for (const sample of samples) {
    width = Math.max(width, measureText(sample, cellFont) + CELL_PAD);
  }
  width = Math.max(fit.min, Math.ceil(width));
  if (fit.max != null) width = Math.min(width, fit.max);
  return width;
}

export function columnTracks(
  columnIds: string[],
  labels: Record<string, string>,
  samples: Record<string, string[]>,
) {
  return columnIds
    .map((id) => {
      const fit = RECORD_COLUMN_FIT[id] ?? DEFAULT_FIT;
      if (fit.flex) return `minmax(${fit.min}px, 1fr)`;
      const width = fitColumnWidth(labels[id] ?? id, samples[id] ?? [], fit);
      return `${width}px`;
    })
    .join(" ");
}
