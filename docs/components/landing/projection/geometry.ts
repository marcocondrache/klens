type Point = { x: number; y: number };

export type Box = { left: number; top: number; right: number; bottom: number };

export type Nodes = {
  kafka: Box;
  core: Box;
  readers: Box;
  laneRows: Box[];
  readerRows: Box[];
};

export type Geometry = {
  width: number;
  height: number;
  lanes: string[];
  pushes: string[];
  polls: string[];
};

/**
 * Lays the beams between the nodes. Side by side, beams leave the right edge of a node and enter
 * the left edge of the next. Stacked, as on a phone, they run from bottom edge to top edge.
 */
export function layBeams(width: number, height: number, nodes: Nodes): Geometry {
  const { kafka, core, readers, laneRows, readerRows } = nodes;
  const stacked = core.top >= kafka.bottom;

  if (stacked) {
    return {
      width,
      height,
      lanes: laneRows.map((_, i, all) =>
        curve(alongBottom(kafka, i, all.length), alongTop(core, i, all.length), 'vertical'),
      ),
      pushes: readerRows.map((_, i, all) =>
        curve(alongBottom(core, i, all.length), alongTop(readers, i, all.length), 'vertical'),
      ),
      polls: readerRows.map((_, i, all) =>
        curve(alongTop(readers, i, all.length), alongBottom(kafka, i, all.length), 'vertical'),
      ),
    };
  }

  return {
    width,
    height,
    lanes: laneRows.map((row, i, all) =>
      curve(alongRight(kafka, i, all.length), { x: core.left, y: middle(row) }, 'horizontal'),
    ),
    pushes: readerRows.map((row, i, all) =>
      curve(alongRight(core, i, all.length), { x: readers.left, y: middle(row) }, 'horizontal'),
    ),
    polls: readerRows.map((row, i, all) =>
      curve({ x: readers.left, y: middle(row) }, alongRight(kafka, i, all.length), 'horizontal'),
    ),
  };
}

function curve(from: Point, to: Point, direction: 'horizontal' | 'vertical'): string {
  if (direction === 'horizontal') {
    const bend = (to.x - from.x) / 2;

    return `M ${from.x} ${from.y} C ${from.x + bend} ${from.y}, ${to.x - bend} ${to.y}, ${to.x} ${to.y}`;
  }

  const bend = (to.y - from.y) / 2;

  return `M ${from.x} ${from.y} C ${from.x} ${from.y + bend}, ${to.x} ${to.y - bend}, ${to.x} ${to.y}`;
}

/** Spreads beam ends evenly along an edge, keeping clear of the rounded corners. */
function spread(start: number, end: number, index: number, count: number): number {
  const inset = (end - start) * 0.12;

  return start + inset + ((end - start - 2 * inset) * (index + 0.5)) / count;
}

function alongRight(box: Box, index: number, count: number): Point {
  return { x: box.right, y: spread(box.top, box.bottom, index, count) };
}

function alongTop(box: Box, index: number, count: number): Point {
  return { x: spread(box.left, box.right, index, count), y: box.top };
}

function alongBottom(box: Box, index: number, count: number): Point {
  return { x: spread(box.left, box.right, index, count), y: box.bottom };
}

function middle(box: Box): number {
  return (box.top + box.bottom) / 2;
}
