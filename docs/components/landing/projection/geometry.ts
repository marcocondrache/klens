import { lanes as ingestLanes } from './model';

type Point = { x: number; y: number };

export type Box = { left: number; top: number; right: number; bottom: number };

export type Nodes = {
  kafka: Box;
  hub: Box;
  readers: Box;
  readerRows: Box[];
};

export type Geometry = {
  width: number;
  height: number;
  lanes: string[];
  pushes: string[];
  polls: string[];
  /** Faint lines that carry the flow in from the left edge and out past the right edge. */
  edges: string[];
};

/** The lens ring's outer radius, as a share of its canvas: radius 21 plus half its stroke of 14. */
const RING_RADIUS = 28 / 64;

/** How far from the horizontal, in degrees, beams spread round the ring. */
const RING_SPREAD = 38;

const EDGE_LINES = 6;

/** The height of the ribbon the edge lines form at the edge, as a share of the stage. */
const EDGE_RIBBON = 0.45;

/**
 * Lays the beams between the nodes. Side by side, the lanes converge on the left of the lens ring
 * and the frames fan out from its right. Stacked, as on a phone, they run from top to bottom.
 */
export function layBeams(width: number, height: number, nodes: Nodes): Geometry {
  const { kafka, hub, readers, readerRows } = nodes;
  const stacked = hub.top >= kafka.bottom;
  const laneCount = ingestLanes.length;
  const lanes = ingestLanes.map((_, i) => i);

  if (stacked) {
    return {
      width,
      height,
      lanes: lanes.map((i) =>
        curve(alongBottom(kafka, i, laneCount), onRing(hub, 270 + fan(i, laneCount)), 'vertical'),
      ),
      pushes: readerRows.map((_, i, all) =>
        curve(onRing(hub, 90 - fan(i, all.length)), alongTop(readers, i, all.length), 'vertical'),
      ),
      polls: readerRows.map((_, i, all) =>
        curve(alongTop(readers, i, all.length), alongBottom(kafka, i, all.length), 'vertical'),
      ),
      edges: [],
    };
  }

  const edgeRows = Array.from({ length: EDGE_LINES }, (_, i) => i);
  const ribbon = (box: Box, i: number) => {
    const half = (height * EDGE_RIBBON) / 2;

    return spread(middle(box) - half, middle(box) + half, i, EDGE_LINES);
  };

  return {
    width,
    height,
    lanes: lanes.map((i) =>
      curve(alongRight(kafka, i, laneCount), onRing(hub, 180 - fan(i, laneCount)), 'horizontal'),
    ),
    pushes: readerRows.map((row, i, all) =>
      curve(onRing(hub, fan(i, all.length)), { x: readers.left, y: middle(row) }, 'horizontal'),
    ),
    polls: readerRows.map((row, i, all) =>
      curve({ x: readers.left, y: middle(row) }, alongRight(kafka, i, all.length), 'horizontal'),
    ),
    edges: [
      ...edgeRows.map((i) =>
        curve({ x: 0, y: ribbon(kafka, i) }, alongLeft(kafka, i, EDGE_LINES), 'horizontal'),
      ),
      ...edgeRows.map((i) =>
        curve(
          alongRight(readers, i, EDGE_LINES),
          { x: width, y: ribbon(readers, i) },
          'horizontal',
        ),
      ),
    ],
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

/** An angle offset that spreads beam ends evenly within the ring spread, from top to bottom. */
function fan(index: number, count: number): number {
  return count === 1 ? 0 : -RING_SPREAD + (2 * RING_SPREAD * index) / (count - 1);
}

/** A point on the lens ring, at an angle in degrees measured clockwise from the right. */
function onRing(box: Box, degrees: number): Point {
  const radians = (degrees * Math.PI) / 180;
  const radius = Math.min(box.right - box.left, box.bottom - box.top) * RING_RADIUS;

  return {
    x: (box.left + box.right) / 2 + radius * Math.cos(radians),
    y: (box.top + box.bottom) / 2 + radius * Math.sin(radians),
  };
}

/** Spreads beam ends evenly along an edge, keeping clear of the rounded corners. */
function spread(start: number, end: number, index: number, count: number): number {
  const inset = (end - start) * 0.12;

  return start + inset + ((end - start - 2 * inset) * (index + 0.5)) / count;
}

function alongLeft(box: Box, index: number, count: number): Point {
  return { x: box.left, y: spread(box.top, box.bottom, index, count) };
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
