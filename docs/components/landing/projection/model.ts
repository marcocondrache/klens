export type Mode = 'projection' | 'direct';

export type BeamKind = 'lane' | 'push' | 'poll';

export type Pulse = { kind: BeamKind; index: number; start: number };

/**
 * The ingest lanes with their default periods. The animation plays each lane faster than its
 * period, so the slow lanes still move while someone watches.
 */
export const lanes = [
  { name: 'topology', period: '10s', seconds: 5, pushes: false },
  { name: 'watermarks', period: '3s · 30s', seconds: 3, pushes: true },
  { name: 'offsets', period: '2s · 20s', seconds: 2.2, pushes: true },
  { name: 'configs', period: '60s', seconds: 9, pushes: false },
  { name: 'subjects', period: '30s', seconds: 7, pushes: false },
  { name: 'log dirs', period: '60s', seconds: 9.6, pushes: false },
  { name: 'acls · quotas · users', period: '60s', seconds: 10.4, pushes: false },
] as const;

export const READER_ROWS = 10;
export const READER_COLUMNS = 10;
export const READER_COUNTS = [1, 10, 100] as const;

export const WINDOW_SECONDS = 20;
export const BIN_SECONDS = 0.5;
export const BINS = WINDOW_SECONDS / BIN_SECONDS;

export const TRAVEL_SECONDS: Record<BeamKind, number> = {
  lane: 1.1,
  push: 0.8,
  poll: 1.5,
};

/** A page that asks Kafka itself refreshes every few seconds. */
const POLL_SECONDS = 3;
const PUSH_STAGGER_SECONDS = 0.04;

/** The reads per second with a projection, whatever the number of readers. */
const PROJECTION_READS_PER_SECOND = lanes.reduce((sum, lane) => sum + 1 / lane.seconds, 0);

/**
 * A time-lapse of one cluster and its readers. With a projection, the lanes read Kafka on their
 * own schedule and push frames to the readers. Without one, every reader asks Kafka itself.
 */
export class FlowModel {
  time = 0;
  pulses: Pulse[] = [];
  readonly laneRefreshed: number[] = lanes.map(() => -Infinity);
  readonly rowFlashed: number[] = Array.from({ length: READER_ROWS }, () => -Infinity);

  private reads: number[] = [];
  private laneDue: number[] = [];
  private rowDue: number[] = [];

  constructor(
    private mode: Mode,
    private readers: number,
  ) {
    this.restartSchedules();
  }

  setMode(mode: Mode) {
    if (mode === this.mode) return;

    this.mode = mode;
    this.restartSchedules();
  }

  setReaders(readers: number) {
    this.readers = readers;
    this.restartSchedules();
  }

  advance(seconds: number) {
    this.time += seconds;
    this.schedule();
    this.settle();
    this.prune();
  }

  /** Plays the model forward without drawing, so the load chart starts full. */
  fastForward(seconds: number) {
    for (let elapsed = 0; elapsed < seconds; elapsed += 0.05) this.advance(0.05);
  }

  /** The reads Kafka served in each bin of the window, oldest first. */
  loadBins(): number[] {
    const bins = new Array<number>(BINS).fill(0);
    const windowStart = this.time - WINDOW_SECONDS;

    for (const read of this.reads) {
      const bin = Math.floor((read - windowStart) / BIN_SECONDS);
      if (bin >= 0 && bin < BINS) bins[bin]++;
    }

    return bins;
  }

  /** The load on Kafka over the window, as a multiple of the load with a projection. */
  loadMultiple(): number {
    const windowStart = this.time - WINDOW_SECONDS;
    const recent = this.reads.filter((read) => read > windowStart).length;

    return recent / WINDOW_SECONDS / PROJECTION_READS_PER_SECOND;
  }

  private restartSchedules() {
    this.laneDue = lanes.map((lane, i) => this.time + ((i * 0.83) % lane.seconds));
    this.rowDue = Array.from(
      { length: READER_ROWS },
      (_, row) => this.time + this.pollInterval(row) * jitter(row),
    );
  }

  private pollInterval(row: number): number {
    return POLL_SECONDS / Math.max(1, readersInRow(this.readers, row));
  }

  private schedule() {
    if (this.mode === 'projection') {
      lanes.forEach((lane, i) => {
        while (this.laneDue[i] <= this.time) {
          this.pulses.push({ kind: 'lane', index: i, start: this.laneDue[i] });
          this.reads.push(this.laneDue[i]);
          this.laneDue[i] += lane.seconds;
        }
      });

      return;
    }

    for (let row = 0; row < READER_ROWS; row++) {
      if (readersInRow(this.readers, row) === 0) continue;

      while (this.rowDue[row] <= this.time) {
        this.pulses.push({ kind: 'poll', index: row, start: this.rowDue[row] });
        this.rowFlashed[row] = this.rowDue[row];
        this.rowDue[row] += this.pollInterval(row) * jitter(this.rowDue[row]);
      }
    }
  }

  /** Lands every pulse that reached the end of its beam. */
  private settle() {
    const flying: Pulse[] = [];
    const landed: Pulse[] = [];

    for (const pulse of this.pulses) {
      if (this.time >= pulse.start + TRAVEL_SECONDS[pulse.kind]) landed.push(pulse);
      else flying.push(pulse);
    }

    this.pulses = flying;

    for (const pulse of landed) {
      const arrival = pulse.start + TRAVEL_SECONDS[pulse.kind];

      switch (pulse.kind) {
        case 'lane':
          this.laneRefreshed[pulse.index] = arrival;
          if (lanes[pulse.index].pushes && this.mode === 'projection') this.push(arrival);
          break;
        case 'push':
          this.rowFlashed[pulse.index] = arrival;
          break;
        case 'poll':
          this.reads.push(arrival);
          break;
      }
    }
  }

  /** A lane that commits a change sends one frame down each open stream. */
  private push(at: number) {
    for (let row = 0; row < READER_ROWS; row++) {
      if (readersInRow(this.readers, row) === 0) continue;

      this.pulses.push({ kind: 'push', index: row, start: at + row * PUSH_STAGGER_SECONDS });
    }
  }

  /** Reads land out of order, since lanes and polls interleave, so this filters rather than slices. */
  private prune() {
    const windowStart = this.time - WINDOW_SECONDS;

    if (this.reads.some((read) => read <= windowStart)) {
      this.reads = this.reads.filter((read) => read > windowStart);
    }
  }
}

/** A stable spread between 0.75 and 1.25, so polls from one row do not march in step. */
function jitter(seed: number): number {
  const x = Math.sin(seed * 12.9898) * 43758.5453;

  return 0.75 + 0.5 * (x - Math.floor(x));
}

/** Readers fill the rows in turn, so ten readers light one cell in every row. */
export function readersInRow(readers: number, row: number): number {
  return Math.floor(readers / READER_ROWS) + (row < readers % READER_ROWS ? 1 : 0);
}
