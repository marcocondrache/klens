'use client';

import { type ComponentProps, useEffect, useRef } from 'react';
import { cn } from '@/lib/cn';
import { DITHER_CELL_PX } from './dither';
import { useMotionAllowed } from './motion';
import { useInView } from './use-in-view';

const BAYER_4X4 = [0, 8, 2, 10, 12, 4, 14, 6, 3, 11, 1, 9, 15, 7, 13, 5].map((v) => (v + 0.5) / 16);

const TURN_MS = 16_000;
const RESTING_ANGLE = (-3 * Math.PI) / 4;

class DitheredRing {
  readonly cols: number;
  readonly rows: number;
  private readonly cells: Int32Array;
  private readonly normals: Float32Array;
  private readonly thresholds: Float32Array;

  constructor(cols: number, rows: number) {
    this.cols = cols;
    this.rows = rows;

    const size = Math.min(cols, rows);
    const ring = (size * 21) / 64;
    const tube = (size * 7) / 64;
    const cells: number[] = [];
    const normals: number[] = [];
    const thresholds: number[] = [];

    for (let y = 0; y < rows; y++) {
      for (let x = 0; x < cols; x++) {
        const dx = x + 0.5 - cols / 2;
        const dy = y + 0.5 - rows / 2;
        const radius = Math.hypot(dx, dy);
        const across = (radius - ring) / tube;
        if (radius === 0 || Math.abs(across) >= 1) continue;

        cells.push(y * cols + x);
        normals.push(
          (across * dx) / radius,
          (across * dy) / radius,
          Math.sqrt(1 - across * across),
        );
        thresholds.push(BAYER_4X4[(x % 4) + 4 * (y % 4)]);
      }
    }

    this.cells = Int32Array.from(cells);
    this.normals = Float32Array.from(normals);
    this.thresholds = Float32Array.from(thresholds);
  }

  render(pixels: Uint32Array, angle: number, color: number) {
    pixels.fill(0);

    const [lx, ly, lz] = normalize(0.75 * Math.cos(angle), 0.75 * Math.sin(angle), 0.65);
    const [hx, hy, hz] = normalize(lx, ly, lz + 1);

    for (let i = 0; i < this.cells.length; i++) {
      const nx = this.normals[i * 3];
      const ny = this.normals[i * 3 + 1];
      const nz = this.normals[i * 3 + 2];

      const diffuse = Math.max(0, nx * lx + ny * ly + nz * lz);
      let specular = Math.max(0, nx * hx + ny * hy + nz * hz);
      specular *= specular;
      specular *= specular;
      specular *= specular;
      specular *= specular;

      const light = 0.03 + 0.8 * diffuse * diffuse + 0.45 * specular;
      if (light > this.thresholds[i]) pixels[this.cells[i]] = color;
    }
  }
}

function normalize(x: number, y: number, z: number): [number, number, number] {
  const length = Math.hypot(x, y, z);

  return [x / length, y / length, z / length];
}

function resolvePixel(probe: CanvasRenderingContext2D, cssColor: string): number {
  probe.fillStyle = cssColor;
  probe.fillRect(0, 0, 1, 1);
  const [r, g, b, a] = probe.getImageData(0, 0, 1, 1).data;

  return new Uint32Array(Uint8ClampedArray.of(r, g, b, a).buffer)[0];
}

export function Lens({ className, ...props }: ComponentProps<'canvas'>) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const angleRef = useRef(RESTING_ANGLE);
  const drawRef = useRef(() => {});
  const inView = useInView(canvasRef);
  const animate = useMotionAllowed() && inView;

  useEffect(() => {
    const canvas = canvasRef.current;
    const context = canvas?.getContext('2d');
    const probe = document.createElement('canvas').getContext('2d', { willReadFrequently: true });
    if (!canvas || !context || !probe) return;

    let ring: DitheredRing | null = null;
    let image: ImageData | null = null;
    let pixels: Uint32Array | null = null;
    let color = resolvePixel(probe, getComputedStyle(canvas).color);

    const draw = () => {
      if (!ring || !image || !pixels) return;

      ring.render(pixels, angleRef.current, color);
      context.putImageData(image, 0, 0);
    };

    const resize = () => {
      const cols = Math.max(1, Math.floor(canvas.clientWidth / DITHER_CELL_PX));
      const rows = Math.max(1, Math.floor(canvas.clientHeight / DITHER_CELL_PX));

      canvas.width = cols;
      canvas.height = rows;
      ring = new DitheredRing(cols, rows);
      image = context.createImageData(cols, rows);
      pixels = new Uint32Array(image.data.buffer);
      draw();
    };

    const resizeObserver = new ResizeObserver(resize);
    resizeObserver.observe(canvas);

    // The theme switch toggles a class on <html>, which changes the brand color.
    const themeObserver = new MutationObserver(() => {
      color = resolvePixel(probe, getComputedStyle(canvas).color);
      draw();
    });
    themeObserver.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['class'],
    });

    drawRef.current = draw;

    return () => {
      resizeObserver.disconnect();
      themeObserver.disconnect();
      drawRef.current = () => {};
    };
  }, []);

  useEffect(() => {
    if (!animate) return;

    let last = performance.now();
    let frame = 0;

    const tick = (now: number) => {
      angleRef.current += ((now - last) / TURN_MS) * 2 * Math.PI;
      last = now;
      drawRef.current();
      frame = requestAnimationFrame(tick);
    };
    frame = requestAnimationFrame(tick);

    return () => cancelAnimationFrame(frame);
  }, [animate]);

  return (
    <canvas
      ref={canvasRef}
      aria-hidden
      className={cn('pointer-events-none text-brand-mark [image-rendering:pixelated]', className)}
      {...props}
    />
  );
}
