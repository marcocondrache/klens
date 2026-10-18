'use client';

import { type RefObject, useEffect, useRef, useState } from 'react';
import dynamic from 'next/dynamic';
import type { GrainGradientParams } from '@paper-design/shaders-react';
import { cn } from '@/lib/cn';
import { DITHER_CELL_PX } from './dither';
import { useMotionAllowed } from './motion';
import { useInView } from './use-in-view';

const Dithering = dynamic(
  () => import('@paper-design/shaders-react').then((mod) => mod.Dithering),
  { ssr: false },
);

const GrainGradient = dynamic(
  () => import('@paper-design/shaders-react').then((mod) => mod.GrainGradient),
  { ssr: false },
);

const TRANSPARENT = '#00000000';
const BRAND = ['--color-brand'];
const GLOWS = ['--color-brand-glow', '--color-amber-glow'];

export function Warp({ className }: { className?: string }) {
  const ref = useRef<HTMLDivElement>(null);
  const animate = useAnimate(ref);
  const colors = useShaderColors(BRAND);

  return (
    <div ref={ref} aria-hidden className={cn('pointer-events-none', className)}>
      {colors && (
        <Dithering
          className="size-full"
          colorBack={TRANSPARENT}
          colorFront={colors[0]}
          shape="warp"
          type="4x4"
          size={DITHER_CELL_PX}
          speed={animate ? 0.4 : 0}
          minPixelRatio={1}
        />
      )}
    </div>
  );
}

export function Grain({
  shape,
  className,
}: {
  shape: GrainGradientParams['shape'];
  className?: string;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const animate = useAnimate(ref);
  const colors = useShaderColors(GLOWS);

  return (
    <div ref={ref} aria-hidden className={cn('pointer-events-none', className)}>
      {colors && (
        <GrainGradient
          className="size-full animate-fd-fade-in"
          colors={[...colors, TRANSPARENT]}
          colorBack={TRANSPARENT}
          shape={shape}
          softness={1}
          intensity={0.9}
          noise={0.5}
          speed={animate ? 1 : 0}
          minPixelRatio={1}
          maxPixelCount={1920 * 1080}
        />
      )}
    </div>
  );
}

function useAnimate(ref: RefObject<HTMLElement | null>): boolean {
  const inView = useInView(ref);

  return useMotionAllowed() && inView;
}

/**
 * The shaders parse only hex, rgb, and hsl, while the theme tokens are oklch. A canvas reads any
 * CSS color back as rgb.
 */
function useShaderColors(tokens: string[]): string[] | null {
  const [colors, setColors] = useState<string[] | null>(null);

  useEffect(() => {
    const probe = document.createElement('canvas').getContext('2d', { willReadFrequently: true });
    if (!probe) return;

    const toRgb = (cssColor: string) => {
      probe.clearRect(0, 0, 1, 1);
      probe.fillStyle = cssColor;
      probe.fillRect(0, 0, 1, 1);
      const [r, g, b, a] = probe.getImageData(0, 0, 1, 1).data;

      return `rgba(${r}, ${g}, ${b}, ${a / 255})`;
    };

    const resolve = () => {
      const style = getComputedStyle(document.documentElement);
      setColors(tokens.map((token) => toRgb(style.getPropertyValue(token))));
    };
    resolve();

    // The theme switch toggles a class on <html>, which changes the tokens.
    const observer = new MutationObserver(resolve);
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] });

    return () => observer.disconnect();
  }, [tokens]);

  return colors;
}
