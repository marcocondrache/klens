import type { ReactNode } from 'react';
import type { StaticImageData } from 'next/image';
import { cn } from '@/lib/cn';
import { DITHER_CELL_PX } from './dither';
import dunes from './scenery/dunes.png';
import mist from './scenery/mist.png';
import night from './scenery/night.png';
import ridges from './scenery/ridges.png';
import sea from './scenery/sea.png';

const scenes = { dunes, mist, night, ridges, sea } satisfies Record<string, StaticImageData>;

export type Scene = keyof typeof scenes;

export function Scenery({
  scene,
  position = 'center',
  className,
  children,
}: {
  scene: Scene;
  position?: string;
  className?: string;
  children: ReactNode;
}) {
  const image = scenes[scene];

  return (
    <div
      className={cn(
        'relative isolate overflow-hidden rounded-2xl border [&>*+*]:shadow-2xl [&>*+*]:shadow-black/40',
        className,
      )}
    >
      <div
        aria-hidden
        className="absolute inset-0 -z-1 bg-no-repeat [image-rendering:pixelated]"
        style={{
          backgroundImage: `url(${image.src})`,
          backgroundSize: `${image.width * DITHER_CELL_PX}px ${image.height * DITHER_CELL_PX}px`,
          backgroundPosition: position,
        }}
      />
      {children}
    </div>
  );
}
