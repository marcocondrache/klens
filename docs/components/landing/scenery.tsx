import type { ReactNode } from 'react';
import type { StaticImageData } from 'next/image';
import { cn } from '@/lib/cn';
import dunes from './scenery/dunes.png';
import mist from './scenery/mist.png';
import night from './scenery/night.png';
import ridges from './scenery/ridges.png';
import sea from './scenery/sea.png';

/** Landscapes dithered into a few tones, stored at one pixel per dither cell. */
const scenes = { dunes, mist, night, ridges, sea } satisfies Record<string, StaticImageData>;

export type Scene = keyof typeof scenes;

/** Matches the cell of the shaders and the lens, so every dithered surface shares one grain. */
const CELL_PX = 3;

/**
 * A panel on a dithered landscape. The image scales by a whole number of pixels per cell and
 * crops to the panel, so the dither stays crisp at any panel size.
 */
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
          backgroundSize: `${image.width * CELL_PX}px ${image.height * CELL_PX}px`,
          backgroundPosition: position,
        }}
      />
      {children}
    </div>
  );
}
