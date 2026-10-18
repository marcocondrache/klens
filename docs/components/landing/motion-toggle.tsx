'use client';

import { useEffect } from 'react';
import { PauseIcon, PlayIcon } from 'lucide-react';
import { cn } from '@/lib/cn';
import { setReaderPaused, useReaderPaused, useReducedMotion } from './motion';

export function MotionToggle({ className }: { className?: string }) {
  const paused = useReaderPaused();
  const reducedMotion = useReducedMotion();

  useEffect(() => {
    document.documentElement.dataset.motion = paused ? 'paused' : 'running';
  }, [paused]);

  if (reducedMotion) return null;

  return (
    <button
      type="button"
      aria-pressed={paused}
      aria-label="Pause animations"
      title={paused ? 'Play animations' : 'Pause animations'}
      onClick={() => setReaderPaused(!paused)}
      className={cn(
        'rounded-full border bg-fd-background/60 p-2 text-fd-muted-foreground backdrop-blur-sm transition-colors hover:text-fd-foreground',
        'focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-brand',
        className,
      )}
    >
      {paused ? <PlayIcon className="size-3.5" /> : <PauseIcon className="size-3.5" />}
    </button>
  );
}
