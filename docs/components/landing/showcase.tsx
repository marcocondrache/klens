'use client';

import { type ReactNode, useId, useState } from 'react';
import { cn } from '@/lib/cn';
import { cardHeading, split } from './variants';

export type ShowcaseItem = { value: string; title: string; body: ReactNode; demo: ReactNode };

export function Showcase({ items }: { items: ShowcaseItem[] }) {
  const id = useId();
  const [active, setActive] = useState(items[0]?.value);

  return (
    <div className={split('lg:items-center')}>
      <div className="flex flex-col">
        {items.map((item) => {
          const open = item.value === active;

          return (
            <div
              key={item.value}
              className={cn(
                'border-s-2 py-4 ps-6 transition-colors',
                open ? 'border-brand' : 'border-fd-border',
              )}
            >
              <h3 className={cardHeading()}>
                <button
                  id={`${id}-${item.value}-button`}
                  type="button"
                  aria-expanded={open}
                  aria-controls={`${id}-${item.value}-body`}
                  onClick={() => setActive(item.value)}
                  className={cn(
                    'rounded-sm text-start transition-colors',
                    'focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-brand',
                    open
                      ? 'text-fd-foreground'
                      : 'text-fd-muted-foreground hover:text-fd-foreground',
                  )}
                >
                  {item.title}
                </button>
              </h3>
              <div
                id={`${id}-${item.value}-body`}
                role="region"
                aria-labelledby={`${id}-${item.value}-button`}
                hidden={!open}
                className="mt-4 flex animate-fd-fade-in flex-col gap-6"
              >
                {item.body}
              </div>
            </div>
          );
        })}
      </div>

      {items.map((item) => (
        <div key={item.value} hidden={item.value !== active} className="animate-fd-fade-in">
          {item.demo}
        </div>
      ))}
    </div>
  );
}
