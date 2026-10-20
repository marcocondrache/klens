'use client';

import { type ReactNode, useId, useState } from 'react';
import { cn } from '@/lib/cn';
import { split } from './variants';

export type ShowcaseTab = { value: string; label: string; content: ReactNode };

export function Showcase({ heading, tabs }: { heading: ReactNode; tabs: ShowcaseTab[] }) {
  const id = useId();
  const [active, setActive] = useState(tabs[0]?.value);

  return (
    <div className="flex flex-col gap-10">
      <div className={split('gap-6 lg:items-end')}>
        {heading}
        <div
          role="tablist"
          aria-label="Features"
          className="flex w-fit max-w-full overflow-x-auto rounded-full border bg-fd-card p-1 shadow-lg shadow-black/5 lg:justify-self-end"
        >
          {tabs.map((tab) => (
            <button
              key={tab.value}
              id={`${id}-${tab.value}-tab`}
              type="button"
              role="tab"
              aria-selected={active === tab.value}
              aria-controls={`${id}-${tab.value}-panel`}
              onClick={() => setActive(tab.value)}
              className={cn(
                'rounded-full px-4 py-2 text-sm font-medium text-nowrap transition-colors',
                'focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-brand',
                active === tab.value
                  ? 'bg-brand text-brand-foreground'
                  : 'text-fd-muted-foreground hover:text-fd-foreground',
              )}
            >
              {tab.label}
            </button>
          ))}
        </div>
      </div>

      {tabs.map((tab) => (
        <div
          key={tab.value}
          id={`${id}-${tab.value}-panel`}
          role="tabpanel"
          aria-labelledby={`${id}-${tab.value}-tab`}
          hidden={active !== tab.value}
          className="animate-fd-fade-in"
        >
          {tab.content}
        </div>
      ))}
    </div>
  );
}
