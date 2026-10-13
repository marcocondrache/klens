'use client';

import { type ReactNode, useId, useState } from 'react';
import { cn } from '@/lib/cn';

export type ShowcaseTab = { value: string; label: string; content: ReactNode };

/** Tabs over a few features. Hidden panels stay in the page, so search engines still read them. */
export function Showcase({ tabs }: { tabs: ShowcaseTab[] }) {
  const id = useId();
  const [active, setActive] = useState(tabs[0]?.value);

  return (
    <div className="col-span-full flex flex-col gap-8">
      <div
        role="tablist"
        aria-label="Features"
        className="mx-auto flex max-w-full overflow-x-auto rounded-full border bg-fd-card p-1 shadow-lg shadow-black/5"
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
