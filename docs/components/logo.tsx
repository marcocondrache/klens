import type { ComponentProps } from 'react';
import { cn } from '@/lib/cn';

export function LogoMark({ className, ...props }: ComponentProps<'svg'>) {
  return (
    <svg
      viewBox="0 0 64 64"
      fill="none"
      aria-hidden
      className={cn('size-5 shrink-0 text-brand-mark', className)}
      {...props}
    >
      <circle cx="32" cy="32" r="21" stroke="currentColor" strokeWidth="14" />
    </svg>
  );
}
