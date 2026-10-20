import { cn } from '@/lib/cn';

type ButtonVariant = 'primary' | 'secondary';

const buttonVariants: Record<ButtonVariant, string> = {
  primary: 'bg-brand text-brand-foreground hover:bg-brand/90',
  secondary: 'border bg-fd-secondary text-fd-secondary-foreground hover:bg-fd-accent',
};

export function button(variant: ButtonVariant = 'primary', className?: string) {
  return cn(
    'inline-flex items-center justify-center gap-2 rounded-full px-5 py-2.5 font-medium tracking-tight transition-colors',
    'focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-brand',
    buttonVariants[variant],
    className,
  );
}

export function card(className?: string) {
  return cn('rounded-2xl border bg-fd-card p-6 text-sm shadow-lg shadow-black/5', className);
}

export function sectionHeading(className?: string) {
  return cn(
    'text-3xl font-medium tracking-tight text-balance text-fd-foreground lg:text-4xl',
    className,
  );
}

/** Every row shares these two tracks, so prose lines up on the left and demos on the right. */
export function split(className?: string) {
  return cn(
    'grid grid-cols-1 gap-8 lg:grid-cols-[minmax(0,2fr)_minmax(0,3fr)] lg:gap-12',
    className,
  );
}

export function cardHeading(className?: string) {
  return cn('mb-4 text-xl font-medium tracking-tight text-fd-foreground lg:text-2xl', className);
}
