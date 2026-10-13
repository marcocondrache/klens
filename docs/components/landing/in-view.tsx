'use client';

import { type ComponentProps, useRef } from 'react';
import { useInView } from './use-in-view';

/** Marks whether its content is on screen, so the CSS loops inside pause when it is not. */
export function InView(props: ComponentProps<'div'>) {
  const ref = useRef<HTMLDivElement>(null);
  const inView = useInView(ref);

  return <div ref={ref} data-active={inView} {...props} />;
}
