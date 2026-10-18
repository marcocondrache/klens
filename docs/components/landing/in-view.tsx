'use client';

import { type ComponentProps, useRef } from 'react';
import { useInView } from './use-in-view';

export function InView(props: ComponentProps<'div'>) {
  const ref = useRef<HTMLDivElement>(null);
  const inView = useInView(ref);

  return <div ref={ref} data-active={inView} {...props} />;
}
