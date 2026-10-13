import { type RefObject, useEffect, useState } from 'react';

export function useInView(ref: RefObject<Element | null>): boolean {
  const [inView, setInView] = useState(false);

  useEffect(() => {
    const element = ref.current;
    if (!element) return;

    const observer = new IntersectionObserver(([entry]) => setInView(entry.isIntersecting));
    observer.observe(element);

    return () => observer.disconnect();
  }, [ref]);

  return inView;
}
