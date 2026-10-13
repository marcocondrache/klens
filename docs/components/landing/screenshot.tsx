import Image from 'next/image';
import { cn } from '@/lib/cn';
import topicsDark from './screenshots/topics-dark.webp';
import topicsLight from './screenshots/topics-light.webp';

const alt =
  'The klens topics page, listing the topics of a cluster with their partitions, sizes, and rates';

/**
 * Both copies load lazily, so the browser fetches only the theme that shows, and nothing on
 * phones where the screenshot is hidden. High priority keeps the visible copy fast, because it
 * is the largest paint on desktop.
 */
export function Screenshot({ className }: { className?: string }) {
  return (
    <div className={cn('overflow-hidden', className)}>
      <Image
        src={topicsLight}
        alt={alt}
        unoptimized
        fetchPriority="high"
        className="w-full dark:hidden"
      />
      <Image
        src={topicsDark}
        alt={alt}
        unoptimized
        fetchPriority="high"
        className="hidden w-full dark:block"
      />
    </div>
  );
}
