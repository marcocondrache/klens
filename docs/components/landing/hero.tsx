import Link from 'next/link';
import { ArrowRightIcon } from 'lucide-react';
import { repoUrl } from '@/lib/shared';
import { GitHubIcon } from './github-icon';
import { Lens } from './lens';
import { MotionToggle } from './motion-toggle';
import { Screenshot } from './screenshot';
import { Grain } from './shaders';
import { button } from './variants';

export function Hero() {
  return (
    <section className="relative mx-auto flex h-[80vh] max-h-[960px] min-h-[680px] w-full max-w-[1400px] overflow-hidden rounded-2xl border bg-fd-card/30">
      <Grain shape="corners" className="absolute inset-0 opacity-70 dark:opacity-60" />
      <Lens className="absolute size-[560px] max-lg:top-[440px] max-lg:-left-[140px] md:max-lg:hidden lg:-top-[120px] lg:-right-[160px] lg:size-[700px] xl:-right-[60px]" />

      <div className="relative z-2 flex size-full flex-col px-6 py-12 max-md:items-center max-md:text-center md:px-12">
        <h1 className="mb-6 text-4xl leading-[1.05] font-medium tracking-tight text-fd-foreground md:text-5xl xl:text-6xl">
          Your Kafka clusters,
          <br />
          in <span className="text-brand">focus</span>.
        </h1>
        <p className="mb-6 max-w-md text-fd-muted-foreground md:text-lg">
          Browse topics, records, consumer groups, and schemas across your clusters, and let AI
          agents read them over MCP.
        </p>
        <div className="flex flex-wrap items-center gap-3 max-md:justify-center">
          <Link href="/docs/getting-started" className={button('primary')}>
            Get started
            <ArrowRightIcon className="size-4" />
          </Link>
          <a
            href={repoUrl}
            rel="noreferrer noopener"
            target="_blank"
            className={button('secondary')}
          >
            <GitHubIcon className="size-4" />
            GitHub
          </a>
        </div>
      </div>

      <Screenshot className="absolute top-[460px] left-[22%] w-[1200px] max-w-none rounded-xl border-2 shadow-2xl shadow-black/20 max-md:hidden" />
      <MotionToggle className="absolute bottom-4 left-4 z-2" />
    </section>
  );
}
