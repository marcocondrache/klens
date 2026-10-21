import type { ReactNode } from 'react';
import type { Metadata } from 'next';
import Link from 'next/link';
import { ArrowRightIcon } from 'lucide-react';
import { ServerCodeBlock, type ServerCodeBlockProps } from 'fumadocs-ui/components/codeblock.rsc';
import { cn } from '@/lib/cn';
import { source } from '@/lib/source';
import { getPageImageUrl, repoUrl } from '@/lib/shared';
import { AccessMatrix } from '@/components/landing/access';
import { AgentChat } from '@/components/landing/agents';
import { McpCeiling } from '@/components/landing/ceiling';
import { GitHubIcon } from '@/components/landing/github-icon';
import { Hero } from '@/components/landing/hero';
import { Lens } from '@/components/landing/lens';
import { LiveTail } from '@/components/landing/live-tail';
import { ObfuscatedRecord } from '@/components/landing/obfuscation';
import { Projection } from '@/components/landing/projection';
import { WritePrivileges } from '@/components/landing/privileges';
import { ConsumerLag, RecordSearch } from '@/components/landing/reading';
import { type Scene, Scenery } from '@/components/landing/scenery';
import { Grain, Warp } from '@/components/landing/shaders';
import { Showcase } from '@/components/landing/showcase';
import { type LogLine, Terminal } from '@/components/landing/terminal';
import {
  button,
  card,
  cardHeading,
  prose,
  sectionHeading,
  split,
} from '@/components/landing/variants';

const description =
  'A web UI and MCP server for Kafka. Browse topics, records, consumer groups, and schemas across your clusters from one Rust binary.';

export function generateMetadata(): Metadata {
  const index = source.getPage([]);

  return {
    title: 'klens, a web UI and MCP server for Kafka',
    description,
    openGraph: index ? { images: getPageImageUrl(index).url } : undefined,
  };
}

// The stock GitHub themes leave these tokens below 4.5:1 against the code block background.
const readableTokens = {
  'github-light': { '#d73a49': '#cf222e', '#22863a': '#116329', '#6a737d': '#59636e' },
  'github-dark': { '#6a737d': '#8b949e' },
};

const runCommand = `docker run --rm -p 8080:8080 \\
  -v "$PWD/config.yaml:/config.yaml:ro" \\
  ghcr.io/marcocondrache/klens:latest`;

const configYaml = `clusters:
  local:
    bootstrap_servers: [broker-1:9092]`;

const startupLog: LogLine[] = [
  { level: 'INFO', message: 'klens: configured kafka clusters', fields: 'clusters=["local"]' },
  {
    level: 'INFO',
    message: 'klens: auth is off, so klens refuses requests addressed to any host but these',
    fields: 'hosts=["localhost", "127.0.0.1", "[::1]"]',
  },
  {
    level: 'INFO',
    message: 'klens::kafka::ingest: starting ingestion lanes',
    fields: 'cluster=local',
  },
  { level: 'INFO', message: 'klens::server: listening', fields: 'bind=0.0.0.0:8080' },
];

export default function Page() {
  return (
    <div className="pt-4 pb-6 md:pb-12">
      <Hero />

      <div className="mx-auto mt-16 flex w-full max-w-[1400px] flex-col gap-24 px-6 md:px-12 lg:mt-24 lg:gap-28">
        <TryIt />

        <section className="flex flex-col gap-10 lg:gap-12">
          <SectionHeader title="Built for reading Kafka." />

          <Showcase
            items={[
              {
                value: 'search',
                title: 'Find any record.',
                body: (
                  <ShowcaseBody href="/docs/use/find-records" link="Find records in a topic">
                    Search a topic by text, partition, and time. klens runs the search on the
                    server, decodes Avro, JSON Schema, and Protobuf values, and exports the matches
                    as NDJSON.
                  </ShowcaseBody>
                ),
                demo: (
                  <DemoScene scene="dunes" position="20% 30%">
                    <RecordSearch />
                  </DemoScene>
                ),
              },
              {
                value: 'live',
                title: 'Follow a topic live.',
                body: (
                  <ShowcaseBody href="/docs/use/follow-a-topic-live" link="Follow a topic live">
                    Switch to Live and records appear as soon as Kafka stores them. On a busy topic
                    the tail samples the newest records, so the page stays responsive.
                  </ShowcaseBody>
                ),
                demo: (
                  <DemoScene scene="sea" position="65% 40%">
                    <LiveTail />
                  </DemoScene>
                ),
              },
              {
                value: 'lag',
                title: 'Watch consumer lag.',
                body: (
                  <ShowcaseBody
                    href="/docs/use/reset-consumer-offsets"
                    link="Reset a group's offsets"
                  >
                    See each group with its members and its lag on every partition. Reset its
                    offsets after a preview of the lag the change leaves, or delete the offsets of a
                    topic it no longer reads.
                  </ShowcaseBody>
                ),
                demo: (
                  <DemoScene scene="ridges" position="30% 35%">
                    <ConsumerLag />
                  </DemoScene>
                ),
              },
            ]}
          />

          <div className={split('lg:items-center')}>
            <FeatureText
              title="Manage more than topics."
              href="/docs/reference/privileges"
              link="See every privilege"
            >
              Beyond topics and groups, klens manages schemas, ACL bindings, SCRAM users, client
              quotas, and broker configs. Each change is its own privilege, so a role can reset
              offsets without the right to delete a topic.
            </FeatureText>
            <WritePrivileges />
          </div>
        </section>

        <section className="flex flex-col gap-10 lg:gap-12">
          <SectionHeader title="A page costs Kafka nothing.">
            <p>
              klens reads each cluster on a fixed schedule into an in-memory projection, and every
              page, API call, and most MCP tools read that copy. The load on Kafka depends on the
              size of the cluster, not on how many people look.
            </p>
            <p>
              Record reads, live tails, broker configs, and changes go to Kafka only when someone
              asks.
            </p>
            <DocsLink href="/docs/concepts/how-klens-reads-kafka">How klens reads Kafka</DocsLink>
          </SectionHeader>

          <div className={card('lg:p-10')}>
            <Projection />
          </div>
        </section>

        <section className="flex flex-col gap-10 lg:gap-12">
          <SectionHeader title="Safe to open up." />

          <div className={split('gap-6 lg:gap-6')}>
            <div className={card('flex flex-col gap-8 lg:p-8')}>
              <FeatureText
                title="Read-only by default."
                href="/docs/operate/allow-changes"
                link="Allow changes on a cluster"
              >
                klens changes nothing on a cluster until you mark it writable. With sign-in and
                roles, only the people who hold the privilege for a change can make it.
              </FeatureText>
              <CodeBlock
                lang="yaml"
                codeblock={{ title: 'config.yaml', className: 'my-0 mt-auto' }}
                code={`clusters:
  production:
    bootstrap_servers: [broker-1:9093]
    # writable: false is the default
  staging:
    bootstrap_servers: [staging-1:9092]
    writable: true`}
              />
            </div>

            <div className={card('flex flex-col gap-8 lg:p-8')}>
              <FeatureText
                title="Sign in with OpenID Connect."
                href="/docs/operate/require-sign-in"
                link="Require sign-in"
              >
                Give each group in your identity provider its own privileges on each cluster. A
                cluster that no role covers stays invisible.
              </FeatureText>
              <div className="mt-auto">
                <AccessMatrix />
              </div>
            </div>
          </div>

          <div className={split('lg:items-center')}>
            <FeatureText
              title="Hide private fields."
              href="/docs/operate/hide-sensitive-fields"
              link="Hide sensitive fields"
            >
              Hash, mask, or drop card numbers, emails, and any other field for every reader, admins
              included. Equal values get equal tokens, so records stay easy to correlate. Search
              sees what the page shows, so a filter can never recover a hidden value.
            </FeatureText>
            <DemoScene scene="mist" position="50% 85%">
              <ObfuscatedRecord />
            </DemoScene>
          </div>
        </section>

        <section className="flex flex-col gap-10 lg:gap-12">
          <SectionHeader title="Ready for agents.">
            <p>
              Turn on MCP. Agents in Claude Code, VS Code, or Cursor then read the same projection
              as the UI, so most calls cost Kafka nothing.
            </p>
            <DocsLink href="/docs/operate/connect-an-agent">Connect an AI agent</DocsLink>
          </SectionHeader>

          <Scenery scene="night" position="70% 30%" className="px-3 py-8 text-sm sm:px-6 lg:py-16">
            <div className="mx-auto flex w-full max-w-[640px] flex-col gap-3 rounded-2xl border bg-fd-card/85 p-3 backdrop-blur-md">
              <AgentChat />
              <CodeBlock
                lang="bash"
                code="claude mcp add --transport http klens http://localhost:8090/mcp"
                codeblock={{ title: 'Terminal', className: 'my-0' }}
              />
            </div>
          </Scenery>

          <div className={split('lg:items-center')}>
            <FeatureText
              title="Under a ceiling you set."
              href="/docs/reference/mcp-tools"
              link="See every MCP tool"
            >
              The <code className="font-mono text-[0.9em]">mcp</code> block names the clusters
              agents reach and the privileges they hold. A tool whose privilege it leaves out never
              reaches the agent, so agents change nothing until it names a write.
            </FeatureText>
            <McpCeiling />
          </div>
        </section>

        <Closing />
      </div>
    </div>
  );
}

function TryIt() {
  return (
    <section className={split('lg:items-center')}>
      <div className="flex flex-col gap-6">
        <h2 className={sectionHeading()}>One binary, one config file.</h2>
        <p className={prose()}>
          klens serves the web UI, a JSON API, and an MCP server from one Rust process. Name a
          broker in <code className="font-mono text-[0.9em]">config.yaml</code>, then start the
          image.
        </p>
        <CodeBlock
          lang="yaml"
          code={configYaml}
          codeblock={{ title: 'config.yaml', className: 'my-0 text-sm' }}
        />
        <p className={prose('lg:text-base')}>
          Then open <code className="font-mono text-[0.9em]">localhost:8080</code>. No broker at
          hand?{' '}
          <Link href="/docs/getting-started" className="font-medium text-brand hover:underline">
            The tutorial
          </Link>{' '}
          runs one next to klens with Docker Compose.
        </p>
      </div>

      <div className="relative isolate overflow-hidden rounded-2xl border p-3 text-sm sm:p-8">
        <div aria-hidden className="pointer-events-none absolute inset-0 -z-1">
          <div className="absolute inset-0 bg-[radial-gradient(70%_90%_at_100%_0%,color-mix(in_oklch,var(--color-brand-glow)_30%,transparent),transparent)]" />
          <Warp className="absolute inset-0 size-full opacity-60 mask-[linear-gradient(to_bottom,white_20%,transparent)]" />
        </div>
        <Terminal
          command={runCommand}
          output={startupLog}
          window={
            <div className="w-56 overflow-hidden rounded-lg border bg-fd-popover shadow-lg">
              <p className="border-b px-4 py-2 text-center text-xs text-fd-muted-foreground">
                localhost:8080
              </p>
              <div className="flex items-center gap-2 px-4 py-3 text-sm">
                <span className="size-2 rounded-full bg-fd-success" />
                <span className="font-mono">local</span>
                <span className="ms-auto text-xs text-fd-muted-foreground">12 topics</span>
              </div>
            </div>
          }
        />
      </div>
    </section>
  );
}

function Closing() {
  return (
    <section
      className={card(
        'relative isolate flex min-h-[600px] flex-col items-center overflow-hidden px-6 pt-16 text-center lg:pt-20',
      )}
    >
      <h2 className="text-4xl font-medium tracking-tight text-balance text-fd-foreground lg:text-6xl">
        Bring Kafka into focus.
      </h2>
      <p className={prose('mt-5 max-w-lg')}>
        klens is open source under the MIT license. Start the image with Docker, or install the Helm
        chart on Kubernetes.
      </p>
      <div className="mt-8 flex flex-wrap justify-center gap-3">
        <Link href="/docs/getting-started" className={button('primary')}>
          Get started
          <ArrowRightIcon className="size-4" />
        </Link>
        <a href={repoUrl} rel="noreferrer noopener" target="_blank" className={button('secondary')}>
          <GitHubIcon className="size-4" />
          GitHub
        </a>
      </div>

      <Grain
        shape="sphere"
        className="absolute inset-x-0 bottom-0 -z-1 h-1/2 opacity-60 mask-[linear-gradient(to_top,white_30%,transparent)]"
      />
      <Lens className="absolute -bottom-[250px] left-1/2 -z-1 size-[440px] -translate-x-1/2" />
    </section>
  );
}

function CodeBlock(props: ServerCodeBlockProps) {
  return <ServerCodeBlock colorReplacements={readableTokens} {...props} />;
}

function SectionHeader({ title, children }: { title: string; children?: ReactNode }) {
  return (
    <div className={split('gap-6 lg:items-start')}>
      <h2 className={sectionHeading()}>{title}</h2>
      {children && <div className={prose('flex flex-col gap-4 lg:pt-2')}>{children}</div>}
    </div>
  );
}

function FeatureText({
  title,
  href,
  link,
  children,
}: {
  title: string;
  href: string;
  link: string;
  children: ReactNode;
}) {
  return (
    <div className="flex flex-col gap-4">
      <h3 className={cardHeading()}>{title}</h3>
      <p className={prose()}>{children}</p>
      <DocsLink href={href} className="mt-2">
        {link}
      </DocsLink>
    </div>
  );
}

function ShowcaseBody({ href, link, children }: { href: string; link: string; children: string }) {
  return (
    <>
      <p className={prose()}>{children}</p>
      <DocsLink href={href}>{link}</DocsLink>
    </>
  );
}

function DocsLink({
  href,
  className,
  children,
}: {
  href: string;
  className?: string;
  children: string;
}) {
  return (
    <Link
      href={href}
      className={cn(
        'inline-flex w-fit items-center gap-1.5 text-base font-medium text-brand hover:underline',
        className,
      )}
    >
      {children}
      <ArrowRightIcon className="size-4" />
    </Link>
  );
}

function DemoScene({
  scene,
  position,
  children,
}: {
  scene: Scene;
  position: string;
  children: ReactNode;
}) {
  return (
    <Scenery
      scene={scene}
      position={position}
      className="flex min-h-[420px] flex-col justify-center p-4 text-sm sm:p-10"
    >
      {children}
    </Scenery>
  );
}
