import type { ReactNode } from 'react';
import type { Metadata } from 'next';
import Link from 'next/link';
import { ArrowRightIcon } from 'lucide-react';
import { ServerCodeBlock } from 'fumadocs-ui/components/codeblock.rsc';
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
import { button, card, cardHeading, sectionHeading, split } from '@/components/landing/variants';

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

      <div className="mx-auto mt-16 flex w-full max-w-[1400px] flex-col gap-24 px-6 md:px-12 lg:mt-24 lg:gap-32">
        <TryIt />

        <section className="flex flex-col gap-16">
          <Showcase
            heading={<h2 className={sectionHeading()}>Built for reading Kafka.</h2>}
            tabs={[
              {
                value: 'search',
                label: 'Find a record',
                content: (
                  <Feature
                    title="Find any record."
                    href="/docs/use/find-records"
                    link="Find records in a topic"
                    scene="dunes"
                    position="20% 30%"
                    demo={<RecordSearch />}
                  >
                    Search a topic by text, partition, and time. klens runs the search on the
                    server, decodes Avro, JSON Schema, and Protobuf values, and exports the matches
                    as NDJSON.
                  </Feature>
                ),
              },
              {
                value: 'live',
                label: 'Follow it live',
                content: (
                  <Feature
                    title="Follow a topic live."
                    href="/docs/use/follow-a-topic-live"
                    link="Follow a topic live"
                    scene="sea"
                    position="65% 40%"
                    demo={<LiveTail />}
                  >
                    Switch to Live and records appear as soon as Kafka stores them. On a busy topic
                    the tail samples the newest records, so the page stays responsive.
                  </Feature>
                ),
              },
              {
                value: 'lag',
                label: 'Watch lag',
                content: (
                  <Feature
                    title="Watch consumer lag."
                    href="/docs/use/reset-consumer-offsets"
                    link="Reset a group's offsets"
                    scene="ridges"
                    position="30% 35%"
                    demo={<ConsumerLag />}
                  >
                    See each group with its members and its lag on every partition. Reset its
                    offsets after a preview of the lag the change leaves, or delete the offsets of a
                    topic it no longer reads.
                  </Feature>
                ),
              },
            ]}
          />

          <div className={split('text-sm lg:items-center')}>
            <div>
              <h3 className={cardHeading()}>Manage more than topics.</h3>
              <p className="mb-4 text-fd-muted-foreground">
                Beyond topics and groups, klens manages schemas, ACL bindings, SCRAM users, client
                quotas, and broker configs. Each change is its own privilege, so a role can reset
                offsets without the right to delete a topic.
              </p>
              <DocsLink href="/docs/reference/privileges">See every privilege</DocsLink>
            </div>
            <WritePrivileges />
          </div>
        </section>

        <section className="flex flex-col gap-10">
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

        <section className="flex flex-col gap-10">
          <SectionHeader title="Safe to open up." />

          <div className={split('gap-6 lg:gap-6')}>
            <div className={card('flex flex-col')}>
              <h3 className={cardHeading()}>Read-only by default.</h3>
              <p className="mb-4 text-fd-muted-foreground">
                klens changes nothing on a cluster until you mark it writable. With sign-in and
                roles, only the people who hold the privilege for a change can make it.
              </p>
              <DocsLink href="/docs/operate/allow-changes" className="mb-6">
                Allow changes on a cluster
              </DocsLink>
              <ServerCodeBlock
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

            <div className={card('flex flex-col')}>
              <h3 className={cardHeading()}>Sign in with OpenID Connect.</h3>
              <p className="mb-4 text-fd-muted-foreground">
                Give each group in your identity provider its own privileges on each cluster. A
                cluster that no role covers stays invisible.
              </p>
              <DocsLink href="/docs/operate/require-sign-in" className="mb-6">
                Require sign-in
              </DocsLink>
              <div className="mt-auto">
                <AccessMatrix />
              </div>
            </div>
          </div>

          <Feature
            title="Hide private fields."
            href="/docs/operate/hide-sensitive-fields"
            link="Hide sensitive fields"
            scene="mist"
            position="50% 85%"
            demo={<ObfuscatedRecord />}
          >
            Hash, mask, or drop card numbers, emails, and any other field for every reader, admins
            included. Equal values get equal tokens, so records stay easy to correlate. Search sees
            what the page shows, so a filter can never recover a hidden value.
          </Feature>
        </section>

        <section className="flex flex-col gap-10">
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
              <ServerCodeBlock
                lang="bash"
                code="claude mcp add --transport http klens http://localhost:8090/mcp"
                codeblock={{ title: 'Terminal', className: 'my-0' }}
              />
            </div>
          </Scenery>

          <div className={split('text-sm lg:items-center')}>
            <div>
              <h3 className={cardHeading()}>Under a ceiling you set.</h3>
              <p className="mb-4 text-fd-muted-foreground">
                The <code className="font-mono text-xs">mcp</code> block names the clusters agents
                reach and the privileges they hold. A tool whose privilege it leaves out never
                reaches the agent, so agents change nothing until it names a write.
              </p>
              <DocsLink href="/docs/reference/mcp-tools">See every MCP tool</DocsLink>
            </div>
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
    <section className={split('text-sm lg:items-center')}>
      <div className="flex flex-col gap-6">
        <div className="flex flex-col gap-4">
          <h2 className={sectionHeading()}>One binary, one config file.</h2>
          <p className="text-base text-fd-muted-foreground">
            klens serves the web UI, a JSON API, and an MCP server from one Rust process. Name a
            broker in <code className="font-mono text-sm">config.yaml</code>, then start the image.
          </p>
        </div>
        <ServerCodeBlock
          lang="yaml"
          code={configYaml}
          codeblock={{ title: 'config.yaml', className: 'my-0' }}
        />
        <p className="text-fd-muted-foreground">
          Then open <code className="font-mono text-xs">localhost:8080</code>. No broker at hand?{' '}
          <Link href="/docs/getting-started" className="font-medium text-brand hover:underline">
            The tutorial
          </Link>{' '}
          runs one next to klens with Docker Compose.
        </p>
      </div>

      <div className="relative isolate overflow-hidden rounded-2xl border p-3 sm:p-8">
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
      <h2 className="text-4xl font-medium tracking-tight text-balance text-fd-foreground lg:text-5xl">
        Bring Kafka into focus.
      </h2>
      <p className="mt-4 max-w-md text-base text-fd-muted-foreground">
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

function SectionHeader({ title, children }: { title: string; children?: ReactNode }) {
  return (
    <div className={split('lg:items-start')}>
      <h2 className={sectionHeading()}>{title}</h2>
      {children && (
        <div className="flex flex-col gap-4 text-base text-fd-muted-foreground lg:pt-1.5">
          {children}
        </div>
      )}
    </div>
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
        'inline-flex w-fit items-center gap-1.5 text-sm font-medium text-brand hover:underline',
        className,
      )}
    >
      {children}
      <ArrowRightIcon className="size-4" />
    </Link>
  );
}

function Feature({
  title,
  href,
  link,
  scene,
  position,
  demo,
  children,
}: {
  title: string;
  href: string;
  link: string;
  scene: Scene;
  position: string;
  demo: ReactNode;
  children: string;
}) {
  return (
    <div className={split('text-sm')}>
      <div className="lg:pt-10">
        <h3 className={cardHeading()}>{title}</h3>
        <p className="mb-4 text-fd-muted-foreground">{children}</p>
        <DocsLink href={href}>{link}</DocsLink>
      </div>
      <Scenery
        scene={scene}
        position={position}
        className="flex min-h-[460px] flex-col justify-center p-4 sm:p-10"
      >
        {demo}
      </Scenery>
    </div>
  );
}
