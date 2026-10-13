import type { ReactNode } from 'react';
import type { Metadata } from 'next';
import Link from 'next/link';
import {
  ArrowRightIcon,
  BoxIcon,
  LockIcon,
  type LucideIcon,
  ScaleIcon,
  ShipIcon,
} from 'lucide-react';
import { ServerCodeBlock } from 'fumadocs-ui/components/codeblock.rsc';
import { cn } from '@/lib/cn';
import { source } from '@/lib/source';
import { getPageImageUrl, repoUrl } from '@/lib/shared';
import { AccessMatrix } from '@/components/landing/access';
import { AgentChat, ToolList } from '@/components/landing/agents';
import { GitHubIcon } from '@/components/landing/github-icon';
import { Hero } from '@/components/landing/hero';
import { Lens } from '@/components/landing/lens';
import { LiveTail } from '@/components/landing/live-tail';
import { ObfuscatedRecord } from '@/components/landing/obfuscation';
import { Projection } from '@/components/landing/projection';
import { ClusterResources, ConsumerLag, RecordSearch } from '@/components/landing/reading';
import { type Scene, Scenery } from '@/components/landing/scenery';
import { Grain, Warp } from '@/components/landing/shaders';
import { Showcase } from '@/components/landing/showcase';
import { type LogLine, Terminal } from '@/components/landing/terminal';
import { button, card, cardHeading, sectionHeading } from '@/components/landing/variants';

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

      <div className="mx-auto mt-12 grid w-full max-w-[1400px] grid-cols-1 gap-10 px-6 md:px-12 lg:mt-20 lg:grid-cols-2">
        <p className="col-span-full text-2xl leading-snug font-light tracking-tight text-fd-foreground md:text-3xl xl:text-4xl">
          klens is a <Accent>web UI</Accent> and an <Accent>MCP server</Accent> for{' '}
          <Accent>Kafka</Accent>, served by <Accent>one Rust binary</Accent>. It keeps a copy of
          each cluster in memory, so a page costs Kafka nothing, however many people have it open.
        </p>

        <TryIt />

        <h2 className={sectionHeading()}>Built for reading Kafka.</h2>

        <Showcase
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
                  Search a topic by text, partition, and time. klens runs the search on the server,
                  decodes Avro, JSON Schema, and Protobuf values, and exports the matches as NDJSON.
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
                  See each group with its members and its lag on every partition. Reset its offsets
                  after a preview of the lag the change leaves, or delete the offsets of a topic it
                  no longer reads.
                </Feature>
              ),
            },
          ]}
        />

        <div className="col-span-full grid grid-cols-1 gap-8 rounded-2xl bg-brand p-6 text-sm text-brand-foreground shadow-lg shadow-black/5 lg:grid-cols-[minmax(0,2fr)_minmax(0,3fr)] lg:items-center lg:p-10">
          <div>
            <h3 className={cardHeading('text-brand-foreground')}>Manage more than topics.</h3>
            <p className="mb-6 opacity-85">
              Beyond topics and groups, klens manages schemas, ACL bindings, SCRAM users, client
              quotas, and broker configs.
            </p>
            <DocsLink href="/docs/reference/privileges" className="mb-0 text-brand-foreground">
              See every privilege
            </DocsLink>
          </div>
          <ClusterResources />
        </div>

        <h2 className={sectionHeading()}>Light on your brokers.</h2>

        <div className={card('col-span-full flex flex-col gap-10 lg:p-10')}>
          <div className="grid gap-6 lg:grid-cols-2">
            <h3 className={cardHeading('mb-0')}>A page costs Kafka nothing.</h3>
            <div className="flex flex-col gap-4 text-fd-muted-foreground">
              <p>
                klens reads each cluster on a fixed schedule into an in-memory projection, and every
                page, API call, and most MCP tools read that copy. The load on Kafka depends on the
                size of the cluster, not on how many people look.
              </p>
              <p>
                Record reads, live tails, broker configs, and changes go to Kafka only when someone
                asks.
              </p>
              <DocsLink href="/docs/concepts/how-klens-reads-kafka" className="mb-0">
                How klens reads Kafka
              </DocsLink>
            </div>
          </div>
          <Projection />
        </div>

        <h2 className={sectionHeading()}>Safe to open up.</h2>

        <div className={card('flex flex-col')}>
          <LockIcon className="mb-4 size-6 text-brand" />
          <h3 className={cardHeading()}>Read-only by default.</h3>
          <p className="mb-4 text-fd-muted-foreground">
            klens changes nothing on a cluster until you mark it writable. With sign-in and roles,
            only the people who hold the privilege for a change can make it.
          </p>
          <DocsLink href="/docs/operate/allow-changes" className="mt-auto mb-0">
            Allow changes on a cluster
          </DocsLink>
        </div>

        <ServerCodeBlock
          lang="yaml"
          codeblock={{ title: 'config.yaml', className: 'my-0 shadow-lg shadow-black/5' }}
          code={`clusters:
  production:
    bootstrap_servers: [broker-1:9093, broker-2:9093]
    # writable: false is the default
  staging:
    bootstrap_servers: [staging-1:9092]
    writable: true`}
        />

        <div className={card('flex flex-col')}>
          <h3 className={cardHeading()}>Sign in with OpenID Connect.</h3>
          <p className="mb-4 text-fd-muted-foreground">
            Give each group in your identity provider its own privileges on each cluster. A cluster
            that no role covers stays invisible.
          </p>
          <DocsLink href="/docs/operate/require-sign-in" className="mb-6">
            Require sign-in
          </DocsLink>
          <AccessMatrix />
        </div>

        <Scenery scene="mist" position="50% 85%" className="flex flex-col gap-4 p-4 text-sm sm:p-6">
          <div className="rounded-xl border bg-fd-card/85 p-5 backdrop-blur-md">
            <h3 className={cardHeading()}>Hide private fields.</h3>
            <p className="mb-4 text-fd-muted-foreground">
              Hash, mask, or drop card numbers, emails, and any other field for every reader, admins
              included. Equal values get equal tokens, so records stay easy to correlate. Search
              sees what the page shows, so a filter can never recover a hidden value.
            </p>
            <DocsLink href="/docs/operate/hide-sensitive-fields" className="mb-0">
              Hide sensitive fields
            </DocsLink>
          </div>
          <ObfuscatedRecord />
        </Scenery>

        <h2 className={sectionHeading()}>Ready for agents.</h2>

        <Scenery
          scene="night"
          position="70% 30%"
          className="col-span-full px-3 py-8 text-sm sm:px-6 lg:py-16"
        >
          <div className="mx-auto flex w-full max-w-[640px] flex-col gap-4 rounded-2xl border bg-fd-card/85 p-3 backdrop-blur-md">
            <div className="px-3 pt-3">
              <h3 className={cardHeading()}>Ask your agent about Kafka.</h3>
              <p className="mb-4 text-fd-muted-foreground">
                Turn on MCP. Agents in Claude Code, VS Code, or Cursor then read the same projection
                as the UI, so most calls cost Kafka nothing.
              </p>
              <DocsLink href="/docs/operate/connect-an-agent" className="mb-0">
                Connect an AI agent
              </DocsLink>
            </div>
            <AgentChat />
            <ServerCodeBlock
              lang="bash"
              code="claude mcp add --transport http klens http://localhost:8090/mcp"
              codeblock={{ title: 'Terminal', className: 'my-0' }}
            />
          </div>
        </Scenery>

        <div
          className={card(
            'col-span-full grid grid-cols-1 gap-8 lg:grid-cols-2 lg:items-center lg:p-10',
          )}
        >
          <div className="flex flex-col">
            <h3 className={cardHeading()}>Under a ceiling you set.</h3>
            <p className="mb-4 text-fd-muted-foreground">
              The <code className="font-mono text-xs">mcp</code> block names the clusters agents
              reach and the privileges they hold. Agents change nothing until it names a write.
            </p>
            <DocsLink href="/docs/reference/mcp-tools" className="mb-6">
              See every MCP tool
            </DocsLink>
            <ServerCodeBlock
              lang="yaml"
              codeblock={{ title: 'config.yaml', className: 'my-0' }}
              code={`mcp:
  bind: 127.0.0.1:8090
  privileges: [topic_configs, schema_text] # no record payloads or ACLs
  clusters: [production, staging]`}
            />
          </div>
          <ToolList />
        </div>

        <h2 className={sectionHeading()}>Open source, one binary.</h2>

        <ul className={card('flex flex-col gap-6')}>
          <Point icon={BoxIcon} title="One Rust binary.">
            The UI, the JSON API, and MCP come from one process with one YAML file.
          </Point>
          <Point icon={ShipIcon} title="Runs where Kafka runs.">
            Start the image with Docker, or install the Helm chart on Kubernetes.
          </Point>
          <Point icon={ScaleIcon} title="MIT licensed.">
            The code, the chart, and these docs are on GitHub.
          </Point>
          <li className="mt-auto flex flex-wrap gap-3">
            <Link href="/docs" className={button('primary')}>
              Read the docs
            </Link>
            <a
              href={repoUrl}
              rel="noreferrer noopener"
              target="_blank"
              className={button('secondary')}
            >
              <GitHubIcon className="size-4" />
              Open GitHub
            </a>
          </li>
        </ul>

        <div className={card('relative flex min-h-[420px] flex-col overflow-hidden p-0 pt-14')}>
          <p className="text-center font-mono text-2xl font-bold tracking-tight uppercase lg:text-3xl">
            Bring Kafka into focus
          </p>
          <p className="mt-3 text-center font-mono text-xs text-fd-muted-foreground">
            one lens for every cluster.
          </p>
          <Grain
            shape="sphere"
            className="absolute inset-x-0 bottom-0 h-2/3 opacity-60 mask-[linear-gradient(to_top,white_30%,transparent)]"
          />
          <Lens className="absolute -bottom-[250px] left-1/2 size-[440px] -translate-x-1/2" />
        </div>
      </div>
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
        'mb-8 inline-flex w-fit items-center gap-1.5 font-medium text-brand hover:underline',
        className,
      )}
    >
      {children}
      <ArrowRightIcon className="size-4" />
    </Link>
  );
}

/** One showcase tab, with the copy beside a demo set on a dithered landscape. */
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
    <div className="grid grid-cols-1 gap-8 text-sm lg:grid-cols-[minmax(0,2fr)_minmax(0,3fr)] lg:items-center">
      <div className="lg:px-4">
        <h3 className={cardHeading()}>{title}</h3>
        <p className="mb-4 text-fd-muted-foreground">{children}</p>
        <DocsLink href={href} className="mb-0">
          {link}
        </DocsLink>
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

function Accent({ children }: { children: string }) {
  return <span className="font-medium text-brand">{children}</span>;
}

function Point({
  icon: Icon,
  title,
  children,
}: {
  icon: LucideIcon;
  title: string;
  children: string;
}) {
  return (
    <li className="flex flex-col gap-2">
      <span className="flex items-center gap-2 font-medium text-fd-foreground">
        <Icon className="size-5 text-brand" />
        {title}
      </span>
      <span className="text-fd-muted-foreground">{children}</span>
    </li>
  );
}

function TryIt() {
  return (
    <div className="relative isolate col-span-full overflow-hidden rounded-2xl border p-2 sm:p-4 md:p-8">
      <div aria-hidden className="pointer-events-none absolute inset-0 -z-1">
        <div className="absolute inset-0 bg-[radial-gradient(60%_80%_at_50%_0%,color-mix(in_oklch,var(--color-brand-glow)_30%,transparent),transparent)]" />
        <Warp className="absolute inset-0 size-full opacity-60 mask-[linear-gradient(to_bottom,white_20%,transparent)]" />
      </div>
      <div className="mx-auto flex w-full max-w-[1000px] flex-col gap-3 rounded-2xl border bg-fd-card p-3 shadow-lg">
        <div className="flex flex-wrap items-center gap-3">
          <h2 className="rounded-xl border-2 border-brand/50 px-3 py-1.5 font-mono text-sm font-bold text-brand uppercase">
            Try it out
          </h2>
          <p className="text-fd-muted-foreground">Name a broker, then start the image.</p>
        </div>
        <div className="grid grid-cols-1 gap-3 lg:grid-cols-[minmax(0,2fr)_minmax(0,3fr)]">
          <div className="flex flex-col gap-4">
            <ServerCodeBlock
              lang="yaml"
              code={configYaml}
              codeblock={{ title: 'config.yaml', className: 'my-0' }}
            />
            <p className="px-1 text-fd-muted-foreground">
              Then open <code className="font-mono text-xs">localhost:8080</code>. No broker at
              hand?{' '}
              <Link href="/docs/getting-started" className="font-medium text-brand hover:underline">
                The tutorial
              </Link>{' '}
              runs one next to klens with Docker Compose.
            </p>
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
      </div>
    </div>
  );
}
