import Link from 'next/link';

export default function HomePage() {
  return (
    <div className="flex flex-col justify-center items-center text-center flex-1 gap-6 px-4">
      <h1 className="text-4xl font-bold">klens</h1>
      <p className="max-w-xl text-fd-muted-foreground">
        A web UI and MCP server for Kafka. Browse topics, records, consumer groups, brokers, and
        schemas across your clusters from one Rust binary.
      </p>
      <div className="flex gap-3">
        <Link
          href="/docs/getting-started"
          className="rounded-md bg-fd-primary px-4 py-2 text-sm font-medium text-fd-primary-foreground"
        >
          Get started
        </Link>
        <Link href="/docs" className="rounded-md border px-4 py-2 text-sm font-medium">
          Read the docs
        </Link>
      </div>
    </div>
  );
}
