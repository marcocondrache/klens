import Link from 'next/link';
import { HomeLayout } from 'fumadocs-ui/layouts/home';
import {
  NavbarMenu,
  NavbarMenuContent,
  NavbarMenuLink,
  NavbarMenuTrigger,
} from 'fumadocs-ui/layouts/home/navbar';
import {
  BookOpenIcon,
  CompassIcon,
  HouseIcon,
  LibraryIcon,
  type LucideIcon,
  RocketIcon,
  WrenchIcon,
} from 'lucide-react';
import { baseOptions } from '@/lib/layout.shared';
import { appName, repoUrl } from '@/lib/shared';
import { LogoMark } from '@/components/logo';

const sections: { icon: LucideIcon; title: string; description: string; url: string }[] = [
  {
    icon: HouseIcon,
    title: 'Overview',
    description: 'What klens is and where to start.',
    url: '/docs',
  },
  {
    icon: CompassIcon,
    title: 'Use klens',
    description: 'Find records, follow topics, and change a cluster.',
    url: '/docs/use/find-records',
  },
  {
    icon: WrenchIcon,
    title: 'Operate klens',
    description: 'Deploy it, connect clusters, and require sign-in.',
    url: '/docs/operate/run-with-docker',
  },
  {
    icon: BookOpenIcon,
    title: 'Concepts',
    description: 'Why klens works the way it does.',
    url: '/docs/concepts/how-klens-reads-kafka',
  },
  {
    icon: LibraryIcon,
    title: 'Reference',
    description: 'Configuration, privileges, API, and MCP tools.',
    url: '/docs/reference/configuration',
  },
];

export default function Layout({ children }: LayoutProps<'/'>) {
  return (
    <>
      <a
        href="#content"
        className="sr-only rounded-md bg-fd-background px-4 py-2 text-sm font-medium shadow-lg focus:not-sr-only focus:fixed focus:start-4 focus:top-4 focus:z-50 focus-visible:outline-2 focus-visible:outline-brand"
      >
        Skip to main content
      </a>
      <HomeLayout
        {...baseOptions()}
        links={[
          {
            type: 'menu',
            on: 'menu',
            text: 'Documentation',
            items: [
              { text: 'Get started', url: '/docs/getting-started', icon: <RocketIcon /> },
              ...sections.map((section) => ({
                text: section.title,
                url: section.url,
                icon: <section.icon />,
              })),
            ],
          },
          {
            type: 'custom',
            on: 'nav',
            children: (
              <NavbarMenu>
                <NavbarMenuTrigger>Documentation</NavbarMenuTrigger>
                <NavbarMenuContent>
                  <NavbarMenuLink href="/docs/getting-started">
                    <RocketIcon className="mb-2 rounded-md bg-brand p-1 text-brand-foreground" />
                    <p className="font-medium">Get started</p>
                    <p className="text-sm text-fd-muted-foreground">
                      Run klens next to a local Kafka broker and try it out.
                    </p>
                  </NavbarMenuLink>
                  {sections.map(({ icon: Icon, title, description, url }) => (
                    <NavbarMenuLink key={url} href={url}>
                      <Icon className="mb-2 rounded-md bg-fd-secondary p-1 text-fd-secondary-foreground" />
                      <p className="font-medium">{title}</p>
                      <p className="text-sm text-fd-muted-foreground">{description}</p>
                    </NavbarMenuLink>
                  ))}
                </NavbarMenuContent>
              </NavbarMenu>
            ),
          },
        ]}
        className="dark:bg-neutral-950 dark:[--color-fd-background:var(--color-neutral-950)]"
      >
        <div id="content" tabIndex={-1} className="outline-none">
          {children}
        </div>
      </HomeLayout>
      <Footer />
    </>
  );
}

function Footer() {
  return (
    <footer className="w-full dark:bg-neutral-950">
      <div className="mx-auto flex w-full max-w-[1400px] flex-wrap items-center gap-x-6 gap-y-3 border-t px-6 py-8 text-sm text-fd-muted-foreground md:px-12">
        <span className="flex items-center gap-2 font-medium text-fd-foreground">
          <LogoMark className="size-4" />
          {appName}
        </span>
        <span>MIT License</span>
        <nav aria-label="Footer" className="ms-auto flex gap-6">
          <Link href="/docs" className="hover:text-fd-foreground">
            Docs
          </Link>
          <a
            href={repoUrl}
            rel="noreferrer noopener"
            target="_blank"
            className="hover:text-fd-foreground"
          >
            GitHub
          </a>
        </nav>
      </div>
    </footer>
  );
}
