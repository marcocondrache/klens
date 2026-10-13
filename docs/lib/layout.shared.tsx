import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';
import { LogoMark } from '@/components/logo';
import { appName, gitConfig } from './shared';

export function baseOptions(): BaseLayoutProps {
  return {
    nav: {
      title: (
        <>
          <LogoMark />
          <span className="font-semibold tracking-tight">{appName}</span>
        </>
      ),
    },
    githubUrl: `https://github.com/${gitConfig.user}/${gitConfig.repo}`,
  };
}
