import type { MetadataRoute } from 'next';
import { source } from '@/lib/source';
import { siteUrl } from '@/lib/shared';

export const revalidate = false;

export default function sitemap(): MetadataRoute.Sitemap {
  const docs = source.getPages().map((page) => ({ url: `${siteUrl}${page.url}` }));

  return [{ url: siteUrl }, ...docs];
}
