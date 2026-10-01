import type { Metadata } from "next";
import { notFound } from "next/navigation";
import Link from "next/link";
import {
  DocsBody,
  DocsDescription,
  DocsPage,
  DocsTitle,
} from "fumadocs-ui/layouts/docs/page";
import { getMDXComponents } from "@/components/mdx";
import { source } from "@/lib/source";
import { pageMetadata, site } from "@/lib/site";

type Props = { params: Promise<{ slug?: string[] }> };
export function generateStaticParams() {
  return source.generateParams();
}
export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const page = source.getPage((await params).slug);
  return page
    ? pageMetadata(
        page.data.title,
        page.data.description ?? site.description,
        page.url,
      )
    : {};
}
export default async function Page({ params }: Props) {
  const page = source.getPage((await params).slug);
  if (!page) notFound();
  const MDX = page.data.body;
  const sourcePath =
    page.data.sourcePath ?? `apps/web/content/authored/${page.path}`;
  return (
    <DocsPage toc={page.data.toc} full={page.data.full}>
      <DocsTitle>{page.data.title}</DocsTitle>
      {!page.data.sourcePath && (
        <DocsDescription>{page.data.description}</DocsDescription>
      )}
      <div className="doc-actions">
        <a href={`${site.github}/blob/main/${sourcePath}`}>View source</a>
        <Link
          href={`/markdown/${page.slugs.length ? page.slugs.join("/") : "index"}`}
        >
          Read Markdown
        </Link>
      </div>
      <DocsBody>
        <MDX components={getMDXComponents()} />
      </DocsBody>
    </DocsPage>
  );
}
