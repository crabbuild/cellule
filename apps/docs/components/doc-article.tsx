import { source } from "@/lib/source";
import { getMDXComponents } from "@/components/mdx";
import Link from "next/link";
import { notFound } from "next/navigation";

export function DocArticle({ slug }: { slug: string[] }) {
  const page = source.getPage(slug);
  if (!page) notFound();
  const MDX = page.data.body;
  return (
    <article className="page-width doc-article">
      <div className="prose max-w-none">
        <MDX components={getMDXComponents()} />
      </div>
      <Link href={page.url} className="inline-link article-reference">
        Open this guide in the documentation
      </Link>
    </article>
  );
}
