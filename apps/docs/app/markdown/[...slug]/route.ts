import { source } from "@/lib/source";

export function generateStaticParams() {
  return source
    .getPages()
    .map((page) => ({ slug: page.slugs.length ? page.slugs : ["index"] }));
}
export async function GET(
  _request: Request,
  { params }: { params: Promise<{ slug: string[] }> },
) {
  const { slug } = await params;
  const page = source.getPage(
    slug.length === 1 && slug[0] === "index" ? [] : slug,
  );
  if (!page) return new Response("Document not found", { status: 404 });
  return new Response(
    `# ${page.data.title}\n\n${await page.data.getText("processed")}`,
    { headers: { "Content-Type": "text/markdown; charset=utf-8" } },
  );
}
