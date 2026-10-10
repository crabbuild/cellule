import { source } from "@/lib/source";
import { site } from "@/lib/site";

export const dynamic = "force-static";
export function GET() {
  const content = `# Cellule\n\n${site.description}\n\n## Documentation\n\n${source
    .getPages()
    .map(
      (page) =>
        `- [${page.data.title}](${site.url}/markdown/${page.slugs.length ? page.slugs.join("/") : "index"}): ${page.data.description ?? ""}`,
    )
    .join("\n")}\n\nFull text: ${site.url}/llms-full.txt\n`;
  return new Response(content, {
    headers: { "Content-Type": "text/plain; charset=utf-8" },
  });
}
