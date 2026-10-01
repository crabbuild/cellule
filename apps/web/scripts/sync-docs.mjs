import { execFileSync } from "node:child_process";
import { mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { unified } from "unified";
import remarkParse from "remark-parse";
import remarkGfm from "remark-gfm";
import remarkStringify from "remark-stringify";
import { visit } from "unist-util-visit";

export const webRoot = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
);
export const repoRoot = path.resolve(webRoot, "../..");
const tracked = execFileSync("git", ["ls-files", "-z"], {
  cwd: repoRoot,
  encoding: "utf8",
})
  .split("\0")
  .filter(Boolean);
const repositoryOnlyDocuments = new Set([
  "docs/reference.md",
  "docs/roadmap.md",
  "docs/verification.md",
]);
export const documentationFiles = tracked.filter(
  (file) =>
    file.endsWith(".md") &&
    !file.startsWith("apps/") &&
    !repositoryOnlyDocuments.has(file) &&
    !["AGENTS.md", "CLAUDE.md"].includes(path.basename(file)),
);
export function docSlug(file) {
  if (file === "README.md") return "guides/introduction";
  if (file === "CONTRIBUTING.md") return "contributing";
  if (file === "CHANGELOG.md") return "changelog";
  return file
    .replace(/^docs\//, "guides/")
    .replace(/\/README\.md$/, "/index")
    .replace(/\.md$/, "")
    .replace(/\/index$/, "")
    .replace(/cellule-/g, "")
    .toLowerCase();
}
const urls = new Map(
  documentationFiles.map((file) => [
    file,
    `/docs${docSlug(file) ? `/${docSlug(file)}` : ""}`,
  ]),
);
const parser = unified().use(remarkParse).use(remarkGfm).use(remarkStringify, {
  fences: true,
  bullet: "-",
  emphasis: "_",
  rule: "-",
});
const assetFiles = tracked.filter(
  (file) => !file.startsWith("apps/") && /\.(svg|png)$/.test(file),
);
const assetSet = new Set(assetFiles);
const manifest = [];
const authoredPages = [];
const diagrams = [];
await mkdir(path.join(webRoot, "content/docs"), { recursive: true });
await mkdir(path.join(webRoot, "lib"), { recursive: true });
const outputs = new Set();
async function writeGenerated(destination, content) {
  outputs.add(destination);
  const bytes = Buffer.from(content);
  try {
    if ((await readFile(destination)).equals(bytes)) return;
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  await mkdir(path.dirname(destination), { recursive: true });
  await writeFile(destination, bytes);
}
// Keep unchanged files intact: unlinking an entire collection overwhelms MDX
// and Next's watchers while a preview is serving it.
async function removeStale(directory) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const file = path.join(directory, entry.name);
    if (entry.isDirectory()) await removeStale(file);
    else if (!outputs.has(file)) await rm(file);
  }
}

function resolveLink(raw, source) {
  if (!raw || /^(?:[a-z][a-z\d+.-]*:|\/|#)/i.test(raw)) return raw;
  const [target, anchor] = raw.split("#");
  let file = path.posix.normalize(
    path.posix.join(path.posix.dirname(source), decodeURI(target)),
  );
  // Prefer the source SVG even when the repository offers a raster preview.
  const svg = file.replace(/(?:@2x)?\.png$/, ".svg");
  if (assetSet.has(svg)) file = svg;
  const suffix = anchor ? `#${anchor}` : "";
  if (urls.has(file)) return urls.get(file) + suffix;
  if (assetSet.has(file)) return `/repository/${file}${suffix}`;
  return `https://github.com/crabbuild/cellule/${tracked.some((item) => item.startsWith(file + "/")) ? "tree" : "blob"}/main/${file}${suffix}`;
}

for (const file of documentationFiles) {
  const original = await readFile(path.join(repoRoot, file), "utf8");
  const tree = parser.parse(original);
  const heading = tree.children.find(
    (node) => node.type === "heading" && node.depth === 1,
  );
  function plain(node) {
    return "value" in node
      ? node.value
      : (node.children ?? []).map(plain).join("");
  }
  const title = heading ? plain(heading) : path.basename(file, ".md");
  const paragraph = tree.children.find(
    (node) =>
      node.type === "paragraph" &&
      !node.children.some((child) => child.type === "image"),
  );
  const fullDescription = (
    paragraph ? plain(paragraph) : `Reference documentation for ${title}.`
  ).replace(/\s+/g, " ");
  const description =
    fullDescription.length > 200
      ? `${fullDescription.slice(0, 197).replace(/\s+\S*$/, "")}…`
      : fullDescription;
  if (heading) tree.children.splice(tree.children.indexOf(heading), 1);
  visit(tree, (node) => {
    if (
      node.type === "link" ||
      node.type === "image" ||
      node.type === "definition"
    )
      node.url = resolveLink(node.url, file);
    if (node.type === "code") {
      if (node.lang?.startsWith("rust")) node.lang = "rust";
      if (node.lang === "mermaid")
        diagrams.push({ source: file, code: node.value });
    }
  });
  const slug = docSlug(file);
  const destination = path.join(
    webRoot,
    "content/docs",
    (slug || "index") + ".md",
  );
  await mkdir(path.dirname(destination), { recursive: true });
  const metadata = { title, description, sourcePath: file };
  await writeGenerated(
    destination,
    `---\n${Object.entries(metadata)
      .map(([key, value]) => `${key}: ${JSON.stringify(value)}`)
      .join("\n")}\n---\n\n${parser.stringify(tree)}`,
  );
  manifest.push({
    ...metadata,
    url: urls.get(file),
    slug,
    bytes: Buffer.byteLength(original),
  });
}
for (const file of assetFiles) {
  const destination = path.join(webRoot, "public/repository", file);
  await mkdir(path.dirname(destination), { recursive: true });
  await writeGenerated(destination, await readFile(path.join(repoRoot, file)));
}

const sections = {
  "": {
    title: "Documentation",
    pages: [
      "index",
      "guides",
      "concepts",
      "primitives",
      "crates",
      "contributing",
      "changelog",
    ],
  },
  guides: {
    title: "Start here",
    pages: [
      "introduction",
      "quickstart",
      "at-a-glance",
      "api",
      "architecture",
      "framework",
      "releasing",
    ],
  },
  crates: {
    title: "Crate guides",
    pages: ["app", "host", "runtime", "ltx", "store", "types", "peer-http"],
  },
};
for (const crate of [
  "app",
  "host",
  "runtime",
  "ltx",
  "store",
  "types",
  "peer-http",
]) {
  sections[`crates/${crate}`] = {
    title: `cellule-${crate}`,
    pages: ["index", "docs", "..."],
  };
}
sections["crates/runtime/docs"] = {
  title: "Runtime contracts",
  pages: [
    "index",
    "overview",
    "runtime",
    "storage",
    "primitives",
    "failover-and-followers",
    "rust-api",
    "deployment",
    "delivery",
    "technical-reference",
    "...",
  ],
};
sections["crates/app/docs"] = {
  title: "Application authoring",
  pages: ["index", "topology", "invocation", "examples"],
};
sections["crates/ltx/docs"] = {
  title: "LTX and recovery",
  pages: ["index", "capture", "publication", "recovery", "safety"],
};
for (const [folder, meta] of Object.entries(sections)) {
  const directory = path.join(webRoot, "content/docs", folder);
  await mkdir(directory, { recursive: true });
  await writeGenerated(
    path.join(directory, "meta.json"),
    JSON.stringify(meta, null, 2),
  );
}
// Authored entry pages complement the canonical repository guides.
async function copyAuthored(directory, relative = "") {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const next = path.join(relative, entry.name);
    if (entry.isDirectory())
      await copyAuthored(path.join(directory, entry.name), next);
    else {
      const source = path.join(directory, entry.name);
      const content = await readFile(source);
      if (/\.mdx?$/.test(entry.name)) {
        const tree = parser.parse(
          content.toString().replace(/^---[\s\S]*?---\n/, ""),
        );
        const sourcePath = path
          .relative(repoRoot, source)
          .replaceAll(path.sep, "/");
        const slug = next
          .replaceAll(path.sep, "/")
          .replace(/\.mdx?$/, "")
          .replace(/(?:^|\/)index$/, "");
        authoredPages.push({ sourcePath, url: `/docs${slug ? `/${slug}` : ""}` });
        visit(tree, (node) => {
          if (node.type === "code" && node.lang === "mermaid")
            diagrams.push({ source: sourcePath, code: node.value });
        });
      }
      const destination = path.join(webRoot, "content/docs", next);
      await mkdir(path.dirname(destination), { recursive: true });
      await writeGenerated(destination, content);
    }
  }
}
await copyAuthored(path.join(webRoot, "content/authored"));
await writeGenerated(
  path.join(webRoot, "lib/docs-manifest.json"),
  JSON.stringify({ pages: manifest, authoredPages, diagrams }, null, 2),
);
await removeStale(path.join(webRoot, "content/docs"));
await removeStale(path.join(webRoot, "public/repository"));
console.log(
  `Synced ${manifest.length} canonical and ${authoredPages.length} authored documents, ${diagrams.length} diagrams, and ${assetFiles.length} assets.`,
);
