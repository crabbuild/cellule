import assert from "node:assert/strict";
import { readFile, readdir, access } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { unified } from "unified";
import remarkParse from "remark-parse";
import remarkGfm from "remark-gfm";
import { visit } from "unist-util-visit";
import Slugger from "github-slugger";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const parser = unified().use(remarkParse).use(remarkGfm);
const documents = new Map();
async function walk(directory) {
  for (const entry of await readdir(directory, { withFileTypes: true })) {
    const file = path.join(directory, entry.name);
    if (entry.isDirectory()) await walk(file);
    else if (/\.mdx?$/.test(file)) {
      const relative = path
        .relative(path.join(root, "content/docs"), file)
        .replaceAll(path.sep, "/");
      const slug = relative
        .replace(/\.mdx?$/, "")
        .replace(/(?:^|\/)index$/, "");
      const url = `/docs${slug ? `/${slug}` : ""}`;
      assert(!documents.has(url), `Duplicate documentation route ${url}`);
      const content = await readFile(file, "utf8");
      const tree = parser.parse(content.replace(/^---[\s\S]*?---\n/, ""));
      const anchors = new Set();
      const slugger = new Slugger();
      const text = (node) =>
        node.value ?? (node.children ?? []).map(text).join("");
      visit(tree, (node) => {
        if (node.type === "heading") anchors.add(slugger.slug(text(node)));
        if (node.type === "html")
          for (const match of node.value.matchAll(/\bid="([^"]+)"/g))
            anchors.add(match[1]);
      });
      documents.set(url, { tree, anchors, file });
    }
  }
}
await walk(path.join(root, "content/docs"));
const errors = [];
let links = 0;
for (const [url, { tree, file }] of documents) {
  visit(tree, (node) => {
    if (!["link", "image", "definition"].includes(node.type)) return;
    if (!node.url.startsWith("/docs") && !node.url.startsWith("#")) return;
    links++;
    const [destination, anchor] = node.url.split("#");
    const page = documents.get(destination || url);
    if (!page)
      errors.push(`${path.relative(root, file)}: missing route ${node.url}`);
    else if (anchor && !page.anchors.has(decodeURIComponent(anchor)))
      errors.push(`${path.relative(root, file)}: missing anchor ${node.url}`);
  });
}
const manifest = JSON.parse(
  await readFile(path.join(root, "lib/docs-manifest.json"), "utf8"),
);
assert.equal(
  new Set(manifest.pages.map((page) => page.url)).size,
  manifest.pages.length,
  "Canonical routes must be unique",
);
for (const page of manifest.pages)
  assert(documents.has(page.url), `Missing canonical page ${page.sourcePath}`);
for (const page of manifest.authoredPages)
  assert(documents.has(page.url), `Missing authored page ${page.sourcePath}`);
for (const { tree, file } of documents.values()) {
  const assets = [];
  visit(tree, (node) => {
    if (node.type === "image" && node.url.startsWith("/"))
      assets.push(node.url);
  });
  for (const asset of assets) {
    try {
      await access(path.join(root, "public", asset));
    } catch {
      errors.push(`${path.relative(root, file)}: missing asset ${asset}`);
    }
  }
}
if (errors.length) throw new Error(errors.join("\n"));
console.log(
  `Verified ${documents.size} document routes, ${links} internal links and anchors, and all local images.`,
);
