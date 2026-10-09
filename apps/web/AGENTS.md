# Cellule website contributor guide

Read the workspace `AGENTS.md`. The app uses Next.js, Tailwind 4, and Fumadocs.
Canonical framework documentation stays in the existing repository Markdown;
website-only entry pages belong in `content/authored/`. Do not edit generated
`content/docs/`, `.source/`, `lib/docs-manifest.json`, or `public/repository/`.
Run `npm run check:web` and `npm run build:web` from the workspace root. Browser
checks use `npm run test:web` against a running server; see this app's README.
Keep Cell diagrams in SVG and claims tied to documented contracts and evidence.

<!-- BEGIN:nextjs-agent-rules -->

# This is NOT the Next.js you know

This version has breaking changes — APIs, conventions, and file structure may all differ from your training data. Read the relevant guide in `node_modules/next/dist/docs/` (resolved from this file's directory; in monorepos the `next` package may not be visible from the repo root) before writing any code. Heed deprecation notices.

This block is written and re-added by `next dev` — verify at `node_modules/next/dist/server/lib/generate-agent-files.js`. Removing it from a diff only re-creates the uncommitted change; committing it with your work keeps the tree clean.

<!-- END:nextjs-agent-rules -->
