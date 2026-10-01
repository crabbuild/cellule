# Cellule website

Next.js App Router, Tailwind CSS 4, and Fumadocs. The website follows Compass's
web application structure and has its own Cellule design, SVG explorers, and
complete repository documentation.

## Develop and verify

Use Node.js 22 or newer and npm 11. From the repository root:

```sh
npm ci
npm run dev:web
```

The preview runs at `http://localhost:3000`. To select a port:

```sh
npm run dev -w @cellule/web -- --port 3100
```

```sh
npm run check:web
npm run build:web
npx playwright install chromium
npm run start -w @cellule/web -- --port 3000
```

With the server running, use another terminal:

```sh
npm run test:web
```

`WEB_TEST_URL` selects another running preview or production server. The browser
check validates the website pages and every canonical document route, search,
Markdown endpoints, unknown-page responses, mobile overflow, theme switching,
clipboard copy, interactive explorers, tenant/target selection, owner handoff,
example walkthroughs and retry explanations, diagram zoom/download, and all repository
Mermaid diagrams rendered as SVG. Screenshots are saved under the ignored
`apps/web/test-results/` directory. CI runs this against the production build.

## Content ownership

| Surface | Source |
| --- | --- |
| Website pages | `app/(site)` |
| Documentation shell and rendering | `app/docs`, `lib/source.ts`, `source.config.ts` |
| Canonical guides, references, reports, audits, and provenance | Repository `README.md`, `docs/`, `crates/`, `CONTRIBUTING.md`, `CHANGELOG.md` |
| Web-specific documentation entry pages | `content/authored/` |
| Marketing capability honeycomb | `components/product-hive.tsx` |
| Interactive high-level architecture and guided journeys | `components/architecture-explorer.tsx` |
| Cell identity and transaction boundary lab | `components/cell-model-lab.tsx` |
| Runtime placement and owner handoff lab | `components/topology-lab.tsx` |
| Guided example walkthroughs | `components/example-walkthrough.tsx`, `lib/walkthroughs.ts` |
| Cell, command, layer, and recovery explorers | `components/explorers.tsx` |
| Repository Mermaid diagrams | `components/mermaid.tsx`, `lib/remark-diagrams.mjs` |
| Shared visual tokens and responsive styles | `app/globals.css`, `app/learning.css` |

`scripts/sync-docs.mjs` discovers tracked repository Markdown, excludes agent
instructions, maps it into a deterministic documentation tree, rewrites links
to local docs or GitHub source, preserves explicit anchors, and copies local
image assets. SVGs are preferred over their raster previews. Original Markdown
and Rust examples remain authoritative; do not edit `content/docs/`.

Development startup, type checking, and production builds regenerate content.
After editing a repository guide during an existing preview, run:

```sh
npm run generate:content -w @cellule/web
```

New repository documents must be tracked by Git to join the collection. Authored
MDX belongs in `content/authored/` and is copied into the generated collection.
The link checker validates every internal documentation route and anchor.
Generated content, manifests, public repository assets, `.source/`, and `.next/`
are ignored. The lockfile is committed.

## Pages and navigation

- `/`: benefit-led product story, interactive capability honeycomb, eight
  capability summaries, application scenarios, ownership, adoption steps,
  evidence links, FAQs, and quickstart calls to action.
- `/architecture`: interactive system map with component inspection and guided
  write/recovery journeys, followed by Cell, layer, durability, and recovery
  diagrams. SVG nodes work with pointer, Enter, and Space; HTML component
  controls provide an alternative on small screens.
- `/concepts`: a Cell mental model, interactive tenant and order targets,
  transaction boundaries, partition choices, and a concise glossary.
- `/topology`: inspect Cell placement across nodes and follow the fenced writer
  handoff, including the period with no active writer.
- `/walkthroughs`: guided SQL, Blob, Workflow, and Cron/Effects examples with
  selectable SVG steps, retry explanations, commands, and source links.
- `/primitives`: all eight capabilities and their transaction boundaries.
- `/examples`: five runnable examples with copyable commands and expected output.
- `/performance`: dated report index and qualification/verification guides.
- `/roadmap` and `/changelog`: rendered directly from the canonical documents.
- `/docs`: Fumadocs sidebar, table of contents, highlighted code, source links,
  Markdown links, and search through `/api/search`.
- `/llms.txt`, `/llms-full.txt`, and `/markdown/*`: generated text for agents.
- `/sitemap.xml` and `/robots.txt`: generated from the same documentation source.

Mermaid is loaded lazily near visible diagrams and emits SVG. Each diagram has
zoom, reset, scrolling, and SVG download controls. A rendering failure preserves
the source and shows the error. Interactive conceptual diagrams illustrate the
documented contracts; they do not execute or simulate a real Cellule runtime.

## Brand assets

The Cellule mark is three separated honeycomb cells.
Use the shared `CelluleLockup` in website and documentation navigation. Its SVG
uses theme tokens, so an explicit website theme always controls its colors.

| Color | Light theme | Dark theme |
| --- | --- | --- |
| Honey, upper cell | `#E7A525` | `#FFD56A` |
| Deep amber, lower left | `#B87313` | `#E8A839` |
| Warm gold, lower right | `#D58B1A` | `#F5BE4F` |

The website uses warm white (`#FFFDF7`), honey-tinted surfaces (`#FBF3DC`),
ink (`#142944`), and a darker amber (`#925B0D`) for readable links. The homepage
leads with benefits and application patterns; its navigation jumps to the
product story, capabilities, and use cases. Technical walkthroughs live on
`/architecture` and in the docs.

The wordmark uses the site's ink color. Keep the narrow gaps between cells;
avoid outlines, gradients, and shadows that obscure the mark at small sizes.

Reusable vector exports live in `public/brand/`: `cellule-mark.svg` follows the
system theme, `cellule-mark-light.svg` and `cellule-mark-dark.svg` have fixed
colors for a chosen background, and `cellule-mark-monochrome.svg` inherits
`currentColor` when inlined. `public/icon.svg` is the favicon, with brighter
honey cells on an ink tile for legibility at 16–32 pixels.
`cellule-brand-sheet.svg` shows both themes, colors, and actual-size favicons.

## Production

Set `NEXT_PUBLIC_SITE_URL` to the intended public origin **before building**.
It controls canonical URLs, Open Graph URLs, robots, the sitemap, and the agent
documentation index. The default is `https://cellule.crab.build`; this is a
configuration default, not evidence of deployment.

```sh
NEXT_PUBLIC_SITE_URL=https://your-domain.example npm run build:web
npm run start -w @cellule/web
```

The app can run on a Node.js Next.js host or a provider with Next.js support.
Deploy the repository with its lockfile, run the build command, and start the
app with `npm run start -w @cellule/web`. The search API needs a server; the
remaining content pages are prerendered. No credentials are required.

Fonts are bundled locally through Fontsource; builds and rendering do not fetch
fonts from Google. Theme preference stays in the browser. The design supports
keyboard navigation, visible focus, mobile navigation, and reduced motion.
