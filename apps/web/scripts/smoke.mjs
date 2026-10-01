import assert from "node:assert/strict";
import { readFile, mkdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "@playwright/test";
import { build } from "esbuild";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const origin = process.env.WEB_TEST_URL || "http://localhost:3000";
const manifest = JSON.parse(
  await readFile(path.join(root, "lib/docs-manifest.json"), "utf8"),
);
const marketing = [
  "/",
  "/architecture",
  "/concepts",
  "/topology",
  "/walkthroughs",
  "/primitives",
  "/examples",
  "/performance",
  "/roadmap",
  "/changelog",
];
const errors = [];
const repositoryOnlyGuides = ["reference", "roadmap", "verification"];
for (const guide of repositoryOnlyGuides) {
  const url = `/docs/guides/${guide}`;
  assert(!manifest.pages.some((page) => page.url === url));
  for (const route of [url, `/markdown/guides/${guide}`])
    assert.equal((await fetch(new URL(route, origin))).status, 404,
      `${route} is a repository-only document`);
  const searchResponse = await fetch(new URL(`/api/search?query=${guide}`, origin));
  assert.equal(searchResponse.status, 200);
  const results = await searchResponse.json();
  assert(results.every((result) => result.url?.split("#")[0] !== url),
    `${url} must not be a search result`);
}
for (const endpoint of ["/sitemap.xml", "/llms.txt", "/llms-full.txt"]) {
  const text = await (await fetch(new URL(endpoint, origin))).text();
  for (const guide of repositoryOnlyGuides) {
    assert(!text.includes(`/docs/guides/${guide}`));
    assert(!text.includes(`/markdown/guides/${guide}`));
  }
}
for (const route of [
  ...marketing,
  "/docs",
  ...manifest.pages.map((page) => page.url),
  ...manifest.authoredPages.map((page) => page.url),
]) {
  const response = await fetch(new URL(route, origin));
  assert.equal(response.status, 200, `${route} must render`);
  const html = await response.text();
  assert(html.includes("<h1"), `${route} needs a page heading`);
  assert(
    !html.includes("Application error: a"),
    `${route} must not contain a server error`,
  );
}
for (const route of [
  "/llms.txt",
  "/llms-full.txt",
  "/sitemap.xml",
  "/robots.txt",
  "/markdown/index",
]) {
  const response = await fetch(new URL(route, origin));
  assert.equal(response.status, 200, `${route} must render`);
  assert(
    (await response.text()).includes("Cellule") ||
      route.endsWith(".xml") ||
      route === "/robots.txt",
    `${route} needs content`,
  );
}
assert.equal(
  (await fetch(new URL("/docs/unknown-cellule-page", origin))).status,
  404,
);
assert.equal(
  (await fetch(new URL("/markdown/unknown-cellule-page", origin))).status,
  404,
);
const search = await fetch(new URL("/api/search?query=receipt", origin));
assert.equal(search.status, 200);
assert((await search.json()).length > 0, "Search must return documentation");
const markdown = await fetch(new URL("/markdown/guides/architecture", origin));
assert(
  (await markdown.text()).includes("```mermaid"),
  "Markdown must preserve the diagram source",
);
console.log(
  "All canonical routes, agent docs, metadata endpoints, 404s, and search passed.",
);

const browser = await chromium.launch();
const context = await browser.newContext({
  viewport: { width: 1440, height: 1000 },
  permissions: ["clipboard-read", "clipboard-write"],
});
const page = await context.newPage();
page.on("pageerror", (error) =>
  errors.push(`${new URL(page.url()).pathname}: ${error.message}`),
);
await mkdir(path.join(root, "test-results"), { recursive: true });
try {
  // Validate repository and authored Mermaid charts, including those below the fold.
  const bundle = await build({
    stdin: {
      contents:
        'import mermaid from "mermaid"; window.celluleMermaid = mermaid;',
      resolveDir: root,
    },
    bundle: true,
    write: false,
    format: "iife",
    platform: "browser",
  });
  const chartPage = await context.newPage();
  await chartPage.setContent("<html><body></body></html>");
  await chartPage.addScriptTag({ content: bundle.outputFiles[0].text });
  const chartErrors = await chartPage.evaluate(async (diagrams) => {
    const failed = [];
    window.celluleMermaid.initialize({
      startOnLoad: false,
      securityLevel: "strict",
    });
    for (let i = 0; i < diagrams.length; i++) {
      try {
        const { svg } = await window.celluleMermaid.render(
          `validation-${i}`,
          diagrams[i].code,
        );
        if (!svg.includes("<svg")) throw new Error("Missing SVG output");
      } catch (error) {
        failed.push(`${diagrams[i].source}: ${error.message}`);
      }
    }
    return failed;
  }, manifest.diagrams);
  assert.deepEqual(
    chartErrors,
    [],
    "Every documentation diagram must render as SVG",
  );
  await chartPage.close();

  await page.goto(origin, { waitUntil: "networkidle" });
  await page.getByRole("button", { name: "Explore Jobs", exact: true }).click();
  assert.match(
    await page.locator(".hive-description").innerText(),
    /Keep useful work moving/,
  );
  await page
    .getByRole("button", { name: "Explore Journeys", exact: true })
    .focus();
  await page.keyboard.press("Enter");
  assert.match(
    await page.locator(".hive-description").innerText(),
    /Remember the journey/,
  );
  await page
    .locator("summary")
    .filter({ hasText: "Can I try it without a cloud account?" })
    .click();
  assert(
    await page
      .locator("details[open]")
      .getByRole("link", { name: "Run the local quickstart" })
      .isVisible(),
  );
  await page
    .locator("summary")
    .filter({ hasText: "Can I try it without a cloud account?" })
    .click();
  await page.getByRole("button", { name: "Explore Data", exact: true }).click();
  await page
    .getByRole("button", { name: "Search documentation", exact: true })
    .click();
  await page
    .getByRole("combobox", { name: "Search", exact: true })
    .fill("receipt");
  await page.getByRole("option").first().waitFor();
  await page.keyboard.press("Escape");
  await page.getByRole("button", { name: "Toggle color theme" }).click();
  await page.waitForFunction(() =>
    document.documentElement.classList.contains("dark"),
  );
  await page.screenshot({
    path: path.join(root, "test-results/home-dark.png"),
    fullPage: true,
  });
  await page.getByRole("button", { name: "Toggle color theme" }).click();
  await page.screenshot({
    path: path.join(root, "test-results/home-desktop.png"),
    fullPage: true,
  });
  await page.goto(new URL("/architecture", origin).href, {
    waitUntil: "networkidle",
  });
  const architecture = page.locator(".architecture-explorer");
  await architecture
    .getByRole("button", { name: "Inspect Authority", exact: true })
    .click();
  assert.match(
    await page.locator(".architecture-inspector").innerText(),
    /never an arbitrary bucket listing/,
  );
  await architecture
    .getByRole("button", { name: "Follow a write", exact: true })
    .click();
  for (let i = 0; i < 4; i++)
    await architecture
      .getByRole("button", { name: "Next", exact: true })
      .click();
  assert.match(
    await page.locator(".architecture-inspector").innerText(),
    /Success follows recoverable durability proof/,
  );
  await architecture
    .getByRole("button", { name: "Replay", exact: true })
    .click();
  assert.match(
    await page.locator(".architecture-inspector").innerText(),
    /STEP 1 OF 5/,
  );
  await architecture
    .getByRole("button", { name: "Recover a Cell", exact: true })
    .click();
  for (let i = 0; i < 3; i++)
    await architecture
      .getByRole("button", { name: "Next", exact: true })
      .click();
  assert.match(
    await page.locator(".architecture-inspector").innerText(),
    /Restore the Cell, then activate it/,
  );
  await architecture
    .getByRole("button", { name: "Inspect Object storage", exact: true })
    .focus();
  await page.keyboard.press("Space");
  assert.match(
    await page.locator(".architecture-inspector").innerText(),
    /Storage transports the bytes/,
  );
  await page
    .getByRole("button", { name: "Inspect Jobs Cell", exact: true })
    .click();
  assert.match(await page.locator(".cell-inspector").innerText(), /shard\/0/);
  await page.getByRole("button", { name: "Next step", exact: true }).click();
  assert.match(
    await page.locator(".step-detail").innerText(),
    /Commit state and outcome/,
  );
  await page
    .getByRole("button", { name: "Follower proof", exact: true })
    .click();
  assert.match(await page.locator(".step-detail").innerText(), /STEP 1/);
  for (let i = 0; i < 4; i++)
    await page.getByRole("button", { name: "Next step", exact: true }).click();
  assert.match(await page.locator(".gate").innerText(), /open/);
  await page
    .getByRole("button", { name: "Corrupt chunk", exact: true })
    .click();
  assert.match(
    await page.locator(".recovery-detail").innerText(),
    /fails without activating/,
  );
  await page
    .getByRole("button", { name: "cellule-ltx Capture and restore" })
    .click();
  assert.match(
    await page.locator(".layer-detail").innerText(),
    /byte-identical recovery/,
  );
  await page.goto(new URL("/concepts", origin).href, {
    waitUntil: "networkidle",
  });
  await page.getByLabel("Example tenant").selectOption("Northwind");
  await page
    .getByRole("button", { name: "Select order 43", exact: true })
    .focus();
  await page.keyboard.press("Enter");
  assert.match(
    await page.locator(".identity-inspector").innerText(),
    /Northwind \/ Order 43/,
  );
  await page
    .getByRole("button", { name: "Two independent orders", exact: true })
    .click();
  assert.match(await page.locator(".boundary-answer").innerText(), /Effects/);
  await page.screenshot({
    path: path.join(root, "test-results/concepts-desktop.png"),
    fullPage: true,
  });

  await page.goto(new URL("/topology", origin).href, {
    waitUntil: "networkidle",
  });
  const servingState = page
    .locator(".topology-inspector dl > div")
    .filter({ hasText: "Serving state" });
  const writerCount = page
    .locator(".topology-inspector dl > div")
    .filter({ hasText: "Active writers" });
  await page.getByRole("button", { name: "One node", exact: true }).click();
  await page
    .getByRole("button", { name: "Inspect Runs placement", exact: true })
    .click();
  assert.match(await servingState.innerText(), /A active/);
  await page.getByRole("button", { name: "Two nodes", exact: true }).click();
  await page
    .getByRole("button", { name: "Inspect Runs placement", exact: true })
    .focus();
  await page.keyboard.press("Space");
  assert.match(await servingState.innerText(), /B active/);
  await page
    .getByRole("button", { name: "Owner handoff", exact: true })
    .click();
  for (const [state, count] of [
    ["No active writer", 0],
    ["B recovering", 0],
    ["B active", 1],
  ]) {
    await page
      .getByRole("button", { name: "Next handoff step", exact: true })
      .click();
    assert((await servingState.innerText()).includes(state));
    assert.equal(await writerCount.locator("dd").innerText(), String(count));
  }
  await page.screenshot({
    path: path.join(root, "test-results/topology-desktop.png"),
    fullPage: true,
  });
  await page
    .getByRole("button", { name: "Replay handoff", exact: true })
    .click();
  assert.match(await servingState.innerText(), /A active/);

  await page.goto(new URL("/walkthroughs", origin).href, {
    waitUntil: "networkidle",
  });
  for (const [index, id, steps, state] of [
    [0, "sql", 4, "Receipt-bound read verified"],
    [1, "blob", 4, "Content bytes verified"],
    [2, "workflow", 5, "Completed state verified"],
    [3, "schedules", 5, "One reminder verified"],
  ]) {
    await page.locator(".walkthrough-tabs button").nth(index).click();
    assert.match(
      await page.locator(".walkthrough-explanation").innerText(),
      /STEP 1 OF/,
    );
    for (let step = 1; step < steps; step++)
      await page
        .getByRole("button", { name: "Next example step", exact: true })
        .click();
    assert(
      (await page.locator(".walkthrough-state").innerText()).includes(state),
    );
    if (id === "sql" || id === "schedules") {
      await page
        .getByRole("button", {
          name:
            id === "sql"
              ? "What if the reply is lost?"
              : "What if delivery repeats?",
          exact: true,
        })
        .click();
      assert.match(
        await page.locator(".walkthrough-edge [role=status]").innerText(),
        id === "sql" ? /original request identity/ : /Reminder rows: 1/,
      );
    }
    await page
      .getByRole("button", { name: "Copy command", exact: true })
      .click();
    assert.equal(
      await page.evaluate(() => navigator.clipboard.readText()),
      `cargo run -p cellule-app --example ${id} --locked`,
    );
    await page
      .getByRole("button", { name: "Replay example", exact: true })
      .click();
    assert.match(
      await page.locator(".walkthrough-explanation").innerText(),
      /STEP 1 OF/,
    );
    assert.equal(await page.locator(".walkthrough-edge").count(), 0);
  }
  await page.locator(".walkthrough-tabs button").first().click();
  await page
    .getByRole("button", { name: "Inspect Publish step", exact: true })
    .focus();
  await page.keyboard.press("Enter");
  assert.match(
    await page.locator(".walkthrough-state").innerText(),
    /Durable outcome published/,
  );
  await page.screenshot({
    path: path.join(root, "test-results/walkthroughs-desktop.png"),
    fullPage: true,
  });

  await page.goto(new URL("/examples", origin).href, {
    waitUntil: "networkidle",
  });
  await page
    .locator("#sql")
    .getByRole("button", { name: "Copy command", exact: true })
    .click();
  assert.equal(
    await page.evaluate(() => navigator.clipboard.readText()),
    "cargo run -p cellule-app --example sql --locked",
  );
  await page.goto(new URL("/docs/guides/architecture", origin).href, {
    waitUntil: "networkidle",
  });
  const diagram = page.locator("[data-diagram]").first();
  await diagram.scrollIntoViewIfNeeded();
  await diagram.locator(".mermaid-svg svg").waitFor();
  const initialDiagramWidth = await diagram
    .locator(".mermaid-svg svg")
    .evaluate((svg) => svg.getBoundingClientRect().width);
  await diagram.getByRole("button", { name: "Zoom in", exact: true }).click();
  assert.equal(
    await diagram
      .locator(".mermaid-svg")
      .evaluate((element) => element.style.width),
    "125%",
  );
  assert(
    (await diagram
      .locator(".mermaid-svg svg")
      .evaluate((svg) => svg.getBoundingClientRect().width)) > initialDiagramWidth,
    "Zoom must increase the rendered diagram size",
  );
  const downloadPromise = page.waitForEvent("download");
  await diagram
    .getByRole("button", { name: "Download SVG", exact: true })
    .click();
  assert.equal(
    (await downloadPromise).suggestedFilename(),
    "cellule-diagram.svg",
  );
  await page.screenshot({
    path: path.join(root, "test-results/docs-desktop.png"),
  });
  await page.goto(
    new URL("/docs/crates/runtime/docs/primitives", origin).href,
    { waitUntil: "networkidle" },
  );
  assert.equal(
    await page.locator("#sql").count(),
    1,
    "Canonical explicit anchors must survive Markdown compilation",
  );

  const primitiveGuides = manifest.authoredPages.filter((guide) =>
    guide.url.startsWith("/docs/primitives"),
  );
  assert.equal(
    primitiveGuides.length,
    9,
    "Overview and all eight guides must exist",
  );
  for (const guide of primitiveGuides) {
    await page.goto(new URL(guide.url, origin).href, { waitUntil: "networkidle" });
    const diagrams = page.locator("[data-diagram]");
    assert(
      (await diagrams.count()) >= 2,
      `${guide.url} needs explanatory diagrams`,
    );
    for (const diagram of await diagrams.all()) {
      await diagram.scrollIntoViewIfNeeded();
      await diagram.locator(".mermaid-svg svg").waitFor();
      assert.equal(await diagram.locator('[role="alert"]').count(), 0);
      assert(
        await diagram.locator(".mermaid-svg svg").evaluate((svg) =>
          svg.getBoundingClientRect().width <= Math.max(360, svg.viewBox.baseVal.width) + 1,
        ),
        `${guide.url} must not stretch a narrow diagram beyond its natural width`,
      );
    }
    assert(
      (await page.locator("pre code").count()) >= 1,
      `${guide.url} needs examples`,
    );
  }
  await page.goto(new URL("/docs/primitives", origin).href, {
    waitUntil: "networkidle",
  });
  await page.getByRole("button", { name: "Follower proof", exact: true }).click();
  for (let i = 0; i < 4; i++)
    await page.getByRole("button", { name: "Next step", exact: true }).click();
  assert.match(await page.locator(".gate").innerText(), /open/);
  await page.goto(new URL("/docs/primitives/effects", origin).href, {
    waitUntil: "networkidle",
  });
  await page.locator("#follow-duplicate-delivery").scrollIntoViewIfNeeded();
  await page.screenshot({
    path: path.join(root, "test-results/effects-guide.png"),
  });

  await page.goto(new URL("/docs/guides/at-a-glance", origin).href, {
    waitUntil: "networkidle",
  });
  const cellModel = page.locator('[data-svg-diagram]').filter({
    has: page.locator('img[src$="/cell-model.svg"]'),
  });
  await cellModel.scrollIntoViewIfNeeded();
  const modelImage = cellModel.locator("img");
  assert(await modelImage.evaluate((image) => image.complete && image.naturalWidth > 0));
  const modelWidth = (await modelImage.boundingBox()).width;
  await cellModel.getByRole("button", { name: "Zoom in", exact: true }).click();
  assert((await modelImage.boundingBox()).width > modelWidth);
  await cellModel.getByRole("button", { name: "Reset diagram zoom", exact: true }).click();
  assert(Math.abs((await modelImage.boundingBox()).width - modelWidth) < 1);
  const [modelDownload] = await Promise.all([
    page.waitForEvent("download"),
    cellModel.getByRole("link", { name: "Download SVG", exact: true }).click(),
  ]);
  const downloadedModel = await readFile(await modelDownload.path(), "utf8");
  assert.match(downloadedModel, /One Cell type, independent Cells/);
  assert.match(
    await (await fetch(new URL("/markdown/guides/at-a-glance", origin))).text(),
    /!\[.*\]\(\/repository\/docs\/diagram\/cell-model\.svg\)/,
    "Markdown exports must preserve the standalone SVG illustration",
  );
  await page.setViewportSize({ width: 390, height: 844 });
  assert(
    await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth + 1),
    "Cell model illustration must scroll within its frame on mobile",
  );
  await cellModel.getByRole("region").focus();
  await page.keyboard.press("ArrowRight");
  await page.waitForFunction(() =>
    document.querySelector('[data-svg-diagram] [role="region"]').scrollLeft > 0,
  );

  // Headers and row grids must fill the table border, even for short content.
  for (const width of [1440, 390]) {
    await page.setViewportSize({ width, height: 900 });
    for (const route of [
      "/docs/guides/introduction",
      "/docs/guides/quickstart",
      "/docs/primitives",
    ]) {
      await page.goto(new URL(route, origin).href, { waitUntil: "networkidle" });
      const tables = await page.locator(".prose table").evaluateAll((tables) =>
        tables.map((table) => ({
          right: table.getBoundingClientRect().right,
          rowEdges: Array.from(table.rows, (row) =>
            row.getBoundingClientRect().right,
          ),
        })),
      );
      assert(tables.length > 0, `${route} needs table layout coverage`);
      for (const table of tables)
        assert(
          table.rowEdges.every((right) => Math.abs(table.right - right) <= 2),
          `${route} has unused space inside a table border at ${width}px`,
        );
      assert(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth + 1,
        ),
        `${route} tables must not overflow the page at ${width}px`,
      );
    }
  }

  await page.setViewportSize({ width: 390, height: 844 });
  for (const route of [
    ...marketing,
    "/docs",
    "/docs/guides/architecture",
    ...primitiveGuides.map((guide) => guide.url),
  ]) {
    await page.goto(new URL(route, origin).href, { waitUntil: "networkidle" });
    assert(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth + 1,
      ),
      `${route} overflows on mobile`,
    );
  }
  const learningDestinations = new Set();
  for (const route of ["/concepts", "/topology", "/walkthroughs"]) {
    await page.goto(new URL(route, origin).href, { waitUntil: "networkidle" });
    for (const href of await page
      .locator('main a[href^="/"]')
      .evaluateAll((links) => links.map((link) => link.getAttribute("href"))))
      learningDestinations.add(href);
  }
  for (const href of learningDestinations)
    assert.equal(
      (await fetch(new URL(href, origin))).status,
      200,
      `${href} must resolve from learning pages`,
    );
  for (const width of [320, 620, 768, 1280]) {
    await page.setViewportSize({ width, height: 900 });
    for (const route of ["/concepts", "/topology", "/walkthroughs"]) {
      await page.goto(new URL(route, origin).href, {
        waitUntil: "networkidle",
      });
      assert(
        await page.evaluate(
          () => document.documentElement.scrollWidth <= innerWidth + 1,
        ),
        `${route} overflows at ${width}px`,
      );
      assert.equal(
        await page.locator('.main-nav a[aria-current="page"]').innerText(),
        "Learn",
      );
      if (width === 320) {
        const picker =
          route === "/concepts"
            ? ".identity-order-picker"
            : route === "/topology"
              ? ".topology-cell-picker"
              : ".walkthrough-step-picker";
        await page.locator(`${picker} button`).last().click();
        assert(
          await page
            .locator(".lesson-diagram")
            .first()
            .evaluate((viewport) => {
              const active = viewport
                .querySelector('[aria-pressed="true"]')
                .getBoundingClientRect();
              const frame = viewport.getBoundingClientRect();
              return (
                active.left >= frame.left - 1 && active.right <= frame.right + 1
              );
            }),
          `${route} must keep the selected diagram node visible`,
        );
        await page.screenshot({
          path: path.join(root, `test-results/${route.slice(1)}-mobile.png`),
          fullPage: true,
        });
      }
    }
  }
  await page.setViewportSize({ width: 390, height: 844 });
  await page.goto(origin, { waitUntil: "networkidle" });
  await page
    .getByRole("button", { name: "Open navigation", exact: true })
    .click();
  await page
    .getByRole("navigation", { name: "Main navigation" })
    .getByRole("link", { name: "Docs", exact: true })
    .click();
  await page.waitForURL("**/docs");
  await page.screenshot({
    path: path.join(root, "test-results/docs-mobile.png"),
    fullPage: false,
  });
  await page.goto(origin, { waitUntil: "networkidle" });
  await page.screenshot({
    path: path.join(root, "test-results/home-mobile.png"),
    fullPage: true,
  });
  console.log(
    "Desktop/mobile layouts, search dialog, clipboard, themes, explorer interactions, and SVG controls passed.",
  );

  assert.deepEqual(errors, [], "Pages must not emit browser errors");
  console.log(
    `Passed ${marketing.length} website pages, ${manifest.pages.length} canonical and ${manifest.authoredPages.length} authored docs, search, Markdown routes, interaction and mobile checks, and ${manifest.diagrams.length} SVG diagrams.`,
  );
} finally {
  await browser.close();
}
