import Link from "next/link";
import { CelluleLockup } from "./brand";

export function SiteFooter() {
  return (
    <footer className="site-footer">
      <div className="footer-main">
        <div>
          <Link href="/">
            <CelluleLockup />
          </Link>
          <p>Durable state. Inside your application.</p>
          <span className="mono">A CRABBUILD PROJECT</span>
        </div>
        <nav aria-label="Footer documentation">
          <strong>Build</strong>
          <Link href="/docs/guides/quickstart">Quickstart</Link>
          <Link href="/docs/guides/api">Typed API</Link>
          <Link href="/examples">Examples</Link>
          <Link href="/docs/guides/framework">Integration</Link>
        </nav>
        <nav aria-label="Footer project">
          <strong>Understand</strong>
          <Link href="/concepts">Think in Cells</Link>
          <Link href="/topology">Runtime topology</Link>
          <Link href="/walkthroughs">Walkthroughs</Link>
          <Link href="/architecture">Architecture</Link>
          <Link href="/primitives">Primitives</Link>
          <Link href="/performance">Evidence</Link>
          <Link href="/roadmap">Roadmap</Link>
        </nav>
        <nav aria-label="Footer community">
          <strong>Project</strong>
          <a href="https://github.com/crabbuild/cellule">GitHub</a>
          <Link href="/docs/contributing">Contributing</Link>
          <Link href="/changelog">Changelog</Link>
          <a href="https://github.com/crabbuild/cellule/blob/main/LICENSE">
            Apache 2.0
          </a>
        </nav>
      </div>
      <div className="footer-bottom">
        <span>Embedded Rust framework · SQLite + LTX</span>
        <Link href="/llms.txt">Docs for agents</Link>
      </div>
    </footer>
  );
}
