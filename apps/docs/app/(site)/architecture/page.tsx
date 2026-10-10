import Link from "next/link";
import { ArchitectureExplorer } from "@/components/architecture-explorer";
import { PageIntro } from "@/components/page-intro";
import {
  LayerExplorer,
  DurabilityExplorer,
  RecoveryExplorer,
  CellExplorer,
} from "@/components/explorers";
import { pageMetadata } from "@/lib/site";

export const metadata = pageMetadata(
  "Architecture",
  "Explore Cell identity, framework boundaries, durable commands, and exact recovery.",
  "/architecture",
);
export default function Architecture() {
  return (
    <>
      <PageIntro
        eyebrow="THE BIG PICTURE"
        title="Your application. A foundation of Cells."
      >
        <p>
          Start with the whole system. See what your application owns, where
          Cellule fits, and how a command becomes recoverable state. Select a
          component or follow a guided journey through the architecture.
        </p>
      </PageIntro>
      <div className="page-width architecture-sections">
        <section aria-labelledby="overview-title">
          <div className="section-heading">
            <div>
              <span className="eyebrow">AN INTERACTIVE SYSTEM MAP</span>
              <h2 id="overview-title">See how the pieces work together.</h2>
            </div>
            <p>
              Application policy above. Cell execution in the middle. The
              providers you integrate below.
            </p>
          </div>
          <ArchitectureExplorer />
        </section>
        <section className="architecture-row">
          <div>
            <span className="eyebrow">IDENTITY & OWNERSHIP</span>
            <h2>
              State has an address.
              <br />
              Ownership can move.
            </h2>
            <p>
              A target combines tenant, application, namespace, and partition. A
              Cell type declares the module, role, and partition rule. Changing
              the owner does not change the Cell’s identity.
            </p>
            <p>
              Place state that must commit together in the same Cell. Work
              across Cells uses Effects and destination inboxes.
            </p>
            <Link className="inline-link" href="/docs/crates/app/docs/topology">
              Read the topology contract
            </Link>
          </div>
          <CellExplorer />
        </section>
        <section>
          <div className="section-heading">
            <div>
              <span className="eyebrow">SEVEN CRATES</span>
              <h2>A layer for each responsibility.</h2>
            </div>
            <p>
              Higher layers use lower layers. The embedding application keeps
              product policy and transport endpoints.
            </p>
          </div>
          <LayerExplorer />
        </section>
        <section>
          <div className="section-heading">
            <div>
              <span className="eyebrow">DURABLE PUBLICATION</span>
              <h2>State and its answer commit together.</h2>
            </div>
            <p>
              SQLite records the mutation and outcome in one transaction. A
              recoverability proof opens the response gate.
            </p>
          </div>
          <DurabilityExplorer />
        </section>
        <section>
          <div className="section-heading">
            <div>
              <span className="eyebrow">VERIFIED RECOVERY</span>
              <h2>Restore exact state, or fail.</h2>
            </div>
            <p>
              Authority pins the root. Recovery checks the root and every
              required chunk before a successor activates the Cell.
            </p>
          </div>
          <RecoveryExplorer />
        </section>
        <section className="read-next">
          <h3>Keep exploring</h3>
          <Link href="/concepts">Think in Cells</Link>
          <Link href="/topology">Runtime topology and owner handoff</Link>
          <Link href="/walkthroughs">Interactive example walkthroughs</Link>
          <Link href="/docs/guides/architecture">
            Complete architecture guide
          </Link>
          <Link href="/docs/guides/framework">Embed a serving node</Link>
          <Link href="/docs/crates/runtime/docs/failover-and-followers">
            Follower durability and failover
          </Link>
        </section>
      </div>
    </>
  );
}
