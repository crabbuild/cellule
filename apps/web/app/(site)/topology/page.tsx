import Link from "next/link";
import { ArrowRight } from "lucide-react";
import { PageIntro } from "@/components/page-intro";
import { LearningNav } from "@/components/learning-nav";
import { TopologyLab } from "@/components/topology-lab";
import { pageMetadata } from "@/lib/site";
export const metadata = pageMetadata(
  "Runtime topology",
  "Explore how Cells sit on nodes, share provider infrastructure, and retain identity through fenced owner handoff.",
  "/topology",
);
export default function Topology() {
  return (
    <>
      <LearningNav current="/topology" />
      <PageIntro
        eyebrow="02 / THE RUNNING SYSTEM"
        title="Many Cells. Clear owners. Your infrastructure."
      >
        <p>
          A node is where your service runs. A Cell is the state it serves.
          Explore how those two ideas fit together, and what must hold when
          ownership moves.
        </p>
      </PageIntro>
      <div className="page-width lesson-page">
        <section>
          <div className="section-heading">
            <div>
              <span className="eyebrow">EXPLORE THE PLACEMENT</span>
              <h2>
                A Cell has an address.
                <br />A node has a job to do.
              </h2>
            </div>
            <p>
              Switch between one node, two nodes, and a guided owner handoff.
              Select a Cell to see its serving state.
            </p>
          </div>
          <TopologyLab />
        </section>
        <section className="lesson-three">
          <article>
            <span className="lesson-kicker">PLACEMENT</span>
            <h3>Several Cells can share a node.</h3>
            <p>
              A serving process can host multiple independently addressed Cells.
              Each keeps its own transaction boundary and fenced ownership;
              sharing a process does not combine their databases.
            </p>
          </article>
          <article>
            <span className="lesson-kicker">ROUTING</span>
            <h3>Clients address the work.</h3>
            <p>
              A typed handle selects a target. Owner routing finds the serving
              node. If you use the optional HTTP peer adapter, your application
              still provides the endpoints and authorization.
            </p>
          </article>
          <article>
            <span className="lesson-kicker">PERSISTENCE</span>
            <h3>Provider roles stay distinct.</h3>
            <p>
              Object storage transports immutable state bytes. Authority
              determines ownership and the published root. A bucket listing
              cannot elect a writer or select recovery state.
            </p>
          </article>
        </section>
        <section className="lesson-split">
          <div>
            <span className="eyebrow">ASSEMBLE A SERVING APPLICATION</span>
            <h2>
              The framework runs inside
              <br />
              the system you operate.
            </h2>
            <p>
              The local examples assemble a small owner and in-memory storage. A
              serving application adds provider probes, a node lease, readiness,
              supervised work, and an orderly drain.
            </p>
            <Link className="inline-link" href="/docs/guides/framework">
              Follow the service integration guide <ArrowRight size={16} />
            </Link>
          </div>
          <ol className="serving-checklist">
            <li>
              <strong>Compile the application.</strong>
              <p>
                Freeze modules, Cell types, schemas, and stable operation
                contracts into the descriptor.
              </p>
            </li>
            <li>
              <strong>Supply and probe providers.</strong>
              <p>
                Configure storage, authority integration, credentials, and the
                facilities your service requires.
              </p>
            </li>
            <li>
              <strong>Enroll, route, and supervise.</strong>
              <p>
                Keep the node lease live, expose readiness, and install the
                supervisors your workload uses.
              </p>
            </li>
            <li>
              <strong>Drain before withdrawing.</strong>
              <p>
                Finish accepted work and release leases, tasks, slots, and
                SQLite handles along every exit path.
              </p>
            </li>
          </ol>
        </section>
        <section className="lesson-dark">
          <span className="eyebrow">THE IMPORTANT INVARIANT</span>
          <h2>
            Moving ownership must not
            <br />
            create two active writers.
          </h2>
          <p className="lesson-dark-lead">
            Fencing is what makes the ownership boundary meaningful. A successor
            also needs valid, verified state before it can serve the Cell.
          </p>
          <div>
            <article>
              <h3>The address stays stable.</h3>
              <p>
                Applications keep using the same target rather than inventing a
                new Cell after every owner change.
              </p>
            </article>
            <article>
              <h3>Recovery has a source of truth.</h3>
              <p>
                Follow the authority-pinned root and verify all required chunks.
                Missing or corrupt data fails recovery.
              </p>
            </article>
            <article>
              <h3>Local success has a scope.</h3>
              <p>
                Provider behavior, fault cases, resource pressure, and upgrade
                compatibility need their own qualification evidence.
              </p>
            </article>
          </div>
        </section>
        <section className="lesson-resource-grid">
          <Link href="/architecture">
            <h3>Follow a single command.</h3>
            <p>
              Explore the high-level system map, publication gate, and recovery
              path.
            </p>
            <span>
              Interactive architecture <ArrowRight size={16} />
            </span>
          </Link>
          <Link href="/docs/crates/runtime/docs/failover-and-followers">
            <h3>Go deeper on failover.</h3>
            <p>
              Read the contracts for followers, promotion, and recovery beyond
              this simplified illustration.
            </p>
            <span>
              Failover and followers <ArrowRight size={16} />
            </span>
          </Link>
          <Link href="/performance">
            <h3>Inspect the operational evidence.</h3>
            <p>
              Review dated reports and the qualification requirements for your
              intended environment.
            </p>
            <span>
              Performance and verification <ArrowRight size={16} />
            </span>
          </Link>
        </section>
        <div className="lesson-next">
          <div>
            <span className="eyebrow">NEXT / THE APPLICATION PATH</span>
            <h2>See the model doing useful work.</h2>
            <p>
              Follow an order, an upload, a workflow, or a scheduled delivery
              through a complete example.
            </p>
          </div>
          <Link href="/walkthroughs" className="button primary">
            Explore the walkthroughs <ArrowRight size={17} />
          </Link>
        </div>
      </div>
    </>
  );
}
