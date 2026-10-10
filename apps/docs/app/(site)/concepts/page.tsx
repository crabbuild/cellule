import Link from "next/link";
import { ArrowRight, Box, Fingerprint, ShieldCheck } from "lucide-react";
import { PageIntro } from "@/components/page-intro";
import { LearningNav } from "@/components/learning-nav";
import { CellModelLab } from "@/components/cell-model-lab";
import { pageMetadata } from "@/lib/site";
export const metadata = pageMetadata(
  "Think in Cells",
  "An interactive introduction to Cell identity, modules, targets, and transaction boundaries.",
  "/concepts",
);
export default function Concepts() {
  return (
    <>
      <LearningNav current="/concepts" />
      <PageIntro
        eyebrow="01 / THE MENTAL MODEL"
        title="A small boundary. A useful way to think."
      >
        <p>
          A Cell is a home for related state, with an address that stays stable
          and one fenced writer at a time. Start here before thinking about
          nodes, providers, or deployment.
        </p>
      </PageIntro>
      <div className="page-width lesson-page">
        <section>
          <div className="section-heading">
            <div>
              <span className="eyebrow">MEET ONE CELL</span>
              <h2>
                Define the behavior once.
                <br />
                Give each instance its own state.
              </h2>
            </div>
            <p>
              Change the tenant and select an order. The module stays the same;
              the target chooses which Cell you are talking to.
            </p>
          </div>
          <CellModelLab />
        </section>
        <section className="lesson-three">
          <article>
            <Box />
            <h3>A module describes behavior.</h3>
            <p>
              Your Rust module declares schemas and operations. A Cell type
              binds that module to a namespace, role, and partition rule in the
              compiled application descriptor.
            </p>
            <Link href="/docs/guides/api">
              Application declarations <ArrowRight size={16} />
            </Link>
          </article>
          <article>
            <Fingerprint />
            <h3>A target gives state an address.</h3>
            <p>
              Tenant, application, namespace, and partition identify a Cell. A
              target describes the state you want, rather than the node
              currently serving it.
            </p>
            <Link href="/docs/crates/app/docs/topology">
              Identity and partition rules <ArrowRight size={16} />
            </Link>
          </article>
          <article>
            <ShieldCheck />
            <h3>An owner coordinates the work.</h3>
            <p>
              One fenced writer runs a Cell’s mutations. The owner may change
              while the Cell’s stable identity, durable state, and recorded
              outcomes remain the reference.
            </p>
            <Link href="/topology">
              See ownership on nodes <ArrowRight size={16} />
            </Link>
          </article>
        </section>
        <section className="lesson-split">
          <div>
            <span className="eyebrow">CHOOSE THE BOUNDARY</span>
            <h2>
              Start with what must
              <br />
              change together.
            </h2>
            <p>
              Choose Cell boundaries from your application’s invariants. Related
              tables can share one Cell. Separately addressed Cells have
              independent transactions, even when they use the same module.
            </p>
          </div>
          <div className="lesson-comparison">
            <article>
              <span className="mono">ENTITY PARTITIONS</span>
              <h3>One addressed unit per entity.</h3>
              <p>
                Derive a partition from a stable entity key. This can express
                independently owned orders or workflow runs when that boundary
                fits the application.
              </p>
            </article>
            <article>
              <span className="mono">FIXED SHARDS</span>
              <h3>A declared set of partitions.</h3>
              <p>
                A canonical scope maps to one of the declared shards. This fits
                scoped collections such as settings or jobs. Changing shard
                count can change routing and identity.
              </p>
            </article>
          </div>
        </section>
        <section className="lesson-dark">
          <span className="eyebrow">THREE DIFFERENT QUESTIONS</span>
          <h2>Identity. Ownership. Durability.</h2>
          <div>
            <article>
              <h3>Which state?</h3>
              <p>
                The target selects a Cell. A different tenant or partition
                addresses different state.
              </p>
            </article>
            <article>
              <h3>Who may change it?</h3>
              <p>
                Authority and fencing determine which owner may write. A storage
                location does not grant that right.
              </p>
            </article>
            <article>
              <h3>What can be recovered?</h3>
              <p>
                A successful command follows recoverable proof. A receipt lets a
                later query require the committed position.
              </p>
            </article>
          </div>
        </section>
        <section className="lesson-glossary">
          <div>
            <span className="eyebrow">A VOCABULARY YOU CAN USE</span>
            <h2>Keep these distinctions close.</h2>
          </div>
          <dl>
            <div>
              <dt>Cell ≠ table</dt>
              <dd>
                A Cell is an ownership and transaction boundary; it can contain
                related tables and recorded outcomes.
              </dd>
            </div>
            <div>
              <dt>Cell ≠ node</dt>
              <dd>
                A node can host multiple Cells. Moving a Cell to another owner
                does not create a new identity.
              </dd>
            </div>
            <div>
              <dt>Receipt ≠ request ID</dt>
              <dd>
                A request identity tracks a logical mutation and its outcome. A
                receipt identifies an observation position for reads.
              </dd>
            </div>
            <div>
              <dt>Effect ≠ shared transaction</dt>
              <dd>
                An Effect records delivery intent across a Cell boundary. The
                destination applies it idempotently in its own transaction.
              </dd>
            </div>
          </dl>
        </section>
        <div className="lesson-next">
          <div>
            <span className="eyebrow">NEXT / THE RUNNING SYSTEM</span>
            <h2>Where do the Cells live?</h2>
            <p>
              Place independent Cells on nodes and follow what changes during an
              ownership handoff.
            </p>
          </div>
          <Link href="/topology" className="button primary">
            Explore runtime topology <ArrowRight size={17} />
          </Link>
        </div>
      </div>
    </>
  );
}
