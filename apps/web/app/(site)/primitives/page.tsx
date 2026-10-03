import Link from "next/link";
import { PageIntro } from "@/components/page-intro";
import { PrimitiveDiagram } from "@/components/explorers";
import { primitives } from "@/lib/primitives";
import { pageMetadata } from "@/lib/site";

export const metadata = pageMetadata(
  "Primitives",
  "SQL, KV, Blob, Queue, Cron, Workflow, Activities, and Effects share one Cell durability path.",
  "/primitives",
);
export default function Primitives() {
  return (
    <>
      <PageIntro
        eyebrow="EIGHT PRIMITIVES"
        title="Choose the shape of your work."
      >
        <p>
          Relational state, atomic metadata, durable decisions, or cross-Cell
          delivery. Every primitive shares the same owner, request ledger,
          publication gate, recovery, and receipt.
        </p>
      </PageIntro>
      <div className="page-width primitive-catalog">
        {primitives.map((item) => (
          <section id={item.id} key={item.id} className="primitive-entry">
            <div>
              <span className="eyebrow">{item.role}</span>
              <h2>{item.name}</h2>
              <p>{item.rule}</p>
              <code className="handle">{item.handle}</code>
              <Link
                className="inline-link"
                href={`/docs/primitives/${item.id}`}
              >
                Read the {item.name} guide
              </Link>
            </div>
            <div>
              <PrimitiveDiagram id={item.id} />
              <div className="primitive-meta">
                <span className="mono">{item.methods}</span>
                <Link href="/examples">Run the {item.example} example</Link>
              </div>
            </div>
          </section>
        ))}
        <div className="callout">
          <h3>One Cell is one transaction boundary.</h3>
          <p>
            Effects and idempotent inboxes coordinate work across Cells. They do
            not make a SQL transaction span multiple Cells.
          </p>
          <Link href="/docs/crates/runtime/docs/primitives">
            Read all primitive contracts
          </Link>
        </div>
      </div>
    </>
  );
}
