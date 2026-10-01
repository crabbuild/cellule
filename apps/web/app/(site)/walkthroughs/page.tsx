import Link from "next/link";
import { ArrowRight } from "lucide-react";
import { PageIntro } from "@/components/page-intro";
import { LearningNav } from "@/components/learning-nav";
import { ExampleWalkthrough } from "@/components/example-walkthrough";
import { pageMetadata } from "@/lib/site";
export const metadata = pageMetadata(
  "Example walkthroughs",
  "Interactive walkthroughs of Cellule SQL, Blob, Workflow, and Cron/Effects examples, connected to runnable Rust source.",
  "/walkthroughs",
);
export default function Walkthroughs() {
  return (
    <>
      <LearningNav current="/walkthroughs" />
      <PageIntro
        eyebrow="03 / THE APPLICATION PATH"
        title="Watch an idea become durable work."
      >
        <p>
          Follow four concrete application paths, one step at a time. Inspect
          what changes, what becomes visible, and what the caller can rely on.
          Then run the real example yourself.
        </p>
      </PageIntro>
      <div className="page-width lesson-page">
        <ExampleWalkthrough />
        <section className="lesson-three">
          <article>
            <span className="lesson-kicker">BEFORE THE CALL</span>
            <h3>Choose an honest boundary.</h3>
            <p>
              Select the Cell that owns the state and keep a stable request
              identity for each logical mutation. A new logical operation needs
              a new identity.
            </p>
          </article>
          <article>
            <span className="lesson-kicker">AFTER THE CALL</span>
            <h3>Verify what you observed.</h3>
            <p>
              Use returned receipts for reads that must observe a committed
              position. Check the actual row, content, or workflow state rather
              than only the transport result.
            </p>
          </article>
          <article>
            <span className="lesson-kicker">BEYOND THE PROCESS</span>
            <h3>Make external work safe to repeat.</h3>
            <p>
              Activities, Queue consumers, and Effect destinations need explicit
              lease and idempotency handling. Durable intent does not make
              arbitrary side effects exactly-once.
            </p>
          </article>
        </section>
        <div className="lesson-next">
          <div>
            <span className="eyebrow">TAKE THE NEXT STEP</span>
            <h2>Build your own complete path.</h2>
            <p>
              The quickstart takes you through setup, observation, and a local
              recovery test. The example catalog includes all five runnable
              applications.
            </p>
          </div>
          <div className="actions">
            <Link className="button primary" href="/docs/guides/quickstart">
              Open the quickstart <ArrowRight size={17} />
            </Link>
            <Link className="button secondary" href="/examples">
              All runnable examples
            </Link>
          </div>
        </div>
      </div>
    </>
  );
}
