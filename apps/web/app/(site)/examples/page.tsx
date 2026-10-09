import Link from "next/link";
import { PageIntro } from "@/components/page-intro";
import { CopyCommand } from "@/components/copy-command";
import { pageMetadata } from "@/lib/site";

export const metadata = pageMetadata(
  "Examples",
  "Run five complete local Rust applications covering all eight Cellule primitives.",
  "/examples",
);
const examples = [
  {
    name: "basic",
    title: "Compile, write, and claim.",
    tags: "KV + QUEUE",
    description:
      "Compile two Cell types, write a setting, read it at a receipt, then claim, validate, and acknowledge a Queue job.",
    output: "setting theme: dark\nqueue job: send-email",
  },
  {
    name: "sql",
    title: "Commit an order. Read its receipt.",
    tags: "SQL",
    description:
      "Register a SQL module, provision its owner, commit a parameterized order, check the observed row, and drain the runtime.",
    output: "order 42 total: 1999 cents",
  },
  {
    name: "blob",
    title: "Stage parts. Publish content.",
    tags: "BLOB",
    description:
      "Begin an attachment, put its parts, complete its references, then read and verify its bytes at the completion receipt.",
    output: "attachment stored: receipt for order 42",
  },
  {
    name: "workflow",
    title: "Run a durable external step.",
    tags: "WORKFLOW + ACTIVITIES",
    description:
      "Start a workflow, install an Activity supervisor, execute a local Echo handler outside SQLite, and observe Completed state.",
    output: "workflow welcome/42: completed",
  },
  {
    name: "schedules",
    title: "Schedule a tick. Deliver its effect.",
    tags: "CRON + EFFECTS",
    description:
      "Store a schedule, drive one due tick, deliver a signed effect through the local peer loopback, and confirm one destination row.",
    output: "schedule reminder: one occurrence delivered",
  },
];
export default function Examples() {
  return (
    <>
      <PageIntro
        eyebrow="RUNNABLE RUST"
        title="Follow a complete application path."
      >
        <p>
          Five examples cover all eight primitives. Each includes local setup,
          real read-back checks, and shutdown. Use Rust 1.97+ and run from the
          repository root.
        </p>
      </PageIntro>
      <div className="page-width examples-page">
        <div className="lesson-next">
          <div>
            <span className="eyebrow">SEE THE PATH BEFORE RUNNING IT</span>
            <h2>Explore the interactive walkthroughs.</h2>
            <p>
              Follow an order, an upload, a workflow, or a scheduled delivery
              one step at a time.
            </p>
          </div>
          <Link href="/walkthroughs" className="button primary">
            Open walkthroughs
          </Link>
        </div>
        <div className="callout">
          <strong>Clone the repository, then run locally.</strong>
          <CopyCommand command="git clone https://github.com/crabbuild/cellule.git" />
          <CopyCommand command="cd cellule" />
          <p>
            Examples use temporary SQLite files and in-memory object storage. On
            workstations with a mounted Workspace volume, set{" "}
            <code>CARGO_TARGET_DIR</code> under{" "}
            <code>$HOME/Workspace/crabbuild-target</code> with one directory per
            checkout.
          </p>
        </div>
        {examples.map((example) => (
          <section className="example-row" id={example.name} key={example.name}>
            <div>
              <span className="eyebrow">{example.tags}</span>
              <h2>{example.title}</h2>
              <p>{example.description}</p>
              <a
                href={`https://github.com/crabbuild/cellule/blob/main/crates/cellule-app/examples/${example.name}.rs`}
                className="inline-link"
              >
                Read {example.name}.rs
              </a>
            </div>
            <div className="terminal">
              <div className="terminal-header">
                <span>{example.name}.rs</span>
                <span>LOCAL EXAMPLE</span>
              </div>
              <CopyCommand
                command={`cargo run -p cellule-app --example ${example.name} --locked`}
              />
              <div className="terminal-result">
                <span className="mono">EXPECTED OUTPUT</span>
                <pre>{example.output}</pre>
              </div>
            </div>
          </section>
        ))}
        <div className="read-next">
          <h3>Verify recovery, then embed it.</h3>
          <p>
            The quickstart ends with an all-primitives recovery scenario. A
            serving application adds provider probes, node enrollment,
            readiness, and supervised background work.
          </p>
          <Link href="/docs/guides/quickstart">Step-by-step quickstart</Link>
          <Link href="/docs/guides/framework">Service integration guide</Link>
        </div>
      </div>
    </>
  );
}
