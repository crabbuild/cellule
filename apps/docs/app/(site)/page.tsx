import Link from "next/link";
import {
  ArrowRight,
  ArrowUpRight,
  Check,
  Database,
  FileBox,
  GitBranch,
  KeyRound,
  ListTodo,
  CalendarClock,
  Radio,
  Workflow,
  ShieldCheck,
  Layers3,
  MoveRight,
  Code2,
  Github,
} from "lucide-react";
import { ProductHive } from "@/components/product-hive";
import { LearningLinks } from "@/components/learning-nav";
import { CelluleMark } from "@/components/brand";

const capabilities = [
  {
    name: "SQL",
    id: "sql",
    icon: Database,
    title: "Keep the whole picture.",
    description:
      "Model related records with familiar SQL. Commit a change and its outcome together, then read the state you just saved.",
  },
  {
    name: "KV",
    id: "kv",
    icon: KeyRound,
    title: "Make small changes count.",
    description:
      "Store settings, metadata, and versioned values. Combine checks and updates in one atomic operation.",
  },
  {
    name: "Blob",
    id: "blob",
    icon: FileBox,
    title: "Give content a home.",
    description:
      "Manage multipart content and durable references. Make a completed upload visible at a defined commit point.",
  },
  {
    name: "Queue",
    id: "queue",
    icon: ListTodo,
    title: "Turn pending into progress.",
    description:
      "Hand work to background consumers through durable messages, leased claims, and explicit acknowledgements.",
  },
  {
    name: "Cron",
    id: "cron",
    icon: CalendarClock,
    title: "Remember what comes next.",
    description:
      "Keep recurring schedules in durable state and turn due occurrences into deliverable work.",
  },
  {
    name: "Workflow",
    id: "workflow",
    icon: GitBranch,
    title: "Stay with the journey.",
    description:
      "Record decisions and history for processes that span steps, signals, and changes of ownership.",
  },
  {
    name: "Activities",
    id: "activities",
    icon: Workflow,
    title: "Connect to the outside world.",
    description:
      "Supervise external work requested by a workflow, validate leases, and record its completion.",
  },
  {
    name: "Effects",
    id: "effects",
    icon: Radio,
    title: "Help independent parts cooperate.",
    description:
      "Save delivery intent in one Cell and apply it through an idempotent inbox in the destination Cell.",
  },
];
const questions = [
  {
    question: "What is Cellule?",
    answer:
      "Cellule is an open-source Rust framework for applications that need durable, distributed state. You define your data and operations in modules, organize state into Cells, and embed the framework in your application. Each Cell has SQLite-backed state and one fenced writer at a time.",
    href: "/docs/guides/at-a-glance",
    link: "Meet the Cell model",
  },
  {
    question: "Is it a hosted database or a managed service?",
    answer:
      "Cellule is an embedded framework. Your application supplies its public API, authorization, providers, credentials, and deployment. That gives your team control over the product boundary and infrastructure choices; it also means operating and qualifying that integration is your responsibility.",
    href: "/docs/guides/framework",
    link: "Read the integration guide",
  },
  {
    question: "How is a Cell different from a table?",
    answer:
      "A Cell is a separately addressed state partition with its own ownership and durability boundary. It can contain related state that must change together. A table organizes records within a database; a Cell defines the unit that executes, commits, and recovers independently.",
    href: "/docs/crates/app/docs/topology",
    link: "Understand Cell boundaries",
  },
  {
    question: "Can I combine data, jobs, and workflows?",
    answer:
      "Yes. Cellule exposes eight capabilities through typed application handles. You choose the Cell types and partitioning for your application. One command changes one Cell; work between Cells uses durable Effects and idempotent destination inboxes. External work should be safe to repeat.",
    href: "/primitives",
    link: "Explore the capabilities",
  },
  {
    question: "Can I try it without a cloud account?",
    answer:
      "Yes. The repository includes five runnable Rust examples using temporary SQLite files and in-memory object storage. Together they cover all eight capabilities. Start with the local quickstart, then use the framework guide when you are ready to assemble a serving application.",
    href: "/docs/guides/quickstart",
    link: "Run the local quickstart",
  },
  {
    question: "How should I evaluate it for production?",
    answer:
      "Start with the implemented contracts and current roadmap, then examine the dated verification and performance reports. Qualify your own provider, resource limits, failure cases, and upgrade path before relying on a deployment. The documentation distinguishes local behavior from broader operational evidence.",
    href: "/performance",
    link: "Review the evidence",
  },
];

export default function Home() {
  return (
    <div className="marketing-home">
      <section
        className="marketing-hero page-width"
        aria-labelledby="home-title"
      >
        <div className="marketing-hero-copy">
          <span className="launch-label">
            <span /> OPEN SOURCE. BUILT FOR RUST.
          </span>
          <h1 id="home-title">
            Big ideas.
            <br />
            Small Cells.
            <br />
            <span>Lasting state.</span>
          </h1>
          <p className="marketing-lead">Build applications that remember.</p>
          <p className="marketing-intro">
            Bring durable data, background work, and workflows into your Rust
            application. Give each part a reliable place to live, and focus on
            what your product does next.
          </p>
          <div className="actions">
            <Link href="/docs/guides/quickstart" className="button primary">
              Start building <ArrowUpRight size={18} />
            </Link>
            <Link href="/architecture" className="button secondary">
              See how it fits <ArrowRight size={17} />
            </Link>
          </div>
          <p className="hero-assurance">
            <Check size={15} /> Start locally <span>·</span> No cloud account
            needed <span>·</span> Apache 2.0
          </p>
        </div>
        <ProductHive />
      </section>

      <div
        className="product-facts page-width"
        aria-label="Cellule at a glance"
      >
        <div>
          <Code2 />
          <span>
            <strong>Made for Rust</strong>Embedded in your application
          </span>
        </div>
        <div>
          <Database />
          <span>
            <strong>Built on SQLite</strong>Familiar relational foundations
          </span>
        </div>
        <div>
          <Layers3 />
          <span>
            <strong>Eight capabilities</strong>One consistent Cell model
          </span>
        </div>
        <div>
          <Github />
          <span>
            <strong>Open by design</strong>Source, contracts, and evidence
          </span>
        </div>
      </div>

      <section
        className="marketing-section page-width product-story"
        id="why-cellule"
        aria-labelledby="story-title"
      >
        <div>
          <span className="eyebrow">ROOM FOR YOUR NEXT BIG IDEA</span>
          <h2 id="story-title">
            Your product has many parts.
            <br />
            <span>Give them a common foundation.</span>
          </h2>
        </div>
        <div className="story-copy">
          <p>
            An order is more than a row. A signup is more than a request. A
            useful product has data to save, work to finish, files to keep, and
            decisions to remember.
          </p>
          <p>
            Cellule brings those needs into a shared model: small, independently
            addressed Cells. Keep related state together. Connect work across
            boundaries. Build around your domain, with durability as part of the
            foundation.
          </p>
          <Link className="inline-link" href="/docs/guides/at-a-glance">
            Get to know Cellule <ArrowUpRight size={16} />
          </Link>
        </div>
      </section>

      <section
        className="page-width home-learning"
        aria-label="Explore Cellule"
      >
        <span className="eyebrow">EXPLORE CELLULE, ONE IDEA AT A TIME</span>
        <LearningLinks />
      </section>

      <section className="promise-band" aria-labelledby="promise-title">
        <div className="page-width">
          <span className="eyebrow">CONFIDENCE IN THE WORK THAT MATTERS</span>
          <h2 id="promise-title">
            Make progress.
            <br />
            Keep the progress you make.
          </h2>
          <div className="promise-grid">
            <article>
              <ShieldCheck />
              <h3>A meaningful “saved.”</h3>
              <p>
                A successful command is backed by a recoverable outcome. Your
                application can use the returned receipt to read the committed
                state.
              </p>
              <Link href="/docs/concepts/durability">
                What durability means <ArrowUpRight size={15} />
              </Link>
            </article>
            <article>
              <GitBranch />
              <h3>Boundaries you can understand.</h3>
              <p>
                Each Cell has its own identity and one writer at a time. Choose
                boundaries that match the work your product needs to commit
                together.
              </p>
              <Link href="/docs/crates/app/docs/topology">
                How Cells fit together <ArrowUpRight size={15} />
              </Link>
            </article>
            <article>
              <Layers3 />
              <h3>A path back to exact state.</h3>
              <p>
                Recovery verifies the published state before activating a Cell.
                Ownership can move while the identity your application uses
                stays stable.
              </p>
              <Link href="/docs/concepts/recovery">
                Explore recovery <ArrowUpRight size={15} />
              </Link>
            </article>
          </div>
        </div>
      </section>

      <section
        className="marketing-section page-width"
        id="capabilities"
        aria-labelledby="capabilities-title"
      >
        <div className="marketing-section-heading">
          <div>
            <span className="eyebrow">THE BUILDING BLOCKS</span>
            <h2 id="capabilities-title">
              More of what your product needs.
              <br />
              <span>One way to think about state.</span>
            </h2>
          </div>
          <p>
            From the first saved setting to a long-running business process,
            choose the capabilities that fit your application.
          </p>
        </div>
        <div className="capability-catalog">
          {capabilities.map(({ icon: Icon, ...item }) => (
            <Link
              href={`/docs/primitives/${item.id}`}
              key={item.id}
              className="capability-item"
            >
              <div className="capability-label">
                <Icon size={23} />
                <span>{item.name}</span>
                <ArrowUpRight size={17} />
              </div>
              <h3>{item.title}</h3>
              <p>{item.description}</p>
            </Link>
          ))}
        </div>
        <p className="capability-footnote">
          A command commits within one Cell. Effects connect work between Cells.{" "}
          <Link href="/primitives">
            See the complete capability guide <ArrowRight size={15} />
          </Link>
        </p>
      </section>

      <section
        className="use-cases-section"
        id="use-cases"
        aria-labelledby="use-cases-title"
      >
        <div className="page-width marketing-section">
          <div className="marketing-section-heading">
            <div>
              <span className="eyebrow">IMAGINE WHAT YOU COULD BUILD</span>
              <h2 id="use-cases-title">
                The foundation is small.
                <br />
                <span>The possibilities are yours.</span>
              </h2>
            </div>
            <p>
              These application patterns show how Cellule’s capabilities can
              work together. You bring the product logic and the integration.
            </p>
          </div>
          <div className="use-case-list">
            <article>
              <div className="use-case-category">SAAS & COLLABORATION</div>
              <div>
                <h3>A home for each customer’s work.</h3>
                <p>
                  Organize product state around the entities and scopes your
                  application understands. Keep settings and metadata
                  consistent, attach content, and hand background tasks to
                  durable queues.
                </p>
                <div className="use-case-tags">
                  <span>SQL + KV</span>
                  <span>Blob</span>
                  <span>Queue</span>
                </div>
              </div>
              <Link href="/docs/guides/quickstart">
                Start with data <ArrowUpRight size={17} />
              </Link>
            </article>
            <article>
              <div className="use-case-category">COMMERCE & OPERATIONS</div>
              <div>
                <h3>Follow the order beyond checkout.</h3>
                <p>
                  Save related order state in one Cell. Record a fulfillment
                  journey in a workflow, schedule follow-up work, and connect
                  separate Cells through durable delivery intent.
                </p>
                <div className="use-case-tags">
                  <span>SQL</span>
                  <span>Workflow</span>
                  <span>Effects</span>
                </div>
              </div>
              <Link href="/examples">
                Explore the examples <ArrowUpRight size={17} />
              </Link>
            </article>
            <article>
              <div className="use-case-category">
                AUTOMATION & AGENT SYSTEMS
              </div>
              <div>
                <h3>Give long-running work a memory.</h3>
                <p>
                  Coordinate multi-step jobs with recorded decisions, incoming
                  signals, and supervised Activities. Your application supplies
                  the tools and external services; Cellule keeps the workflow’s
                  durable history.
                </p>
                <div className="use-case-tags">
                  <span>Workflow</span>
                  <span>Activities</span>
                  <span>Cron</span>
                </div>
              </div>
              <Link href="/docs/primitives/workflow">
                Build a workflow <ArrowUpRight size={17} />
              </Link>
            </article>
          </div>
        </div>
      </section>

      <section
        className="marketing-section page-width ownership-story"
        aria-labelledby="ownership-title"
      >
        <div>
          <span className="eyebrow">YOUR APPLICATION. YOUR CHOICES.</span>
          <h2 id="ownership-title">
            Cellule inside.
            <br />
            <span>Your product all around it.</span>
          </h2>
          <p>
            Keep the API, authorization, and product experience your team
            chooses. Embed Cellule where you need durable state and supply the
            infrastructure that fits your application.
          </p>
          <p>
            You define what the product does. Cellule provides the contracts for
            how its Cells execute, publish outcomes, and recover.
          </p>
          <Link href="/architecture" className="inline-link">
            Explore the interactive architecture <ArrowUpRight size={16} />
          </Link>
        </div>
        <div className="ownership-illustration">
          <div className="ownership-outer">
            <span className="mono">YOUR APPLICATION</span>
            <div className="ownership-choices">
              <span>Product experience</span>
              <span>API & authorization</span>
              <span>Infrastructure choices</span>
            </div>
            <div className="ownership-core">
              <CelluleMark />
              <h3>Cellule</h3>
              <p>Data. Work. Durable progress.</p>
              <div>
                <span>Clear ownership</span>
                <span>Recoverable outcomes</span>
                <span>Verified state</span>
              </div>
            </div>
            <span className="ownership-caption">
              Embedded in the system you build.
            </span>
          </div>
        </div>
      </section>

      <section
        className="getting-started-section page-width"
        aria-labelledby="get-started-title"
      >
        <div className="marketing-section-heading">
          <div>
            <span className="eyebrow">FROM CURIOUS TO BUILDING</span>
            <h2 id="get-started-title">
              Start small.
              <br />
              <span>Understand every step.</span>
            </h2>
          </div>
          <p>
            Try a complete local path before choosing infrastructure. The
            examples use temporary files and in-memory storage.
          </p>
        </div>
        <ol className="adoption-steps">
          <li>
            <span className="adoption-number">01</span>
            <h3>See a Cell in action.</h3>
            <p>
              Run the SQL example. Save an order, read the committed value, and
              follow the result back to the code.
            </p>
            <Link href="/docs/guides/quickstart">
              Follow the quickstart <MoveRight size={17} />
            </Link>
          </li>
          <li>
            <span className="adoption-number">02</span>
            <h3>Find your building blocks.</h3>
            <p>
              Explore five runnable examples covering data, files, jobs,
              workflows, and recurring delivery.
            </p>
            <Link href="/examples">
              Browse the examples <MoveRight size={17} />
            </Link>
          </li>
          <li>
            <span className="adoption-number">03</span>
            <h3>Make the integration yours.</h3>
            <p>
              Define your Cell boundaries, supply your providers, and assemble
              the serving lifecycle for your application.
            </p>
            <Link href="/docs/guides/framework">
              Plan your integration <MoveRight size={17} />
            </Link>
          </li>
        </ol>
      </section>

      <section
        className="marketing-section page-width evidence-story"
        aria-labelledby="evidence-title"
      >
        <div>
          <span className="eyebrow">OPEN SOURCE. OPEN TO INSPECTION.</span>
          <h2 id="evidence-title">Look beyond the promise.</h2>
          <p>
            A foundation deserves a closer look. Read the implementation,
            understand the current scope, and evaluate the recorded evidence
            against the needs of your application.
          </p>
        </div>
        <div className="evidence-links">
          <Link href="/performance">
            <span>
              <strong>Performance & verification</strong>
              <small>Dated measurements and the environments behind them</small>
            </span>
            <ArrowUpRight />
          </Link>
          <Link href="/roadmap">
            <span>
              <strong>Scope & roadmap</strong>
              <small>Implemented behavior and work still to qualify</small>
            </span>
            <ArrowUpRight />
          </Link>
          <a href="https://github.com/crabbuild/cellule">
            <span>
              <strong>Source on GitHub</strong>
              <small>Apache 2.0 framework · contributions welcome</small>
            </span>
            <ArrowUpRight />
          </a>
        </div>
      </section>

      <section
        className="marketing-section page-width faq-section"
        aria-labelledby="faq-title"
      >
        <div>
          <span className="eyebrow">A FEW GOOD QUESTIONS</span>
          <h2 id="faq-title">Before you build.</h2>
          <p>
            Start with the essentials. The docs are there when you want the
            details.
          </p>
          <Link href="/docs" className="inline-link">
            Browse all documentation <ArrowUpRight size={16} />
          </Link>
        </div>
        <div className="faq-list">
          {questions.map((item) => (
            <details key={item.question}>
              <summary>
                {item.question}
                <span aria-hidden="true">+</span>
              </summary>
              <div>
                <p>{item.answer}</p>
                <Link href={item.href}>
                  {item.link} <ArrowUpRight size={15} />
                </Link>
              </div>
            </details>
          ))}
        </div>
      </section>

      <section className="marketing-cta page-width" aria-labelledby="cta-title">
        <CelluleMark />
        <span className="eyebrow">BUILD SOMETHING THAT LASTS</span>
        <h2 id="cta-title">
          Your next idea.
          <br />A durable place to begin.
        </h2>
        <p>Start with one Cell. Discover what you can build around it.</p>
        <div className="actions">
          <Link href="/docs/guides/quickstart" className="button primary">
            Start building <ArrowUpRight size={18} />
          </Link>
          <a
            href="https://github.com/crabbuild/cellule"
            className="button secondary"
          >
            <Github size={18} /> Explore the source
          </a>
        </div>
      </section>
    </div>
  );
}
