"use client";

import { useId, useState } from "react";
import Link from "next/link";
import { ArrowLeft, ArrowRight, ArrowUpRight, RotateCcw } from "lucide-react";

type NodeId =
  | "application"
  | "owner"
  | "cell"
  | "publication"
  | "authority"
  | "storage"
  | "result";
type Mode = "explore" | "write" | "recover";
const nodes: Record<
  NodeId,
  {
    title: string;
    subtitle: string;
    owner: string;
    description: string;
    takeaway: string;
    href: string;
  }
> = {
  application: {
    title: "Your application",
    subtitle: "API · authorization · typed handle",
    owner: "APPLICATION RESPONSIBILITY",
    description:
      "Your service authenticates the caller, chooses the tenant and Cell target, and invokes a typed application handle. Your domain modules define what the Cell can do.",
    takeaway: "You keep the public API and product policy.",
    href: "/docs/guides/framework",
  },
  owner: {
    title: "Fenced owner",
    subtitle: "Route · admit · coordinate",
    owner: "CELLULE RUNTIME",
    description:
      "A command reaches the node that currently owns the addressed Cell. The runtime checks ownership and its fence, admits work, and coordinates execution. Each Cell has one fenced writer at a time.",
    takeaway: "A Cell’s identity stays the same when its owner changes.",
    href: "/docs/guides/architecture",
  },
  cell: {
    title: "One Cell",
    subtitle: "SQLite state + request outcomes",
    owner: "YOUR MODULE, MANAGED BY CELLULE",
    description:
      "Your module operates on one addressed SQLite-backed state partition. A mutation and its recorded request outcome commit in the same transaction. Other Cells have independent state and ownership.",
    takeaway: "Put state that must commit together inside one Cell.",
    href: "/docs/crates/app/docs/topology",
  },
  publication: {
    title: "Durability & recovery",
    subtitle: "Verified LTX · exact state",
    owner: "CELLULE LTX + RUNTIME",
    description:
      "LTX captures SQLite changes as verified immutable objects. The object-publication path stores the bytes and pins the exact root through fenced authority compare-and-swap before success is returned.",
    takeaway:
      "Stored bytes and the authoritative published root work together.",
    href: "/docs/concepts/durability",
  },
  authority: {
    title: "Authority",
    subtitle: "Current owner · fence · pinned root",
    owner: "PROVIDER CONTRACT YOU INTEGRATE",
    description:
      "Authority records which owner may write and pins the published root used for object-store recovery. Ownership and publication updates must satisfy the authority contract; storage transport alone does not confer write authority.",
    takeaway:
      "Recovery follows the pinned root, never an arbitrary bucket listing.",
    href: "/docs/guides/architecture",
  },
  storage: {
    title: "Object storage",
    subtitle: "Immutable LTX objects",
    owner: "INFRASTRUCTURE YOU SUPPLY",
    description:
      "The configured object provider stores immutable LTX data used to reconstruct a Cell. Your application supplies credentials and provider configuration and qualifies its behavior in the intended environment.",
    takeaway:
      "Storage transports the bytes; authority determines which state counts.",
    href: "/docs/guides/framework",
  },
  result: {
    title: "Result + receipt",
    subtitle: "A recoverable command outcome",
    owner: "BACK TO YOUR APPLICATION",
    description:
      "A successful mutation returns its recorded result and an observation receipt after the durability gate. A subsequent query can require that receipt. If a reply is lost, resolve the original request identity before retrying.",
    takeaway: "A receipt lets a later read require the committed position.",
    href: "/docs/guides/api",
  },
};
const journeys = {
  write: [
    {
      node: "application",
      title: "Choose the work and its Cell.",
      description:
        "Your application authorizes the caller, selects a Cell target, and submits a command with a stable request identity.",
      signal: "The application defines the operation and its boundary.",
    },
    {
      node: "owner",
      title: "Reach the current writer.",
      description:
        "Cellule routes to the current owner and checks its fence before admitting the command. Another node cannot simply write the same Cell.",
      signal: "One fenced writer owns this Cell’s mutations.",
    },
    {
      node: "cell",
      title: "Commit the change and its outcome.",
      description:
        "The module runs inside managed SQLite. The state change and the recorded request outcome commit together in one Cell transaction.",
      signal: "The local commit alone does not open the success gate.",
    },
    {
      node: "publication",
      title: "Make the outcome recoverable.",
      description:
        "Cellule writes verified immutable LTX objects and pins their exact root through fenced authority compare-and-swap. Both the stored bytes and authoritative root matter.",
      signal: "This walkthrough follows the object-publication path.",
    },
    {
      node: "result",
      title: "Return a result your application can use.",
      description:
        "The durability gate opens. The caller receives the result and a receipt that can be supplied to a subsequent read of the Cell.",
      signal: "Success follows recoverable durability proof.",
    },
  ],
  recover: [
    {
      node: "authority",
      title: "Start from authority.",
      description:
        "The recovery path uses fenced ownership and the authority-pinned root. A successor must establish its right to own the Cell; finding files in storage is not enough.",
      signal: "The Cell keeps its identity while ownership changes.",
    },
    {
      node: "storage",
      title: "Fetch the required objects.",
      description:
        "Read the exact root and all required chunks from the configured provider. Recovery cannot substitute a newer-looking object or silently skip missing data.",
      signal: "Object names and references identify the required bytes.",
    },
    {
      node: "publication",
      title: "Verify before trusting the state.",
      description:
        "LTX verifies the pinned root and each required chunk. Missing or corrupt data fails recovery instead of producing an approximate database.",
      signal: "Reconstruction must be byte-identical or fail.",
    },
    {
      node: "cell",
      title: "Restore the Cell, then activate it.",
      description:
        "After exact reconstruction and the required ownership checks, the successor can activate the Cell with its durable state and request outcomes intact.",
      signal: "Your application addresses the same Cell after recovery.",
    },
  ],
} satisfies Record<
  "write" | "recover",
  { node: NodeId; title: string; description: string; signal: string }[]
>;
const nodePositions: {
  id: Exclude<NodeId, "cell">;
  x: number;
  y: number;
  w: number;
  h: number;
}[] = [
  { id: "application", x: 40, y: 42, w: 290, h: 94 },
  { id: "result", x: 470, y: 42, w: 290, h: 94 },
  { id: "owner", x: 40, y: 246, w: 210, h: 110 },
  { id: "publication", x: 550, y: 246, w: 210, h: 110 },
  { id: "authority", x: 40, y: 466, w: 310, h: 88 },
  { id: "storage", x: 450, y: 466, w: 310, h: 88 },
];
const modes = [
  { id: "explore", label: "Explore the system" },
  { id: "write", label: "Follow a write" },
  { id: "recover", label: "Recover a Cell" },
] as const;

export function ArchitectureExplorer() {
  const [mode, setMode] = useState<Mode>("explore");
  const [selected, setSelected] = useState<NodeId>("cell");
  const [step, setStep] = useState(0);
  const id = useId();
  const journey = mode === "explore" ? null : journeys[mode];
  const current = journey?.[step];
  const active: NodeId = current?.node ?? selected;
  const info = nodes[active];
  function selectNode(node: NodeId) {
    setSelected(node);
    setMode("explore");
    setStep(0);
  }
  function selectMode(next: Mode) {
    setMode(next);
    setStep(0);
  }
  const edges = [
    {
      name: "Request",
      from: "application",
      to: "owner",
      d: "M145 136V246",
      x: 182,
      y: 163,
    },
    {
      name: "Execute",
      from: "owner",
      to: "cell",
      d: "M250 301H324",
      x: 285,
      y: 289,
    },
    {
      name: mode === "recover" ? "Restore" : "Capture",
      from: "cell",
      to: "publication",
      d: mode === "recover" ? "M550 301H476" : "M476 301H550",
      x: 513,
      y: 289,
    },
    {
      name: "Reply",
      from: "publication",
      to: "result",
      d: "M655 246V136",
      x: 686,
      y: 163,
    },
    {
      name: "Fence",
      from: "authority",
      to: "owner",
      d: "M145 466V356",
      x: 175,
      y: 420,
    },
    {
      name: mode === "recover" ? "Fetch" : "Publish",
      from: "publication",
      to: "storage",
      d: mode === "recover" ? "M655 466V356" : "M655 356V466",
      x: 690,
      y: 420,
    },
    {
      name: mode === "recover" ? "Read root" : "Pin root",
      from: "publication",
      to: "authority",
      d: mode === "recover" ? "M285 466V430H590V356" : "M590 356V430H285V466",
      x: 435,
      y: 422,
    },
  ];
  return (
    <div className="architecture-explorer" id="system-overview">
      <div className="architecture-toolbar">
        <div className="architecture-mode" aria-label="Architecture view">
          {modes.map((item) => (
            <button
              key={item.id}
              aria-pressed={mode === item.id}
              onClick={() => selectMode(item.id)}
            >
              {item.label}
            </button>
          ))}
        </div>
        <span className="architecture-hint">
          {mode === "explore"
            ? "Select any component to explore"
            : "Follow the highlighted steps"}
        </span>
      </div>
      <div className="architecture-workspace">
        <div className="architecture-canvas">
          <div
            className="architecture-diagram-scroll"
            tabIndex={0}
            role="region"
            aria-label="Interactive architecture diagram; scroll horizontally on small screens"
          >
            <svg
              viewBox="0 0 800 586"
              className="architecture-svg"
              role="group"
              aria-labelledby={`${id}-title ${id}-description`}
            >
              <title id={`${id}-title`}>
                How your application, Cellule, authority, and object storage
                work together
              </title>
              <desc id={`${id}-description`}>
                Your application sends a command through a fenced owner into a
                SQLite-backed Cell. LTX publishes data to object storage and
                pins its root in authority before the result returns. Select a
                component or use the guided write and recovery walkthroughs.
              </desc>
              <defs>
                <marker
                  id={`${id}-arrow`}
                  viewBox="0 0 10 10"
                  refX="9"
                  refY="5"
                  markerWidth="6"
                  markerHeight="6"
                  orient="auto-start-reverse"
                >
                  <path
                    d="M1 1 9 5 1 9"
                    fill="none"
                    stroke="context-stroke"
                    strokeWidth="1.5"
                  />
                </marker>
              </defs>
              <text x="40" y="24" className="arch-region-label">
                YOUR APPLICATION
              </text>
              <rect
                x="16"
                y="188"
                width="768"
                height="208"
                rx="16"
                className="arch-runtime-boundary"
              />
              <text x="40" y="214" className="arch-region-label">
                CELLULE · EMBEDDED IN YOUR SERVICE
              </text>
              <text x="40" y="451" className="arch-region-label">
                PROVIDERS & INFRASTRUCTURE YOU INTEGRATE
              </text>
              {edges.map((edge) => (
                <g
                  key={edge.name}
                  className={`arch-edge${active === edge.from || active === edge.to ? " is-active" : ""}`}
                  aria-hidden="true"
                >
                  <path d={edge.d} markerEnd={`url(#${id}-arrow)`} />
                  <text x={edge.x} y={edge.y} textAnchor="middle">
                    {edge.name}
                  </text>
                </g>
              ))}
              {nodePositions.map(({ id: node, x, y, w, h }) => (
                <g
                  key={node}
                  className={`arch-node${active === node ? " is-active" : ""}`}
                  tabIndex={0}
                  role="button"
                  aria-label={`Inspect ${nodes[node].title}`}
                  aria-pressed={active === node}
                  onClick={() => selectNode(node)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" || event.key === " ") {
                      event.preventDefault();
                      selectNode(node);
                    }
                  }}
                >
                  <rect x={x} y={y} width={w} height={h} rx="10" />
                  <text
                    x={x + w / 2}
                    y={y + h / 2 - 4}
                    textAnchor="middle"
                    className="arch-node-title"
                  >
                    {nodes[node].title}
                  </text>
                  <text
                    x={x + w / 2}
                    y={y + h / 2 + 20}
                    textAnchor="middle"
                    className="arch-node-subtitle"
                  >
                    {nodes[node].subtitle}
                  </text>
                </g>
              ))}
              <g
                className={`arch-node arch-cell${active === "cell" ? " is-active" : ""}`}
                tabIndex={0}
                role="button"
                aria-label="Inspect One Cell"
                aria-pressed={active === "cell"}
                onClick={() => selectNode("cell")}
                onKeyDown={(event) => {
                  if (event.key === "Enter" || event.key === " ") {
                    event.preventDefault();
                    selectNode("cell");
                  }
                }}
              >
                <path d="M400 218l75 43v86l-75 43-75-43v-86Z" />
                <text
                  x="400"
                  y="283"
                  textAnchor="middle"
                  className="arch-node-title"
                >
                  One Cell
                </text>
                <text
                  x="400"
                  y="308"
                  textAnchor="middle"
                  className="arch-node-subtitle"
                >
                  SQLite state
                </text>
                <text
                  x="400"
                  y="327"
                  textAnchor="middle"
                  className="arch-node-subtitle"
                >
                  + outcomes
                </text>
                <text
                  x="400"
                  y="350"
                  textAnchor="middle"
                  className="arch-cell-note"
                >
                  ONE WRITER
                </text>
              </g>
            </svg>
          </div>
          <p className="architecture-mobile-hint">
            Swipe the diagram to explore. Or choose a component below.
          </p>
          <div
            className="architecture-component-picker"
            aria-label="Choose an architecture component"
          >
            {(Object.keys(nodes) as NodeId[]).map((node) => (
              <button
                key={node}
                aria-pressed={mode === "explore" && selected === node}
                onClick={() => selectNode(node)}
              >
                {nodes[node].title}
              </button>
            ))}
          </div>
          <p className="architecture-note">
            Conceptual overview of one Cell and the object-publication path. A
            configured{" "}
            <Link href="/docs/crates/runtime/docs/failover-and-followers">
              follower-log path
            </Link>{" "}
            can acknowledge after recoverable follower proof, with object
            publication following.
          </p>
        </div>
        <aside
          className="architecture-inspector"
          aria-label="Architecture explanation"
        >
          <div aria-live="polite" aria-atomic="true">
            <span className="eyebrow">
              {current
                ? `${mode === "write" ? "WRITE" : "RECOVERY"} · STEP ${step + 1} OF ${journey?.length}`
                : info.owner}
            </span>
            <h3>{current?.title ?? info.title}</h3>
            <p>{current?.description ?? info.description}</p>
            <div className="architecture-takeaway">
              <span>THE KEY IDEA</span>
              <p>{current?.signal ?? info.takeaway}</p>
            </div>
          </div>
          {journey ? (
            <div className="architecture-tour-controls">
              <div
                className="architecture-progress"
                aria-label="Walkthrough steps"
              >
                {journey.map((item, index) => (
                  <button
                    key={item.title}
                    aria-label={`Step ${index + 1}: ${item.title}`}
                    aria-pressed={step === index}
                    onClick={() => setStep(index)}
                  >
                    {index + 1}
                  </button>
                ))}
              </div>
              <div className="architecture-tour-actions">
                <button
                  className="button secondary"
                  disabled={step === 0}
                  onClick={() => setStep((value) => value - 1)}
                  aria-label="Previous architecture step"
                >
                  <ArrowLeft size={16} />
                </button>
                {step < journey.length - 1 ? (
                  <button
                    className="button primary"
                    onClick={() => setStep((value) => value + 1)}
                  >
                    Next <ArrowRight size={16} />
                  </button>
                ) : (
                  <button className="button primary" onClick={() => setStep(0)}>
                    Replay <RotateCcw size={16} />
                  </button>
                )}
              </div>
            </div>
          ) : (
            <button
              className="button primary architecture-start"
              onClick={() => selectMode("write")}
            >
              Start the walkthrough <ArrowRight size={16} />
            </button>
          )}
          <Link className="inline-link" href={info.href}>
            Read the related guide <ArrowUpRight size={15} />
          </Link>
        </aside>
      </div>
    </div>
  );
}
