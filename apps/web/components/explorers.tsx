"use client";

import { useId, useState } from "react";
import Link from "next/link";
import { primitives } from "@/lib/primitives";

const cells = [
  {
    name: "Orders",
    partition: "order/42",
    primitive: "SQL",
    label: "Relational state and recorded request outcomes.",
    x: 180,
    y: 167,
  },
  {
    name: "Settings",
    partition: "scope/team",
    primitive: "KV",
    label: "Scoped values, conditional checks, and atomic updates.",
    x: 309,
    y: 91,
  },
  {
    name: "Jobs",
    partition: "shard/0",
    primitive: "Queue",
    label: "At-least-once messages with explicit claim leases.",
    x: 309,
    y: 242,
  },
  {
    name: "Runs",
    partition: "welcome/42",
    primitive: "Workflow",
    label: "Durable decisions with supervised external activities.",
    x: 440,
    y: 167,
  },
];
export function CellExplorer() {
  const [selected, setSelected] = useState(0);
  const cell = cells[selected];
  const id = useId();
  return (
    <div className="cell-explorer">
      <div className="explorer-heading">
        <span className="mono">ONE APPLICATION · MANY CELLS</span>
        <span className="diagram-badge">Explore a Cell</span>
      </div>
      <svg
        viewBox="0 0 620 365"
        className="cell-map"
        aria-labelledby={`${id}-title ${id}-desc`}
        role="group"
      >
        <title id={`${id}-title`}>Independent SQLite-backed Cells</title>
        <desc id={`${id}-desc`}>
          Orders, Settings, Jobs and Runs each have a separate state partition
          and one fenced writer. Choose a Cell below to explore it.
        </desc>
        <defs>
          <pattern
            id={`${id}-grid`}
            width="26"
            height="26"
            patternUnits="userSpaceOnUse"
          >
            <circle cx="1" cy="1" r="1" fill="currentColor" opacity=".12" />
          </pattern>
        </defs>
        <rect width="620" height="365" fill={`url(#${id}-grid)`} />
        {cells.map((item, i) => (
          <g
            key={item.name}
            role="button"
            tabIndex={0}
            aria-label={`Inspect ${item.name} Cell`}
            aria-pressed={selected === i}
            onClick={() => setSelected(i)}
            onKeyDown={(event) => {
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                setSelected(i);
              }
            }}
            className={i === selected ? "cell-node selected" : "cell-node"}
          >
            <path d={`M${item.x} ${item.y - 82}l71 41v82l-71 41-71-41v-82z`} />
            <text
              x={item.x}
              y={item.y - 17}
              textAnchor="middle"
              className="cell-label"
            >
              {item.name}
            </text>
            <text
              x={item.x}
              y={item.y + 8}
              textAnchor="middle"
              className="cell-role"
            >
              {item.primitive}
            </text>
            <rect
              x={item.x - 40}
              y={item.y + 26}
              width="80"
              height="22"
              rx="11"
            />
            <text
              x={item.x}
              y={item.y + 41}
              textAnchor="middle"
              className="cell-writer"
            >
              one writer
            </text>
          </g>
        ))}
        <text x="30" y="338" className="map-note">
          STABLE IDENTITY. MOVABLE OWNERSHIP.
        </text>
      </svg>
      <div className="cell-selector" aria-label="Select a Cell">
        {cells.map((item, i) => (
          <button
            key={item.name}
            aria-pressed={selected === i}
            onClick={() => setSelected(i)}
          >
            {item.name}
          </button>
        ))}
      </div>
      <div className="cell-inspector" aria-live="polite">
        <div>
          <span className="mono">{cell.primitive} CELL</span>
          <strong>{cell.name}</strong>
          <p>{cell.label}</p>
        </div>
        <dl>
          <div>
            <dt>Partition</dt>
            <dd>{cell.partition}</dd>
          </div>
          <div>
            <dt>Identity</dt>
            <dd>tenant / app / namespace / partition</dd>
          </div>
        </dl>
      </div>
    </div>
  );
}

const objectSteps = [
  {
    title: "Accept a command",
    detail:
      "The caller supplies a stable request identity. Admission checks the current owner and its fence.",
    short: "Command",
  },
  {
    title: "Commit state and outcome",
    detail:
      "One SQLite transaction records the mutation and its request outcome. A local commit alone does not open the response gate.",
    short: "SQLite",
  },
  {
    title: "Prepare verified bytes",
    detail:
      "LTX captures the committed WAL boundary and prepares immutable objects and the exact proposed root.",
    short: "LTX objects",
  },
  {
    title: "Pin the exact root",
    detail:
      "Fenced authority compare-and-swap publishes the root. An obsolete owner cannot make its proposal authoritative.",
    short: "Authority CAS",
  },
  {
    title: "Return output and receipt",
    detail:
      "Durable publication proof opens the response gate. A later query can require this same Cell’s receipt.",
    short: "Receipt",
  },
];
const followerSteps = [
  objectSteps[0],
  objectSteps[1],
  {
    title: "Replicate the follower log",
    detail:
      "A configured follower-log path obtains recoverable follower proof for the committed command.",
    short: "Follower log",
  },
  {
    title: "Prove recoverability",
    detail:
      "The response gate requires valid recoverable follower proof. A network send by itself is insufficient.",
    short: "Follower proof",
  },
  {
    title: "Return output and receipt",
    detail:
      "Recoverable follower proof opens the gate; object publication follows. Recovery must preserve the acknowledged prefix.",
    short: "Receipt",
  },
];
export function DurabilityExplorer() {
  const [mode, setMode] = useState<"objects" | "followers">("objects");
  const [step, setStep] = useState(0);
  const steps = mode === "objects" ? objectSteps : followerSteps;
  return (
    <div className="interactive-panel">
      <div className="panel-top">
        <span className="mono">THE DURABLE COMMAND PATH</span>
        <div className="segmented" aria-label="Durability mode">
          <button
            aria-pressed={mode === "objects"}
            onClick={() => {
              setMode("objects");
              setStep(0);
            }}
          >
            Object publication
          </button>
          <button
            aria-pressed={mode === "followers"}
            onClick={() => {
              setMode("followers");
              setStep(0);
            }}
          >
            Follower proof
          </button>
        </div>
      </div>
      <svg
        viewBox="0 0 800 140"
        role="img"
        aria-label={`${mode === "objects" ? "Object publication" : "Follower proof"} sequence, step ${step + 1} of 5`}
        className="pipeline-svg"
      >
        {steps.map((item, i) => (
          <g
            key={i}
            className={i <= step ? "pipeline-node active" : "pipeline-node"}
          >
            {i < 4 && <path d={`M${147 + i * 157} 64h20`} />}
            <rect x={12 + i * 157} y="26" width="135" height="76" rx="12" />
            <text
              x={79 + i * 157}
              y="51"
              textAnchor="middle"
              className="step-number"
            >
              {i + 1}
            </text>
            <text x={79 + i * 157} y="78" textAnchor="middle">
              {item.short}
            </text>
          </g>
        ))}
      </svg>
      <div className="step-selector" aria-label="Select command step">
        {steps.map((item, i) => (
          <button
            aria-pressed={step === i}
            key={item.title}
            onClick={() => setStep(i)}
          >
            {i + 1}
            <span>{item.short}</span>
          </button>
        ))}
      </div>
      <div className="step-detail" aria-live="polite">
        <span className="mono">STEP {step + 1} / 5</span>
        <h3>{steps[step].title}</h3>
        <p>{steps[step].detail}</p>
        <span className={`gate ${step === 4 ? "open" : ""}`}>
          Response gate: {step === 4 ? "open" : "closed"}
        </span>
      </div>
      <div className="panel-bottom">
        <span>
          A lost reply requires resolution of the original request ID.
        </span>
        <button
          className="text-button"
          onClick={() => setStep((step + 1) % steps.length)}
        >
          {step === 4 ? "Restart" : "Next step"}
        </button>
      </div>
    </div>
  );
}

const layers = [
  {
    name: "cellule-host",
    label: "Run the node",
    detail: "Readiness, installed facilities, admission, drain, and shutdown.",
    url: "/docs/crates/host",
  },
  {
    name: "cellule-app",
    label: "Describe the application",
    detail:
      "Static modules, stable Cell topology, compiled descriptors, and typed author handles.",
    url: "/docs/crates/app",
  },
  {
    name: "cellule-runtime",
    label: "Coordinate Cells",
    detail:
      "Fenced owners, actors, request outcomes, receipts, primitives, and authority transitions.",
    url: "/docs/crates/runtime",
  },
  {
    name: "cellule-ltx",
    label: "Capture and restore",
    detail:
      "Managed SQLite WAL capture, verified immutable LTX roots, and byte-identical recovery.",
    url: "/docs/crates/ltx",
  },
  {
    name: "cellule-store",
    label: "Transport object bytes",
    detail:
      "Provider-neutral operations, conditional writes, bounded retries, and classified failures.",
    url: "/docs/crates/store",
  },
  {
    name: "cellule-types",
    label: "Name providers",
    detail:
      "Stable provider and bucket identities shared by the framework layers.",
    url: "/docs/crates/types",
  },
  {
    name: "cellule-peer-http",
    label: "Connect runtime peers",
    detail:
      "Optional signed peer transport that depends only on runtime. The embedding application supplies endpoints and authorization.",
    url: "/docs/crates/peer-http",
  },
];
export function LayerExplorer() {
  const [selected, setSelected] = useState(2);
  const item = layers[selected];
  return (
    <div className="layer-explorer">
      <div className="layer-list" aria-label="Explore a framework layer">
        {layers.map((layer, i) => (
          <button
            key={layer.name}
            aria-pressed={i === selected}
            onClick={() => setSelected(i)}
          >
            <span className="mono">{layer.name}</span>
            <span>{layer.label}</span>
          </button>
        ))}
      </div>
      <div className="layer-detail" aria-live="polite">
        <span className="eyebrow">FRAMEWORK LAYER</span>
        <h3>{item.name}</h3>
        <p>{item.detail}</p>
        <Link className="inline-link" href={item.url}>
          Read the crate guide
        </Link>
        <hr />
        <p className="small">
          The host also uses runtime directly. The optional{" "}
          <Link href="/docs/crates/peer-http">peer HTTP adapter</Link> depends
          on runtime. Your application owns public ingress, authorization,
          credentials, and deployment.
        </p>
      </div>
    </div>
  );
}
export function RecoveryExplorer() {
  const [valid, setValid] = useState(true);
  return (
    <div className="interactive-panel">
      <div className="panel-top">
        <span className="mono">EXACT RECOVERY</span>
        <div className="segmented">
          <button aria-pressed={valid} onClick={() => setValid(true)}>
            Verified chunks
          </button>
          <button aria-pressed={!valid} onClick={() => setValid(false)}>
            Corrupt chunk
          </button>
        </div>
      </div>
      <svg
        viewBox="0 0 720 170"
        role="img"
        aria-label={
          valid
            ? "Authority root to verified chunks to restored SQLite"
            : "Invalid chunk fails verification and blocks activation"
        }
        className="recovery-svg"
      >
        <path d="M175 80h65m200 0h65" />
        <g>
          <rect x="15" y="40" width="160" height="80" rx="12" />
          <text x="95" y="75" textAnchor="middle">
            Pinned root
          </text>
          <text x="95" y="96" textAnchor="middle" className="small-svg">
            Authority selects state
          </text>
        </g>
        <g>
          <rect x="240" y="40" width="200" height="80" rx="12" />
          <text x="340" y="75" textAnchor="middle">
            Verify every chunk
          </text>
          <text x="340" y="96" textAnchor="middle" className="small-svg">
            {valid
              ? "Bytes + checksums + endpoints"
              : "Checksum does not match"}
          </text>
        </g>
        <g className={valid ? "verified" : "failed"}>
          <rect x="505" y="40" width="200" height="80" rx="12" />
          <text x="605" y="75" textAnchor="middle">
            {valid ? "Exact SQLite state" : "Recovery fails"}
          </text>
          <text x="605" y="96" textAnchor="middle" className="small-svg">
            {valid ? "Activate fenced successor" : "No Cell activation"}
          </text>
        </g>
      </svg>
      <p className="recovery-detail" aria-live="polite">
        {valid
          ? "A successor restores the authority-pinned root and resumes the recovered request ledger. Ownership changes; Cell identity stays stable."
          : "A plausible snapshot or partial listing cannot substitute for verified bytes. Recovery fails without activating the Cell."}
      </p>
    </div>
  );
}
export function PrimitiveDiagram({ id }: { id: string }) {
  const primitive = primitives.find((item) => item.id === id);
  if (!primitive) return null;
  return (
    <svg
      className="primitive-svg"
      viewBox="0 0 700 140"
      role="img"
      aria-label={`${primitive.name}: ${primitive.diagram.join(", then ")}`}
    >
      <path d="M220 70h25m210 0h25" />
      {primitive.diagram.map((label, i) => (
        <g key={label}>
          <rect x={10 + i * 235} y="30" width="210" height="80" rx="12" />
          <text x={115 + i * 235} y="75" textAnchor="middle">
            {label}
          </text>
        </g>
      ))}
    </svg>
  );
}
