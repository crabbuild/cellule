"use client";

import { useId, useState } from "react";
import Link from "next/link";
import { primitives } from "@/lib/primitives";
import {
  CalendarClock,
  Database,
  DatabaseBackup,
  FileCheck2,
  FileWarning,
  FolderArchive,
  ListTodo,
  ShieldCheck,
  LockKeyhole,
  SlidersHorizontal,
  Workflow,
  OctagonX,
} from "lucide-react";
import { CelluleMark } from "./brand";
import { useDiagramFocus } from "./use-diagram-focus";

const cells = [
  {
    name: "Orders",
    partition: "order/42",
    primitive: "SQL",
    label: "Relational state and recorded request outcomes.",
    x: 230.8,
    y: 114.8,
    icon: Database,
  },
  {
    name: "Settings",
    partition: "scope/team",
    primitive: "KV",
    label: "Scoped values, conditional checks, and atomic updates.",
    x: 389.2,
    y: 114.8,
    icon: SlidersHorizontal,
  },
  {
    name: "Assets",
    partition: "object/attachment",
    primitive: "Blob",
    label: "Staged content and durable references to published Blob artifacts.",
    x: 468.4,
    y: 252,
    icon: FolderArchive,
  },
  {
    name: "Runs",
    partition: "welcome/42",
    primitive: "Workflow",
    label: "Durable decisions with supervised external activities.",
    x: 389.2,
    y: 389.2,
    icon: Workflow,
  },
  {
    name: "Jobs",
    partition: "shard/0",
    primitive: "Queue",
    label: "At-least-once messages with explicit claim leases.",
    x: 230.8,
    y: 389.2,
    icon: ListTodo,
  },
  {
    name: "Timers",
    partition: "schedule/reminder",
    primitive: "Cron",
    label:
      "Recorded, deduplicated schedule occurrences with explicit maintenance.",
    x: 151.6,
    y: 252,
    icon: CalendarClock,
  },
];
const cellHexagon = "M0 -88l76.2 44v88L0 88l-76.2-44v-88z";

export function CellExplorer() {
  const [selected, setSelected] = useState(0);
  const cell = cells[selected];
  const id = useId();
  const viewport = useDiagramFocus(cell.name);
  return (
    <div className="cell-explorer">
      <div className="explorer-heading">
        <span className="mono">ONE APPLICATION · MANY CELLS</span>
        <span className="diagram-badge">Explore a Cell</span>
      </div>
      <div
        className="cell-hive-viewport"
        ref={viewport}
        role="region"
        tabIndex={0}
        aria-label="Scrollable Cell hive"
      >
        <svg
          viewBox="0 0 620 515"
          className="cell-map"
          aria-labelledby={`${id}-title ${id}-desc`}
          role="group"
        >
          <title id={`${id}-title`}>
            Cellule surrounded by six independent Cells
          </title>
          <desc id={`${id}-desc`}>
            Orders, Settings, Assets, Runs, Jobs, and Timers each have separate
            SQLite state and one fenced writer. The center represents the
            embedded Cellule framework, not another Cell or a shared database.
            Select a surrounding hexagon to inspect its identity.
          </desc>
          <defs>
            <pattern
              id={`${id}-grid`}
              width="26"
              height="26"
              patternUnits="userSpaceOnUse"
            >
              <circle cx="1" cy="1" r="1" fill="currentColor" opacity=".1" />
            </pattern>
          </defs>
          <rect width="620" height="515" fill={`url(#${id}-grid)`} />
          <g className="cell-hive-center" transform="translate(310 252)">
            <path d={cellHexagon} />
            <g transform="translate(-17 -57)">
              <CelluleMark />
            </g>
            <text y="8" textAnchor="middle" className="cell-label">
              cellule.
            </text>
            <text y="33" textAnchor="middle" className="cell-role">
              Embedded framework
            </text>
          </g>
          {cells.map((item, i) => {
            const Icon = item.icon;
            return (
              <g
                key={item.name}
                transform={`translate(${item.x} ${item.y})`}
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
                <path d={cellHexagon} />
                <Icon
                  x={-13}
                  y={-55}
                  width={26}
                  height={26}
                  className="cell-icon"
                  aria-hidden="true"
                />
                <text y="-2" textAnchor="middle" className="cell-label">
                  {item.name}
                </text>
                <text y="21" textAnchor="middle" className="cell-role">
                  {item.primitive}
                </text>
                <rect x="-40" y="35" width="80" height="20" rx="10" />
                <text y="49" textAnchor="middle" className="cell-writer">
                  one writer
                </text>
              </g>
            );
          })}
          <text x="310" y="503" textAnchor="middle" className="map-note">
            STABLE IDENTITY · INDEPENDENT STATE
          </text>
        </svg>
      </div>
      <p className="hive-scroll-hint">
        Swipe across the hive, or select a Cell below.
      </p>
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
      <div className="cell-inspector" aria-live="polite" aria-atomic="true">
        <div>
          <span className="mono">{cell.primitive} CELL</span>
          <strong>{cell.name}</strong>
          <p>{cell.label}</p>
        </div>
        <dl>
          <div>
            <dt>Example partition</dt>
            <dd>{cell.partition}</dd>
          </div>
          <div>
            <dt>Identity</dt>
            <dd>tenant / app / namespace / partition</dd>
          </div>
        </dl>
      </div>
      <p className="hive-caption">
        The center is the embedded framework. Each surrounding Cell has its own
        SQLite state and fenced writer; the hive does not share a transaction.
      </p>
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
  const id = useId();
  const stages = [
    {
      title: "Pinned root",
      label: "SELECT AUTHORITY",
      detail: "Authority selects state",
      note: "Exact committed root",
      icon: LockKeyhole,
    },
    {
      title: "Verify every chunk",
      label: "VERIFY BYTES",
      detail: valid ? "Checksums + endpoints" : "Checksum mismatch",
      note: valid ? "All required bytes verified" : "Verification fails here",
      icon: valid ? FileCheck2 : FileWarning,
    },
    {
      title: valid ? "Exact SQLite state" : "Restore blocked",
      label: "RESTORE STATE",
      detail: valid ? "State + request ledger" : "No partial reconstruction",
      note: valid ? "Byte-identical restore" : "Required proof is missing",
      icon: DatabaseBackup,
    },
    {
      title: valid ? "Fenced successor" : "No Cell activation",
      label: "ACTIVATE WRITER",
      detail: valid ? "Resume recovered Cell" : "No writer is activated",
      note: valid ? "Verified before activation" : "Preserve the failure",
      icon: valid ? ShieldCheck : OctagonX,
    },
  ];
  return (
    <div className="interactive-panel recovery-panel">
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
      <div
        className="recovery-viewport"
        role="region"
        tabIndex={0}
        aria-label="Scrollable recovery stages"
      >
        <svg
          viewBox="0 0 960 310"
          role="img"
          aria-labelledby={`${id}-title ${id}-desc`}
          className="recovery-svg"
        >
          <title id={`${id}-title`}>
            {valid
              ? "Authority root to verified chunks to restored SQLite and active successor"
              : "Invalid chunk fails verification and blocks restoration and activation"}
          </title>
          <desc id={`${id}-desc`}>
            {valid
              ? "Authority selects the pinned root. Recovery verifies all required bytes, reconstructs SQLite and its request ledger byte-identically, then activates the fenced successor."
              : "A checksum mismatch at the verification stage blocks restoration and Cell activation. A partial or plausible snapshot cannot replace the required proof."}
          </desc>
          <defs>
            <marker
              id={`${id}-arrow`}
              viewBox="0 0 10 10"
              refX="9"
              refY="5"
              markerWidth="6"
              markerHeight="6"
              orient="auto"
            >
              <path d="M0 0L10 5L0 10z" fill="currentColor" />
            </marker>
          </defs>
          {[0, 1, 2].map((i) => (
            <path
              key={i}
              d={`M${214 + i * 241} 142h49`}
              className={`recovery-connector ${!valid && i > 0 ? "is-blocked" : ""}`}
              markerEnd={`url(#${id}-arrow)`}
            />
          ))}
          {stages.map((stage, i) => {
            const Icon = stage.icon;
            return (
              <g
                key={stage.label}
                transform={`translate(${24 + i * 241} 35)`}
                className={`recovery-stage ${!valid && i === 1 ? "is-failed" : !valid && i > 1 ? "is-blocked" : ""}`}
              >
                <rect width="190" height="219" rx="14" />
                <text x="18" y="28" className="recovery-step">
                  0{i + 1} · {stage.label}
                </text>
                <Icon
                  x="18"
                  y="49"
                  width="36"
                  height="36"
                  className="recovery-icon"
                  aria-hidden="true"
                />
                <text x="18" y="118" className="recovery-title">
                  {stage.title}
                </text>
                <text x="18" y="145" className="recovery-copy">
                  {stage.detail}
                </text>
                <path d="M18 164h154" className="recovery-divider" />
                <text x="18" y="187" className="recovery-copy">
                  {stage.note}
                </text>
              </g>
            );
          })}
          <text
            x="480"
            y="289"
            textAnchor="middle"
            className={`recovery-gate ${valid ? "is-verified" : "is-failed"}`}
          >
            {valid
              ? "VERIFIED STATE → ACTIVATION ALLOWED"
              : "VERIFICATION FAILED → ACTIVATION BLOCKED"}
          </text>
        </svg>
      </div>
      <p className="recovery-scroll-hint">
        Swipe to follow all four stages, or focus the diagram and use the arrow
        keys.
      </p>
      <p className="recovery-detail" aria-live="polite" aria-atomic="true">
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
