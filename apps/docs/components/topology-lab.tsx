"use client";
import { useId, useState } from "react";
import { useDiagramFocus } from "./use-diagram-focus";
import { ArrowLeft, ArrowRight, RotateCcw } from "lucide-react";

type Placement = "single" | "split" | "handoff";
const cells = [
  { name: "Orders", role: "SQL" },
  { name: "Settings", role: "KV" },
  { name: "Jobs", role: "Queue" },
  { name: "Runs", role: "Workflow" },
];
const phases = [
  {
    title: "Node A serves Orders.",
    description:
      "Orders has one active fenced writer on A. Other Cells have their own ownership and continue to be independently addressed.",
    status: "A active",
    writers: 1,
  },
  {
    title: "The old writer is fenced.",
    description:
      "The old writer must no longer be able to publish before a successor becomes active. This illustration pauses serving Orders at the ownership boundary.",
    status: "No active writer",
    writers: 0,
  },
  {
    title: "Node B verifies the published state.",
    description:
      "The successor follows the authority-pinned root, reads the required objects, and verifies exact reconstruction. It does not serve Orders before recovery succeeds.",
    status: "B recovering",
    writers: 0,
  },
  {
    title: "Node B serves the same Cell.",
    description:
      "After verified reconstruction and the required ownership checks, B activates Orders. The target, durable state, and recorded request outcomes still identify the same Cell.",
    status: "B active",
    writers: 1,
  },
];
export function TopologyLab() {
  const [placement, setPlacement] = useState<Placement>("split");
  const [selected, setSelected] = useState("Orders");
  const [phase, setPhase] = useState(0);
  const id = useId();
  const diagram = useDiagramFocus(`${placement}:${selected}:${phase}`);
  const handoff = placement === "handoff";
  const activePhase = phases[phase];
  const chosen = cells.find((cell) => cell.name === selected) ?? cells[0];
  const owner =
    selected === "Orders" && handoff
      ? activePhase.status
      : placement === "single" ||
          selected === "Orders" ||
          selected === "Settings"
        ? "A active"
        : "B active";
  function choosePlacement(next: Placement) {
    setPlacement(next);
    setPhase(0);
    setSelected("Orders");
  }
  return (
    <div className="lesson-lab topology-lab">
      <div className="lesson-lab-bar">
        <div className="lesson-choices" aria-label="Placement scenario">
          {(
            [
              ["single", "One node"],
              ["split", "Two nodes"],
              ["handoff", "Owner handoff"],
            ] as const
          ).map(([key, label]) => (
            <button
              key={key}
              aria-pressed={placement === key}
              onClick={() => choosePlacement(key)}
            >
              {label}
            </button>
          ))}
        </div>
        <span className="mono">CONCEPTUAL TOPOLOGY</span>
      </div>
      <div className="topology-workspace">
        <div>
          <div
            className="lesson-diagram topology-diagram"
            ref={diagram}
            tabIndex={0}
            role="region"
            aria-label="Scrollable runtime topology diagram"
          >
            <svg
              className="lesson-svg"
              viewBox="0 0 800 580"
              role="group"
              aria-labelledby={`${id}-title`}
            >
              <title id={`${id}-title`}>
                Cell placement on two application nodes with shared authority
                and object storage. Select a Cell to inspect its owner.
              </title>
              <rect
                x="268"
                y="12"
                width="264"
                height="66"
                rx="10"
                className="lesson-node"
              />
              <text x="400" y="39" textAnchor="middle" className="svg-title">
                Your application
              </text>
              <text x="400" y="61" textAnchor="middle" className="svg-small">
                Select a target · route to its owner
              </text>
              <path
                d="M400 78V95H195V121M400 95H605V121"
                className="lesson-connector"
              />
              {[
                { x: 22, label: "A" },
                { x: 432, label: "B" },
              ].map((node) => (
                <g key={node.label}>
                  <rect
                    x={node.x}
                    y="121"
                    width="346"
                    height="292"
                    rx="16"
                    className="topology-node-frame"
                  />
                  <text x={node.x + 22} y="154" className="svg-title">
                    Node {node.label}
                  </text>
                  <text x={node.x + 22} y="178" className="svg-small">
                    Your service + Cellule
                  </text>
                </g>
              ))}
              {cells.map((cell, index) => {
                const nodeB =
                  placement !== "single" &&
                  (index >= 2 ||
                    (cell.name === "Orders" && handoff && phase >= 2));
                const x =
                  placement === "single"
                    ? 128 + (index % 2) * 132
                    : nodeB
                      ? cell.name === "Runs"
                        ? 672
                        : 538
                      : cell.name === "Settings"
                        ? 260
                        : 128;
                const y =
                  placement === "single"
                    ? 244 + Math.floor(index / 2) * 104
                    : cell.name === "Orders" && nodeB
                      ? 348
                      : 250;
                const state =
                  cell.name === "Orders" && handoff && phase === 1
                    ? "fenced"
                    : cell.name === "Orders" && handoff && phase === 2
                      ? "recovering"
                      : "one writer";
                return (
                  <g
                    key={cell.name}
                    role="button"
                    tabIndex={0}
                    aria-label={`Inspect ${cell.name} placement`}
                    aria-pressed={selected === cell.name}
                    className={`lesson-hex-button${selected === cell.name ? " is-active" : ""}${state !== "one writer" ? " is-paused" : ""}`}
                    onClick={() => setSelected(cell.name)}
                    onKeyDown={(event) => {
                      if (event.key === "Enter" || event.key === " ") {
                        event.preventDefault();
                        setSelected(cell.name);
                      }
                    }}
                  >
                    <path d={`M${x} ${y - 48}l42 24v48l-42 24-42-24v-48Z`} />
                    <text
                      x={x}
                      y={y - 9}
                      textAnchor="middle"
                      className="topology-cell-name"
                    >
                      {cell.name}
                    </text>
                    <text
                      x={x}
                      y={y + 10}
                      textAnchor="middle"
                      className="svg-small"
                    >
                      {cell.role}
                    </text>
                    <text
                      x={x}
                      y={y + 28}
                      textAnchor="middle"
                      className="topology-cell-status"
                    >
                      {state}
                    </text>
                  </g>
                );
              })}
              {placement === "single" ? (
                <text x="605" y="266" textAnchor="middle" className="svg-copy">
                  No Cells placed here
                </text>
              ) : null}
              <path
                d="M195 413V438H177V466M605 413V438H623V466M177 438H623"
                className="lesson-connector provider-connector"
              />
              <rect
                x="22"
                y="466"
                width="346"
                height="83"
                rx="12"
                className="lesson-node"
              />
              <text x="195" y="499" textAnchor="middle" className="svg-title">
                Authority
              </text>
              <text x="195" y="525" textAnchor="middle" className="svg-small">
                Owner · fence · pinned recovery root
              </text>
              <rect
                x="432"
                y="466"
                width="346"
                height="83"
                rx="12"
                className="lesson-node"
              />
              <text x="605" y="499" textAnchor="middle" className="svg-title">
                Object storage
              </text>
              <text x="605" y="525" textAnchor="middle" className="svg-small">
                Verified immutable state objects
              </text>
              <text x="400" y="572" textAnchor="middle" className="svg-small">
                The application integrates the providers and node lifecycle.
              </text>
            </svg>
          </div>
          <div
            className="lesson-choices topology-cell-picker"
            aria-label="Select a hosted Cell"
          >
            {cells.map((cell) => (
              <button
                key={cell.name}
                aria-pressed={selected === cell.name}
                onClick={() => setSelected(cell.name)}
              >
                {cell.name}
              </button>
            ))}
          </div>
        </div>
        <aside className="topology-inspector">
          <div aria-live="polite" aria-atomic="true">
            <span className="eyebrow">
              {handoff
                ? `HANDOFF · ${phase + 1} OF ${phases.length}`
                : "INSPECT THE PLACEMENT"}
            </span>
            <h3>
              {handoff
                ? activePhase.title
                : `${chosen.name} is served by node ${owner[0]}.`}
            </h3>
            <p>
              {handoff
                ? activePhase.description
                : "The target selects the Cell, and ownership determines which node serves its writes. Sharing a node does not merge the transactions of separate Cells."}
            </p>
            <dl className="identity-fields">
              <div>
                <dt>Selected Cell</dt>
                <dd>{selected}</dd>
              </div>
              <div>
                <dt>Serving state</dt>
                <dd>{owner}</dd>
              </div>
              <div>
                <dt>Active writers</dt>
                <dd>
                  {selected === "Orders" && handoff ? activePhase.writers : 1}
                </dd>
              </div>
              <div>
                <dt>Identity</dt>
                <dd>Unchanged</dd>
              </div>
            </dl>
          </div>
          {handoff ? (
            <div className="lesson-step-actions">
              <button
                className="button secondary"
                aria-label="Previous handoff step"
                disabled={phase === 0}
                onClick={() => setPhase((value) => value - 1)}
              >
                <ArrowLeft size={17} />
              </button>
              <button
                className="button primary"
                onClick={() => {
                  setSelected("Orders");
                  setPhase((value) =>
                    value === phases.length - 1 ? 0 : value + 1,
                  );
                }}
              >
                {phase === phases.length - 1
                  ? "Replay handoff"
                  : "Next handoff step"}
                {phase === phases.length - 1 ? (
                  <RotateCcw size={16} />
                ) : (
                  <ArrowRight size={16} />
                )}
              </button>
            </div>
          ) : (
            <button
              className="button primary"
              onClick={() => choosePlacement("handoff")}
            >
              Follow an owner handoff <ArrowRight size={16} />
            </button>
          )}
        </aside>
      </div>
      <p className="lesson-caption">
        Illustrative placements and a simplified recovery-based handoff. These
        controls do not provision nodes, simulate availability, or execute the
        runtime.
      </p>
    </div>
  );
}
