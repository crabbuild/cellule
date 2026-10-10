"use client";

import { useId, useState } from "react";

const readerStates = [
  { id: "behind", label: "Behind · 40", position: "40" },
  { id: "caught-up", label: "Caught up · 44", position: "44" },
  { id: "unavailable", label: "Unavailable", position: "—" },
] as const;

export function ReadConsistencyLab() {
  const [policy, setPolicy] = useState<"owner" | "replica">("replica");
  const [reader, setReader] = useState<(typeof readerStates)[number]>(
    readerStates[0],
  );
  const [sameCell, setSameCell] = useState(true);
  const id = useId();
  const accepted =
    sameCell && (policy === "owner" || reader.id === "caught-up");
  const position = policy === "owner" ? "44" : reader.position;
  const result = !sameCell
    ? "Receipt belongs to another Cell"
    : policy === "owner"
      ? "Owner read satisfies the minimum"
      : reader.id === "behind"
        ? "ReplicaBehind · no stale result"
        : reader.id === "unavailable"
          ? "ReplicaUnavailable · no automatic fallback"
          : "Replica read satisfies the minimum";
  const explanation = !sameCell
    ? "Order 43’s receipt cannot establish a minimum for Order 42. Commit sequences are scoped to a Cell; unrelated numbers cannot be compared."
    : accepted
      ? "The observed position is 44, which covers the required position 42 in this illustrated Cell incarnation. A minimum is a lower bound, so the read may include later commits."
      : reader.id === "behind"
        ? "This reader only covers position 40. An explicit replica read must prove the requested minimum or fail closed. Catch it up or deliberately choose an owner read."
        : "There is no admitted reader available. The caller receives an availability error and chooses its next action; the replica policy does not silently switch to the owner.";
  return (
    <div className="lesson-lab concept-lab" data-concept-lab="reads">
      <div className="lesson-lab-bar">
        <span className="mono">A RECEIPT IS A READ BOUNDARY</span>
      </div>
      <div className="concept-controls">
        <fieldset>
          <legend>Read policy</legend>
          <div className="lesson-choices">
            <button
              aria-pressed={policy === "owner"}
              onClick={() => setPolicy("owner")}
            >
              Current owner
            </button>
            <button
              aria-pressed={policy === "replica"}
              onClick={() => setPolicy("replica")}
            >
              Explicit replica
            </button>
          </div>
        </fieldset>
        <fieldset>
          <legend>Replica position</legend>
          <div className="lesson-choices">
            {readerStates.map((state) => (
              <button
                key={state.id}
                aria-pressed={reader.id === state.id}
                onClick={() => setReader(state)}
              >
                {state.label}
              </button>
            ))}
          </div>
        </fieldset>
        <label className="concept-toggle">
          <input
            type="checkbox"
            checked={!sameCell}
            onChange={(event) => setSameCell(!event.target.checked)}
          />
          Try a receipt from Order 43
        </label>
      </div>
      <div
        className="lesson-diagram"
        role="region"
        tabIndex={0}
        aria-label="Scrollable receipt and read diagram"
      >
        <svg
          className="lesson-svg concept-svg"
          viewBox="0 0 760 290"
          role="img"
          aria-labelledby={`${id}-title ${id}-desc`}
        >
          <title id={`${id}-title`}>
            A read must cover a receipt from the same Cell
          </title>
          <desc id={`${id}-desc`}>
            {result}. {explanation}
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
              <path d="M0 0L10 5L0 10z" fill="var(--accent)" />
            </marker>
          </defs>
          <path
            d="M222 118H286M474 118H540"
            className="lesson-connector"
            markerEnd={`url(#${id}-arrow)`}
          />
          <rect
            className="lesson-node"
            x="22"
            y="60"
            width="200"
            height="116"
            rx="12"
          />
          <text x="40" y="86" className="svg-overline">
            REQUIRED RECEIPT
          </text>
          <text x="40" y="117" className="svg-title">
            Order {sameCell ? "42" : "43"}
          </text>
          <text x="40" y="144" className="svg-copy">
            Minimum position · 42
          </text>
          <path
            d="M380 18l94 54v108l-94 54-94-54V72z"
            className="concept-cell"
          />
          <text x="380" y="89" textAnchor="middle" className="svg-overline">
            ORDERS · ORDER 42
          </text>
          <text x="380" y="121" textAnchor="middle" className="svg-title">
            {policy === "owner" ? "Current owner" : "Read replica"}
          </text>
          <text x="380" y="151" textAnchor="middle" className="svg-copy">
            Observed · {position}
          </text>
          <rect
            className={`lesson-node ${accepted ? "concept-pass" : "concept-blocked"}`}
            x="540"
            y="60"
            width="198"
            height="116"
            rx="12"
          />
          <text x="559" y="86" className="svg-overline">
            READ GATE
          </text>
          <text x="559" y="117" className="svg-title">
            {accepted ? "Return state" : "Reject read"}
          </text>
          <text x="559" y="144" className="svg-copy">
            {!sameCell
              ? "Different Cell identity"
              : accepted
                ? "Minimum is covered"
                : "Minimum is not proven"}
          </text>
          <text x="380" y="267" textAnchor="middle" className="svg-small">
            The owner and replica are illustrated at different committed
            positions.
          </text>
        </svg>
      </div>
      <p className="lesson-scroll-hint">
        Swipe to see the full diagram, or focus it and use the arrow keys.
      </p>
      <div
        className="concept-result"
        role="status"
        aria-live="polite"
        aria-atomic="true"
      >
        <strong>{result}</strong>
        <p>{explanation}</p>
      </div>
      <p className="lesson-caption">
        Illustrative positions within one incarnation. Real receipts also carry
        Cell identity and incarnation; these controls do not contact a runtime.
      </p>
    </div>
  );
}

const deliverySteps = [
  {
    title: "Record the intent",
    source: "Intent recorded",
    destination: "Not applied",
    detail:
      "The source command records its state change and Effect intent in one Cell transaction. Completion here does not mean the destination has applied it.",
  },
  {
    title: "Claim and deliver",
    source: "Lease validated",
    destination: "Delivery in flight",
    detail:
      "An explicit Effect supervisor claims the work, validates its lease, and sends the command with the stable Effect identity.",
  },
  {
    title: "Apply at the destination",
    source: "Awaiting acknowledgement",
    destination: "Inbox + state committed",
    detail:
      "The destination records the inbox identity and domain change together. In this example the inventory reservation has been applied once.",
  },
  {
    title: "Acknowledge the source",
    source: "Delivery complete",
    destination: "Reservation count · 1",
    detail:
      "The source records the terminal acknowledgement. These are two local transactions connected by durable delivery, not a shared SQL transaction.",
  },
] as const;

export function DeliveryLab() {
  const [step, setStep] = useState(0);
  const [lostAck, setLostAck] = useState(false);
  const id = useId();
  const retried = step === 3 && lostAck;
  const current = deliverySteps[step];
  const title = retried ? "Retry after a lost acknowledgement" : current.title;
  const detail = retried
    ? "The reservation already committed, but its reply was lost. Delivery retries the same Effect identity. The destination inbox recognizes it and does not apply the reservation again; the retry can complete the source acknowledgement."
    : current.detail;
  return (
    <div className="lesson-lab concept-lab" data-concept-lab="delivery">
      <div className="lesson-lab-bar">
        <span className="mono">TWO CELLS · ONE DURABLE INTENT</span>
        <span className="mono">STEP {step + 1} / 4</span>
      </div>
      <div className="concept-controls">
        <label className="concept-toggle">
          <input
            type="checkbox"
            checked={lostAck}
            onChange={(event) => setLostAck(event.target.checked)}
          />
          Lose the destination acknowledgement
        </label>
      </div>
      <div
        className="lesson-diagram"
        role="region"
        tabIndex={0}
        aria-label="Scrollable cross-Cell delivery diagram"
      >
        <svg
          className="lesson-svg concept-svg"
          viewBox="0 0 760 300"
          role="img"
          aria-labelledby={`${id}-title ${id}-desc`}
        >
          <title id={`${id}-title`}>{title}</title>
          <desc id={`${id}-desc`}>{detail}</desc>
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
              <path d="M0 0L10 5L0 10z" fill="var(--accent)" />
            </marker>
          </defs>
          <path
            d="M256 108H504"
            className="lesson-connector"
            markerEnd={`url(#${id}-arrow)`}
          />
          <path
            d="M504 201H256"
            className={`lesson-connector ${lostAck && step >= 2 ? "concept-retry" : ""}`}
            markerEnd={`url(#${id}-arrow)`}
          />
          <text x="380" y="92" textAnchor="middle" className="svg-small">
            {retried ? "Retry · same Effect ID" : "Effect · reserve inventory"}
          </text>
          <text x="380" y="225" textAnchor="middle" className="svg-small">
            {lostAck && step >= 2
              ? retried
                ? "Retry acknowledged"
                : "Reply lost"
              : "Destination acknowledgement"}
          </text>
          {[
            {
              x: 146,
              label: "ORDERS CELL",
              title: "Order 42",
              state: current.source,
            },
            {
              x: 614,
              label: "INVENTORY CELL",
              title: "Stock / item 7",
              state: retried ? "Deduplicated · count 1" : current.destination,
            },
          ].map((cell) => (
            <g key={cell.label}>
              <path
                d={`M${cell.x} 20l110 64v128l-110 64-110-64V84z`}
                className="concept-cell"
              />
              <text
                x={cell.x}
                y="100"
                textAnchor="middle"
                className="svg-overline"
              >
                {cell.label}
              </text>
              <text
                x={cell.x}
                y="137"
                textAnchor="middle"
                className="svg-title"
              >
                {cell.title}
              </text>
              <text
                x={cell.x}
                y="171"
                textAnchor="middle"
                className="svg-small"
              >
                {cell.state}
              </text>
            </g>
          ))}
        </svg>
      </div>
      <p className="lesson-scroll-hint">
        Swipe to see both Cells, or focus the diagram and use the arrow keys.
      </p>
      <div
        className="concept-result"
        role="status"
        aria-live="polite"
        aria-atomic="true"
      >
        <strong>{title}</strong>
        <p>{detail}</p>
      </div>
      <div className="concept-actions">
        <button onClick={() => setStep(0)} disabled={step === 0}>
          Restart delivery
        </button>
        <button
          onClick={() => setStep(step + 1)}
          disabled={step === deliverySteps.length - 1}
        >
          Next delivery step
        </button>
      </div>
      <p className="lesson-caption">
        Illustrative successful destination command. External services need
        their own idempotency mechanism; a Cell inbox cannot atomically commit
        another service’s side effect.
      </p>
    </div>
  );
}
