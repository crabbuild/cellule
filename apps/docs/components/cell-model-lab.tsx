"use client";
import { useId, useState } from "react";
import { useDiagramFocus } from "./use-diagram-focus";

export function CellModelLab() {
  const [tenant, setTenant] = useState("Acme");
  const [order, setOrder] = useState(42);
  const [boundary, setBoundary] = useState<"together" | "across">("together");
  const id = useId();
  const diagram = useDiagramFocus(`${tenant}:${order}`);
  return (
    <div className="lesson-lab cell-model-lab">
      <div className="lesson-lab-bar">
        <span className="mono">ONE DEFINITION. INDEPENDENT CELLS.</span>
        <label>
          Tenant{" "}
          <select
            value={tenant}
            onChange={(event) => setTenant(event.target.value)}
            aria-label="Example tenant"
          >
            <option>Acme</option>
            <option>Northwind</option>
          </select>
        </label>
      </div>
      <div className="identity-workspace">
        <div className="identity-visual">
          <div
            className="lesson-diagram"
            ref={diagram}
            tabIndex={0}
            role="region"
            aria-label="Scrollable Cell identity diagram"
          >
            <svg
              viewBox="0 0 720 340"
              role="group"
              aria-labelledby={`${id}-title`}
              className="lesson-svg identity-svg"
            >
              <title id={`${id}-title`}>
                One Orders module defines the behavior of two independently
                addressed Cells. Select order 42 or 43.
              </title>
              <rect
                x="22"
                y="76"
                width="232"
                height="190"
                rx="14"
                className="lesson-node"
              />
              <text x="46" y="112" className="svg-overline">
                THE DEFINITION
              </text>
              <text x="46" y="148" className="svg-title">
                Orders module
              </text>
              <text x="46" y="179" className="svg-copy">
                Schemas + operations
              </text>
              <text x="46" y="220" className="svg-small">
                Cell type: Orders
              </text>
              <text x="46" y="241" className="svg-small">
                Partition rule: entity key
              </text>
              <path
                d="M265 168H307m-8-6 8 6-8 6"
                className="lesson-connector"
              />
              <text x="335" y="49" className="svg-overline">
                THE INSTANCES · {tenant.toUpperCase()}
              </text>
              {[42, 43].map((value, index) => (
                <g
                  key={value}
                  role="button"
                  tabIndex={0}
                  aria-label={`Select order ${value}`}
                  aria-pressed={order === value}
                  className={`lesson-hex-button${order === value ? " is-active" : ""}`}
                  onClick={() => setOrder(value)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" || event.key === " ") {
                      event.preventDefault();
                      setOrder(value);
                    }
                  }}
                >
                  <path
                    d={`M${400 + index * 166} 82l66 38v76l-66 38-66-38v-76Z`}
                  />
                  <text
                    x={400 + index * 166}
                    y="147"
                    textAnchor="middle"
                    className="svg-title"
                  >
                    Order {value}
                  </text>
                  <text
                    x={400 + index * 166}
                    y="174"
                    textAnchor="middle"
                    className="svg-copy"
                  >
                    One Cell
                  </text>
                  <text
                    x={400 + index * 166}
                    y="199"
                    textAnchor="middle"
                    className="svg-small"
                  >
                    one fenced writer
                  </text>
                </g>
              ))}
              <text x="484" y="276" textAnchor="middle" className="svg-copy">
                Same behavior. Separate state.
              </text>
              <text x="484" y="302" textAnchor="middle" className="svg-small">
                Select a Cell to inspect its target.
              </text>
            </svg>
          </div>
          <p className="lesson-scroll-hint">
            Swipe to see the full diagram, or select an order below.
          </p>
          <div
            className="lesson-choices identity-order-picker"
            aria-label="Choose an order"
          >
            {[42, 43].map((value) => (
              <button
                key={value}
                aria-pressed={order === value}
                onClick={() => setOrder(value)}
              >
                Order {value}
              </button>
            ))}
          </div>
        </div>
        <div
          className="identity-inspector"
          aria-live="polite"
          aria-atomic="true"
        >
          <span className="eyebrow">THE SELECTED TARGET</span>
          <h3>
            {tenant} / Order {order}
          </h3>
          <dl className="identity-fields">
            {[
              ["Tenant", tenant],
              ["Application", "Shop"],
              ["Namespace", "Orders"],
              ["Partition", `Entity ${order}`],
            ].map(([name, value]) => (
              <div key={name}>
                <dt>{name}</dt>
                <dd>{value}</dd>
              </div>
            ))}
          </dl>
          <p>
            Changing the tenant or partition selects a different Cell. Moving
            the owner to another node keeps this target’s identity.
          </p>
        </div>
      </div>
      <div className="boundary-lab">
        <div>
          <span className="eyebrow">WHERE DOES ONE TRANSACTION END?</span>
          <div className="lesson-choices" aria-label="Transaction boundary">
            <button
              aria-pressed={boundary === "together"}
              onClick={() => setBoundary("together")}
            >
              Order + line items
            </button>
            <button
              aria-pressed={boundary === "across"}
              onClick={() => setBoundary("across")}
            >
              Two independent orders
            </button>
          </div>
        </div>
        <div className="boundary-answer" aria-live="polite">
          <strong>
            {boundary === "together"
              ? "Keep related state in one Cell."
              : "Coordinate across two Cells."}
          </strong>
          <p>
            {boundary === "together"
              ? "An order and its line items can change in one transaction when your module stores them in the same Cell. The Cell boundary is broader than a single table."
              : "A command cannot make a single SQL transaction across independently addressed Cells. Record durable intent and deliver it through Effects and an idempotent destination inbox."}
          </p>
        </div>
      </div>
      <p className="lesson-caption">
        Illustrative tenant and partition labels. Cellule derives persisted
        identities from canonical target fields; this diagram does not generate
        real IDs.
      </p>
    </div>
  );
}
