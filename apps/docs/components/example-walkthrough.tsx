"use client";
import { useId, useState } from "react";
import Link from "next/link";
import { ArrowLeft, ArrowRight, ArrowUpRight, RotateCcw } from "lucide-react";
import { walkthroughs } from "@/lib/walkthroughs";
import { useDiagramFocus } from "./use-diagram-focus";
import { CopyCommand } from "./copy-command";
export function ExampleWalkthrough() {
  const [example, setExample] = useState(0);
  const [step, setStep] = useState(0);
  const [edgeCase, setEdgeCase] = useState(false);
  const id = useId();
  const diagram = useDiagramFocus(`${example}:${step}`);
  const scenario = walkthroughs[example];
  const current = scenario.steps[step];
  const final = step === scenario.steps.length - 1;
  function selectExample(index: number) {
    setExample(index);
    setStep(0);
    setEdgeCase(false);
  }
  function selectStep(index: number) {
    setStep(index);
    setEdgeCase(false);
  }
  return (
    <div className="example-walkthrough">
      <div className="walkthrough-tabs" aria-label="Choose an example">
        {walkthroughs.map((item, index) => (
          <button
            key={item.id}
            aria-pressed={example === index}
            onClick={() => selectExample(index)}
          >
            <span className="mono">{item.category}</span>
            {item.name}
          </button>
        ))}
      </div>
      <div className="walkthrough-intro">
        <div>
          <span className="eyebrow">{scenario.category}</span>
          <h2>{scenario.title}</h2>
        </div>
        <p>{scenario.introduction}</p>
      </div>
      <div className="lesson-lab walkthrough-lab">
        <div className="lesson-lab-bar">
          <span className="mono">INTERACTIVE WALKTHROUGH</span>
          <span className="lesson-demo-label">
            Illustration · no Rust runs in your browser
          </span>
        </div>
        <div
          className="walkthrough-diagram lesson-diagram"
          ref={diagram}
          tabIndex={0}
          role="region"
          aria-label="Scrollable example diagram"
        >
          <svg
            viewBox={`0 0 ${scenario.steps.length * 160} 174`}
            className="lesson-svg"
            role="group"
            aria-labelledby={`${id}-title`}
          >
            <title id={`${id}-title`}>
              {`${scenario.name}: select a step from ${scenario.steps.map((item) => item.label).join(", ")}`}
            </title>
            {scenario.steps.map((item, index) => (
              <g key={item.label}>
                <g
                  role="button"
                  tabIndex={0}
                  className={`lesson-hex-button${step === index ? " is-active" : ""}${step > index ? " is-complete" : ""}`}
                  aria-label={`Inspect ${item.label} step`}
                  aria-pressed={step === index}
                  onClick={() => selectStep(index)}
                  onKeyDown={(event) => {
                    if (event.key === "Enter" || event.key === " ") {
                      event.preventDefault();
                      selectStep(index);
                    }
                  }}
                >
                  <path
                    d={`M${80 + index * 160} 14l58 33v67l-58 33-58-33V47Z`}
                  />
                  <text
                    x={80 + index * 160}
                    y="64"
                    textAnchor="middle"
                    className="svg-small"
                  >
                    {String(index + 1).padStart(2, "0")}
                  </text>
                  <text
                    x={80 + index * 160}
                    y="93"
                    textAnchor="middle"
                    className="svg-title"
                  >
                    {item.label}
                  </text>
                </g>
                {index < scenario.steps.length - 1 ? (
                  <path
                    d={`M${142 + index * 160} 81h34m-7-6 7 6-7 6`}
                    className="lesson-connector"
                  />
                ) : null}
              </g>
            ))}
          </svg>
        </div>
        <div
          className="walkthrough-step-picker lesson-choices"
          aria-label="Walkthrough steps"
        >
          {scenario.steps.map((item, index) => (
            <button
              key={item.label}
              aria-pressed={step === index}
              onClick={() => selectStep(index)}
            >
              {index + 1}. {item.label}
            </button>
          ))}
        </div>
        <div className="walkthrough-detail">
          <div
            className="walkthrough-explanation"
            aria-live="polite"
            aria-atomic="true"
          >
            <span className="eyebrow">
              STEP {step + 1} OF {scenario.steps.length}
            </span>
            <h3>{current.title}</h3>
            <p>{current.description}</p>
            <div className="walkthrough-observation">
              <span className="mono">WHAT TO NOTICE</span>
              <p>{current.observation}</p>
            </div>
          </div>
          <div
            className="walkthrough-state"
            aria-live="polite"
            aria-atomic="true"
          >
            <span className="mono">ILLUSTRATED STATE</span>
            <div className="walkthrough-state-light">
              <span />
              {current.state}
            </div>
            <strong>{current.detail}</strong>
            <p>{scenario.invariant}</p>
            {final && (scenario.id === "sql" || scenario.id === "schedules") ? (
              <div className="walkthrough-edge">
                <button
                  className="button secondary"
                  aria-pressed={edgeCase}
                  onClick={() => setEdgeCase((value) => !value)}
                >
                  {scenario.id === "sql"
                    ? "What if the reply is lost?"
                    : "What if delivery repeats?"}
                </button>
                {edgeCase ? (
                  <p role="status">
                    {scenario.id === "sql"
                      ? "The durable outcome is still recorded. Keep the original request identity and resolve its outcome before retrying; an uncertain reply is not permission to submit a new logical command."
                      : "Reminder rows: 1. In this illustration, the repeated occurrence is applied idempotently. Your destination must preserve that contract; delivery is not a single cross-Cell transaction."}
                  </p>
                ) : null}
              </div>
            ) : null}
          </div>
        </div>
        <div className="walkthrough-bottom">
          <span>
            {final
              ? "You have reached the end of this path."
              : "Advance the diagram at your own pace."}
          </span>
          <div className="lesson-step-actions">
            <button
              className="button secondary"
              disabled={step === 0}
              aria-label="Previous example step"
              onClick={() => selectStep(step - 1)}
            >
              <ArrowLeft size={16} />
            </button>
            <button
              className="button primary"
              onClick={() => selectStep(final ? 0 : step + 1)}
            >
              {final ? "Replay example" : "Next example step"}
              {final ? <RotateCcw size={16} /> : <ArrowRight size={16} />}
            </button>
          </div>
        </div>
      </div>
      <div className="walkthrough-run">
        <div>
          <span className="eyebrow">NOW RUN THE REAL EXAMPLE</span>
          <h3>From the diagram to your terminal.</h3>
          <p>
            Run from the repository root with Rust 1.97+. The examples use
            temporary SQLite files and in-memory object storage.
          </p>
          <div className="walkthrough-source-links">
            <a
              href={`https://github.com/crabbuild/cellule/blob/main/crates/cellule-app/examples/${scenario.id}.rs`}
            >
              Read {scenario.id}.rs <ArrowUpRight size={15} />
            </a>
            <Link href={scenario.guide}>
              Read the capability guide <ArrowUpRight size={15} />
            </Link>
          </div>
        </div>
        <div className="terminal">
          <div className="terminal-header">
            <span>{scenario.id}.rs</span>
            <span>RUN LOCALLY</span>
          </div>
          <CopyCommand
            key={scenario.id}
            command={`cargo run -p cellule-app --example ${scenario.id} --locked`}
          />
          <div className="terminal-result">
            <span className="mono">EXPECTED OUTPUT FROM THE RUST EXAMPLE</span>
            <code>{scenario.output}</code>
          </div>
        </div>
      </div>
    </div>
  );
}
