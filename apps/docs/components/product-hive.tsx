"use client";

import { useId, useState } from "react";
import Link from "next/link";
import { ArrowUpRight, MousePointer2 } from "lucide-react";

const capabilities = [
  {
    name: "Data",
    subtitle: "SQL + KV",
    x: 226,
    y: 122,
    title: "Give every record a reliable home.",
    description:
      "Keep related data together, make changes atomically, and read back the state your application committed.",
    href: "/docs/primitives/sql",
    link: "Explore durable data",
  },
  {
    name: "Files",
    subtitle: "Blob",
    x: 374,
    y: 122,
    title: "Bring content into the picture.",
    description:
      "Connect documents and uploads to durable references, with an explicit moment when completed content becomes visible.",
    href: "/docs/primitives/blob",
    link: "Explore content storage",
  },
  {
    name: "Jobs",
    subtitle: "Queue",
    x: 448,
    y: 250,
    title: "Keep useful work moving.",
    description:
      "Give background jobs a durable place to wait, with leased claims and acknowledgements your workers can act on.",
    href: "/docs/primitives/queue",
    link: "Explore background work",
  },
  {
    name: "Journeys",
    subtitle: "Workflow + Activities",
    x: 374,
    y: 378,
    title: "Remember the journey, step by step.",
    description:
      "Record long-running decisions and coordinate supervised external work, so a workflow has history it can return to.",
    href: "/docs/primitives/workflow",
    link: "Explore workflows",
  },
  {
    name: "Schedules",
    subtitle: "Cron",
    x: 226,
    y: 378,
    title: "Make recurring work part of the plan.",
    description:
      "Keep schedule state durable and turn due occurrences into work your application can deliver and track.",
    href: "/docs/primitives/cron",
    link: "Explore recurring work",
  },
  {
    name: "Connections",
    subtitle: "Effects",
    x: 152,
    y: 250,
    title: "Let independent parts work together.",
    description:
      "Record delivery intent in one Cell and apply it idempotently in another, connecting work across clear boundaries.",
    href: "/docs/primitives/effects",
    link: "Explore Cell-to-Cell delivery",
  },
];

export function ProductHive() {
  const [selected, setSelected] = useState(0);
  const id = useId();
  const active = capabilities[selected];
  return (
    <div className="product-hive">
      <div className="hive-caption">
        <span className="mono">A SMALL CELL. A WORLD OF POSSIBILITY.</span>
        <span className="hive-instruction">
          Select a cell <MousePointer2 size={15} aria-hidden="true" />
        </span>
      </div>
      <svg
        viewBox="0 0 600 488"
        className="hive-svg"
        role="group"
        aria-labelledby={`${id}-title`}
      >
        <title id={`${id}-title`}>
          Explore six ways to build with Cellule. Select a honeycomb cell.
        </title>
        <g aria-hidden="true" className="hive-orbits">
          <path d="M300 12 506 131v238L300 488 94 369V131Z" />
          <path d="M300 44 478 147v206L300 456 122 353V147Z" />
        </g>
        {capabilities.map((item, index) => (
          <g
            key={item.name}
            className={`hive-cell${selected === index ? " is-selected" : ""}`}
            role="button"
            tabIndex={0}
            aria-label={`Explore ${item.name}`}
            aria-pressed={selected === index}
            onClick={() => setSelected(index)}
            onKeyDown={(event) => {
              if (event.key === "Enter" || event.key === " ") {
                event.preventDefault();
                setSelected(index);
              }
            }}
          >
            <path d={`M${item.x} ${item.y - 80}l69 40v80l-69 40-69-40v-80Z`} />
            <text
              x={item.x}
              y={item.y - 2}
              className="hive-cell-name"
              textAnchor="middle"
            >
              {item.name}
            </text>
            <text
              x={item.x}
              y={item.y + 21}
              className="hive-cell-subtitle"
              textAnchor="middle"
            >
              {item.subtitle}
            </text>
          </g>
        ))}
        <g aria-hidden="true" className="hive-center">
          <path d="M300 170l69 40v80l-69 40-69-40v-80Z" />
          <g transform="translate(278 192) scale(.69)">
            <path d="M32 3 45 10.5v15L32 33 19 25.5v-15Z" fill="#ffd56a" />
            <path d="M18 29 31 36.5v15L18 59 5 51.5v-15Z" fill="#e8a839" />
            <path d="M46 29 59 36.5v15L46 59 33 51.5v-15Z" fill="#f5be4f" />
          </g>
          <text x="300" y="268" textAnchor="middle">
            cellule.
          </text>
          <text
            x="300"
            y="289"
            textAnchor="middle"
            className="hive-center-note"
          >
            ONE FOUNDATION
          </text>
        </g>
      </svg>
      <div className="hive-description" aria-live="polite" aria-atomic="true">
        <div>
          <span className="hive-selection">{active.name}</span>
          <h2>{active.title}</h2>
        </div>
        <p>{active.description}</p>
        <Link href={active.href}>
          {active.link}
          <ArrowUpRight size={15} />
        </Link>
      </div>
    </div>
  );
}
