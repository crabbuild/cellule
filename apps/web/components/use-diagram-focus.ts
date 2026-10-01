"use client";
import { useEffect, useRef } from "react";

export function useDiagramFocus(selection: string) {
  const diagram = useRef<HTMLDivElement>(null);
  useEffect(() => {
    const viewport = diagram.current;
    const active = viewport?.querySelector('[aria-pressed="true"]');
    if (!viewport || !active) return;
    const box = active.getBoundingClientRect();
    const frame = viewport.getBoundingClientRect();
    viewport.scrollTo({
      left:
        viewport.scrollLeft +
        box.left -
        frame.left -
        (viewport.clientWidth - box.width) / 2,
      behavior: "instant",
    });
  }, [selection]);
  return diagram;
}
