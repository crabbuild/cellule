"use client";

import { useEffect, useId, useRef, useState } from "react";
import { Download, Minus, Plus, RotateCcw } from "lucide-react";

// Mermaid shares renderer state and temporary DOM nodes across diagrams.
let renderQueue: Promise<unknown> = Promise.resolve();
export function Mermaid({ code }: { code: string }) {
  const id = `diagram-${useId().replace(/[^a-zA-Z0-9]/g, "")}`;
  const root = useRef<HTMLDivElement>(null);
  const [svg, setSvg] = useState("");
  const [naturalWidth, setNaturalWidth] = useState<number>();
  const [error, setError] = useState("");
  const [zoom, setZoom] = useState(1);
  useEffect(() => {
    let cancelled = false;
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (!entry.isIntersecting) return;
        observer.disconnect();
        const render = async () => {
          if (cancelled) return;
          try {
            const { default: mermaid } = await import("mermaid");
            mermaid.initialize({
              startOnLoad: false,
              securityLevel: "strict",
              theme: "base",
              fontFamily: "IBM Plex Sans, sans-serif",
              themeVariables: {
                primaryColor: "#fbf3dc",
                primaryTextColor: "#142944",
                primaryBorderColor: "#b87313",
                lineColor: "#64758d",
                secondaryColor: "#e9f5f2",
                tertiaryColor: "#fffdf7",
              },
            });
            const result = await mermaid.render(id, code);
            if (!cancelled) {
              const renderedSVG = new DOMParser().parseFromString(
                result.svg,
                "image/svg+xml",
              ).documentElement;
              const width = Number(
                renderedSVG.getAttribute("viewBox")?.trim().split(/[\s,]+/)[2],
              );
              setNaturalWidth(
                Number.isFinite(width) && width > 0 ? width : undefined,
              );
              setSvg(result.svg);
            }
          } catch (cause) {
            if (!cancelled)
              setError(
                cause instanceof Error
                  ? cause.message
                  : "Unable to render diagram",
              );
          }
        };
        renderQueue = renderQueue.then(render, render);
      },
      { rootMargin: "300px" },
    );
    if (root.current) observer.observe(root.current);
    return () => {
      cancelled = true;
      observer.disconnect();
    };
  }, [code, id]);
  function download() {
    const url = URL.createObjectURL(new Blob([svg], { type: "image/svg+xml" }));
    const link = document.createElement("a");
    link.href = url;
    link.download = "cellule-diagram.svg";
    link.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }
  return (
    <figure className="mermaid-figure" ref={root} data-diagram>
      <div className="diagram-toolbar">
        <span>SVG diagram</span>
        <div>
          <button
            aria-label="Zoom out"
            disabled={zoom <= 0.5}
            onClick={() => setZoom(Math.max(0.5, zoom - 0.25))}
          >
            <Minus size={16} />
          </button>
          <button aria-label="Reset diagram zoom" onClick={() => setZoom(1)}>
            <RotateCcw size={15} />
          </button>
          <button
            aria-label="Zoom in"
            disabled={zoom >= 3}
            onClick={() => setZoom(Math.min(3, zoom + 0.25))}
          >
            <Plus size={16} />
          </button>
          <button aria-label="Download SVG" disabled={!svg} onClick={download}>
            <Download size={16} />
          </button>
        </div>
      </div>
      <div
        className="mermaid-scroll"
        tabIndex={0}
        aria-label="Scrollable diagram"
      >
        {svg ? (
          <div
            className="mermaid-svg"
            style={{
              width: `${zoom * 100}%`,
              minWidth: `${zoom * 360}px`,
              maxWidth: naturalWidth ? `${zoom * naturalWidth}px` : undefined,
            }}
            dangerouslySetInnerHTML={{ __html: svg }}
          />
        ) : (
          <pre className="diagram-source">{code}</pre>
        )}
      </div>
      {error && (
        <figcaption role="alert" className="diagram-error">
          {error}
        </figcaption>
      )}
    </figure>
  );
}
