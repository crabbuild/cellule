"use client";

import { useState } from "react";
import { Download, Minus, Plus, RotateCcw } from "lucide-react";

export function SvgDiagram({ src, alt }: { src: string; alt: string }) {
  const [zoom, setZoom] = useState(1);
  return (
    <figure className="mermaid-figure svg-file-figure" data-svg-diagram>
      <div className="diagram-toolbar">
        <span>SVG illustration</span>
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
          <a href={src} download aria-label="Download SVG">
            <Download size={16} />
          </a>
        </div>
      </div>
      <div className="mermaid-scroll" tabIndex={0} role="region" aria-label={alt}>
        <div
          className="svg-file-canvas"
          style={{ width: `${zoom * 100}%`, minWidth: `${zoom * 600}px` }}
        >
          <img src={src} alt={alt} loading="lazy" />
        </div>
      </div>
    </figure>
  );
}
