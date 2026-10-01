"use client";
import { useState } from "react";
import { Check, Copy } from "lucide-react";

export function CopyCommand({ command }: { command: string }) {
  const [status, setStatus] = useState<"idle" | "copied" | "failed">("idle");
  async function copy() {
    try {
      await navigator.clipboard.writeText(command);
      setStatus("copied");
    } catch {
      setStatus("failed");
    }
  }
  return (
    <div className="copy-command">
      <span className="command-prompt" aria-hidden="true">
        $
      </span>
      <code>{command}</code>
      <button aria-label="Copy command" onClick={copy}>
        {status === "copied" ? <Check size={17} /> : <Copy size={17} />}
      </button>
      <span className="sr-only" role="status">
        {status === "copied"
          ? "Command copied"
          : status === "failed"
            ? "Could not copy. Select the command to copy it manually."
            : ""}
      </span>
    </div>
  );
}
