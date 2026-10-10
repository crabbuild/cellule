export function CelluleMark({ className = "" }: { className?: string }) {
  return (
    <svg
      className={className}
      width="34"
      height="34"
      viewBox="0 0 64 64"
      fill="none"
      aria-hidden="true"
    >
      <path d="M32 3 45 10.5v15L32 33 19 25.5v-15Z" fill="var(--honey)" />
      <path d="M18 29 31 36.5v15L18 59 5 51.5v-15Z" fill="var(--honey-deep)" />
      <path d="M46 29 59 36.5v15L46 59 33 51.5v-15Z" fill="var(--honey-warm)" />
    </svg>
  );
}
export function CelluleLockup() {
  return (
    <span className="brand">
      <CelluleMark />
      <span>
        cellule<span className="brand-period">.</span>
      </span>
    </span>
  );
}
