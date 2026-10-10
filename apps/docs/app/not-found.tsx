import Link from "next/link";
import { CelluleLockup } from "@/components/brand";
export default function NotFound() {
  return (
    <main id="main-content" className="not-found">
      <CelluleLockup />
      <span className="eyebrow">404</span>
      <h1>This path has no Cell.</h1>
      <p>The page may have moved. Find a guide in the documentation.</p>
      <div className="actions">
        <Link href="/docs" className="button primary">
          Open documentation
        </Link>
        <Link href="/" className="button secondary">
          Go home
        </Link>
      </div>
    </main>
  );
}
