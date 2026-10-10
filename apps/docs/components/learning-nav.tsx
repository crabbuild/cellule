import Link from "next/link";
import { ArrowUpRight } from "lucide-react";
import { learningPages } from "@/lib/learning";

export function LearningNav({ current }: { current: string }) {
  return (
    <nav className="learning-nav page-width" aria-label="Learning pages">
      {learningPages.map((page) => (
        <Link
          key={page.href}
          href={page.href}
          aria-current={current === page.href ? "page" : undefined}
        >
          <span>{page.number}</span>
          {page.label}
        </Link>
      ))}
    </nav>
  );
}
export function LearningLinks() {
  return (
    <div className="learning-links">
      {learningPages.map((page) => (
        <Link key={page.href} href={page.href}>
          <span className="eyebrow">{page.eyebrow}</span>
          <h3>
            {page.label}
            <ArrowUpRight size={20} />
          </h3>
          <p>{page.description}</p>
        </Link>
      ))}
    </div>
  );
}
