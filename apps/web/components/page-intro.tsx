import type { ReactNode } from "react";
export function PageIntro({
  eyebrow,
  title,
  children,
}: {
  eyebrow: string;
  title: string;
  children: ReactNode;
}) {
  return (
    <div className="page-intro page-width">
      <span className="eyebrow">{eyebrow}</span>
      <h1>{title}</h1>
      <div className="intro-description">{children}</div>
    </div>
  );
}
