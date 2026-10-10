import type { Metadata } from "next";

export const site = {
  name: "Cellule",
  url: process.env.NEXT_PUBLIC_SITE_URL || "https://cellule.crab.build",
  github: "https://github.com/crabbuild/cellule",
  description:
    "Build Rust applications with durable data, background jobs, and workflows. Eight capabilities, one foundation of independent SQLite-backed Cells.",
};
export function pageMetadata(
  title: string,
  description: string,
  path: string,
): Metadata {
  return {
    title,
    description,
    alternates: { canonical: path },
    openGraph: {
      title: `${title} · Cellule`,
      description,
      url: new URL(path, site.url).href,
      type: "website",
    },
  };
}
