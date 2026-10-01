import type { BaseLayoutProps } from "fumadocs-ui/layouts/shared";
import { CelluleLockup } from "@/components/brand";

export function baseOptions(): BaseLayoutProps {
  return {
    nav: { title: <CelluleLockup /> },
    links: [
      { text: "Website", url: "/" },
      { text: "Architecture", url: "/architecture" },
      {
        text: "GitHub",
        url: "https://github.com/crabbuild/cellule",
        external: true,
      },
    ],
  };
}
