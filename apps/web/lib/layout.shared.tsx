import type { BaseLayoutProps } from "fumadocs-ui/layouts/shared";
import { Github, House, Network } from "lucide-react";
import { CelluleLockup } from "@/components/brand";

export function baseOptions(): BaseLayoutProps {
  return {
    nav: { title: <CelluleLockup /> },
    links: [
      { text: "Website", url: "/", icon: <House aria-hidden="true" /> },
      {
        text: "Architecture",
        url: "/architecture",
        icon: <Network aria-hidden="true" />,
      },
      {
        text: "GitHub",
        url: "https://github.com/crabbuild/cellule",
        external: true,
        icon: <Github aria-hidden="true" />,
      },
    ],
  };
}
