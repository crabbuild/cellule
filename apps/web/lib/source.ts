import { docs } from "collections/server";
import { loader } from "fumadocs-core/source";
import { createElement } from "react";
import {
  BookOpen,
  Boxes,
  Compass,
  GitPullRequest,
  Hexagon,
  History,
  Lightbulb,
  type LucideIcon,
} from "lucide-react";

const sectionIcons: Record<string, LucideIcon> = {
  guides: Compass,
  concepts: Lightbulb,
  primitives: Hexagon,
  crates: Boxes,
};

const pageIcons: Record<string, LucideIcon> = {
  "/docs": BookOpen,
  "/docs/contributing": GitPullRequest,
  "/docs/changelog": History,
};

function navigationIcon(Icon: LucideIcon) {
  return createElement(Icon, { "aria-hidden": true });
}

export const source = loader({
  baseUrl: "/docs",
  source: docs.toFumadocsSource(),
  pageTree: {
    transformers: [
      {
        file(node) {
          const Icon = pageIcons[node.url];
          return Icon ? { ...node, icon: navigationIcon(Icon) } : node;
        },
        folder(node, path) {
          const Icon = sectionIcons[path];
          return Icon ? { ...node, icon: navigationIcon(Icon) } : node;
        },
      },
    ],
  },
});
