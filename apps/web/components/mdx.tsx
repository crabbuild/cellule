import defaultMdxComponents from "fumadocs-ui/mdx";
import type { MDXComponents } from "mdx/types";
import { Mermaid } from "./mermaid";
import { SvgDiagram } from "./svg-diagram";
import {
  DurabilityExplorer,
  RecoveryExplorer,
  LayerExplorer,
  PrimitiveDiagram,
} from "./explorers";

export function getMDXComponents(components?: MDXComponents): MDXComponents {
  return {
    ...defaultMdxComponents,
    Mermaid,
    SvgDiagram,
    DurabilityExplorer,
    RecoveryExplorer,
    LayerExplorer,
    PrimitiveDiagram,
    ...components,
  };
}
export const useMDXComponents = getMDXComponents;
declare global {
  type MDXProvidedComponents = ReturnType<typeof getMDXComponents>;
}
