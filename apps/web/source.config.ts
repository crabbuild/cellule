import { defineConfig, defineDocs } from "fumadocs-mdx/config";
import { pageSchema } from "fumadocs-core/source/schema";
import { z } from "zod";
import remarkDiagrams from "./lib/remark-diagrams.mjs";

export const docs = defineDocs({
  dir: "content/docs",
  docs: {
    schema: pageSchema.extend({ sourcePath: z.string().optional() }),
    postprocess: {
      includeProcessedMarkdown: {
        filterElement: (node) => (node.type === "mdxjsEsm" ? false : true),
        stringify: (node) => {
          if (
            (node.type === "mdxJsxFlowElement" ||
              node.type === "mdxJsxTextElement") &&
            node.name === "Mermaid"
          ) {
            const code = node.attributes.find(
              (attribute) =>
                attribute.type === "mdxJsxAttribute" &&
                attribute.name === "code",
            );
            if (
              code?.type === "mdxJsxAttribute" &&
              typeof code.value === "string"
            )
              return `\n\n\`\`\`mermaid\n${code.value}\n\`\`\`\n\n`;
          }
        },
      },
    },
  },
});
export default defineConfig({
  mdxOptions: { remarkPlugins: [remarkDiagrams] },
});
