import { visit } from "unist-util-visit";

export default function remarkDiagrams() {
  return (tree) => {
    visit(tree, (node, index, parent) => {
      if (node.type === "code" && node.lang === "mermaid") {
        parent.children[index] = {
          type: "mdxJsxFlowElement",
          name: "Mermaid",
          attributes: [
            { type: "mdxJsxAttribute", name: "code", value: node.value },
          ],
          children: [],
        };
      }
      // Preserve explicit repository anchors when compiling ordinary Markdown.
      if (node.type === "html") {
        const anchor = node.value.trim().match(/^<a id="([^"]+)">(?:<\/a>)?$/);
        if (anchor)
          parent.children[index] = {
            type:
              parent.type === "root"
                ? "mdxJsxFlowElement"
                : "mdxJsxTextElement",
            name: "span",
            attributes: [
              { type: "mdxJsxAttribute", name: "id", value: anchor[1] },
            ],
            children: [],
          };
        else if (node.value.trim() === "</a>")
          parent.children[index] = { type: "text", value: "" };
      }
    });
  };
}
