import { PageIntro } from "@/components/page-intro";
import { DocArticle } from "@/components/doc-article";
import { pageMetadata } from "@/lib/site";
export const metadata = pageMetadata(
  "Changelog",
  "User-facing changes to the matched Cellule crate set.",
  "/changelog",
);
export default function Changelog() {
  return (
    <>
      <PageIntro
        eyebrow="PROJECT HISTORY"
        title="Changes across the crate set."
      >
        <p>
          Release notes for Cellule’s matched framework crates, published from
          the repository changelog.
        </p>
      </PageIntro>
      <DocArticle slug={["changelog"]} />
    </>
  );
}
