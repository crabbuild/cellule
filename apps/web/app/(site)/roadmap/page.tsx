import { PageIntro } from "@/components/page-intro";
import { DocArticle } from "@/components/doc-article";
import { pageMetadata } from "@/lib/site";
export const metadata = pageMetadata(
  "Roadmap",
  "Implemented framework contracts and the evidence still needed before broader adoption.",
  "/roadmap",
);
export default function Roadmap() {
  return (
    <>
      <PageIntro
        eyebrow="FRAMEWORK SCOPE"
        title="Implemented contracts. Measured next steps."
      >
        <p>
          A planning map for Cellule: what the framework supplies, what your
          application integrates, and what needs qualification.
        </p>
      </PageIntro>
      <DocArticle slug={["guides", "roadmap"]} />
    </>
  );
}
