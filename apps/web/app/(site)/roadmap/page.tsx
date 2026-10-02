import { PageIntro } from "@/components/page-intro";
import Link from "next/link";
import { pageMetadata, site } from "@/lib/site";
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
      <article className="page-width doc-article">
        <div className="prose max-w-none">
          <h2>Build on the implemented foundation</h2>
          <p>
            Eight typed primitives share fenced ownership, durable request
            outcomes, receipt-bound reads, and verified recovery. Start with the
            <Link href="/docs/guides/quickstart"> local quickstart</Link>, then
            follow the <Link href="/docs/guides/framework">integration guide</Link>{" "}
            to bring Cellule into your service.
          </p>
          <h2>Qualify the environment you plan to run</h2>
          <p>
            Broader adoption depends on measured provider behavior, constrained
            fleets, failure and rollout paths, and upgrade compatibility. Keep
            evidence tied to the source revision, image, and qualification
            profile. The <Link href="/performance">recorded reports</Link> and
            <Link href="/docs/crates/runtime/docs/delivery"> evidence guide</Link>{" "}
            explain what each level proves.
          </p>
          <p>
            Read the <a href={`${site.github}/blob/main/docs/roadmap.md`}>full
            framework roadmap on GitHub</a> for implementation scope and remaining
            qualification work. It is a planning map, with no promised release
            dates.
          </p>
        </div>
      </article>
    </>
  );
}
