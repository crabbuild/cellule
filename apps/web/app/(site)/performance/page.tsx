import Link from "next/link";
import { PageIntro } from "@/components/page-intro";
import { pageMetadata } from "@/lib/site";
import manifest from "@/lib/docs-manifest.json";

export const metadata = pageMetadata(
  "Performance and evidence",
  "Dated benchmark reports, verification notes, and qualification profiles for Cellule.",
  "/performance",
);
export default function Performance() {
  const reports = manifest.pages
    .filter(
      (page) =>
        page.sourcePath.startsWith("crates/cellule-app/performance/") ||
        page.sourcePath.startsWith("crates/cellule-ltx/perf/history/"),
    )
    .sort((a, b) => b.sourcePath.localeCompare(a.sourcePath));
  return (
    <>
      <PageIntro
        eyebrow="INSPECT THE EVIDENCE"
        title="A result belongs to its environment."
      >
        <p>
          Read the measured workload, revision, provider, and limits behind each
          result. Local success establishes a local path; production
          qualification requires separate evidence.
        </p>
      </PageIntro>
      <div className="page-width evidence-page">
        <div className="evidence-guides">
          <Link href="/docs/crates/app/performance">
            <span className="eyebrow">MEASUREMENT</span>
            <h2>Performance guide</h2>
            <p>
              Workload definitions and measurement routes for application and
              host behavior.
            </p>
          </Link>
          <Link href="/docs/crates/runtime/docs/delivery">
            <span className="eyebrow">QUALIFICATION</span>
            <h2>What each level proves</h2>
            <p>
              Evidence requirements for unit, integration, provider, fault, and
              production runs.
            </p>
          </Link>
          <Link href="/docs/guides/verification">
            <span className="eyebrow">VERIFICATION</span>
            <h2>Recorded workspace checks</h2>
            <p>The dated verification report and its source revision.</p>
          </Link>
        </div>
        <div className="section-heading">
          <h2>Recorded reports</h2>
          <span className="mono">{reports.length} REPORTS</span>
        </div>
        <div className="report-list">
          {reports.map((report) => (
            <Link href={report.url} key={report.url}>
              <div>
                <span className="mono">
                  {report.sourcePath.match(/\d{4}-\d{2}(?:-\d{2})?/)?.[0]}
                </span>
                <h3>{report.title}</h3>
                <p>{report.description}</p>
              </div>
              <span className="report-action">Read report ↗</span>
            </Link>
          ))}
        </div>
        <div className="callout">
          <h3>Keep the raw evidence.</h3>
          <p>
            Qualification binds raw logs and receipts to the source, image, and
            profile digest. Do not weaken a qualification profile or its
            expected evidence to silence a failure.
          </p>
          <Link href="/docs/crates/runtime/qualification">
            Read the qualification profiles
          </Link>
        </div>
      </div>
    </>
  );
}
