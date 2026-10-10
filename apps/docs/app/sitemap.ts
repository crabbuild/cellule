import type { MetadataRoute } from "next";
import { source } from "@/lib/source";
import { site } from "@/lib/site";

export default function sitemap(): MetadataRoute.Sitemap {
  return [
    "/",
    "/architecture",
    "/concepts",
    "/topology",
    "/walkthroughs",
    "/primitives",
    "/examples",
    "/performance",
    "/roadmap",
    "/changelog",
    ...source.getPages().map((page) => page.url),
  ].map((url) => ({ url: new URL(url, site.url).href }));
}
