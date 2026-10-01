import { createMDX } from "fumadocs-mdx/next";

export default createMDX()({
  reactStrictMode: true,
  devIndicators: false,
  async redirects() {
    return [{ source: "/docs/README", destination: "/docs", permanent: true }];
  },
});
