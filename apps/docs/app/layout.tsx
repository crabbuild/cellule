import type { Metadata } from "next";
import type { ReactNode } from "react";
import { RootProvider } from "fumadocs-ui/provider/next";
import { site } from "@/lib/site";
import "@fontsource/ibm-plex-sans/400.css";
import "@fontsource/ibm-plex-sans/500.css";
import "@fontsource/ibm-plex-sans/600.css";
import "@fontsource/ibm-plex-mono/400.css";
import "@fontsource/sora/500.css";
import "@fontsource/sora/600.css";
import "./globals.css";
import "./learning.css";

export const metadata: Metadata = {
  metadataBase: new URL(site.url),
  title: {
    default: "Cellule · Durable state inside your application",
    template: "%s · Cellule",
  },
  description: site.description,
  icons: { icon: "/icon.svg" },
  openGraph: {
    title: "Cellule",
    description: site.description,
    type: "website",
    siteName: "Cellule",
  },
};
export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body>
        <RootProvider theme={{ defaultTheme: "light" }}>
          <a className="skip-link" href="#main-content">
            Skip to content
          </a>
          {children}
        </RootProvider>
      </body>
    </html>
  );
}
