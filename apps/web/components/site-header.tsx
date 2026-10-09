"use client";

import { useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { useTheme } from "next-themes";
import { useSearchContext } from "fumadocs-ui/contexts/search";
import { Menu, X, Search, SunMoon, Github } from "lucide-react";
import { CelluleLockup } from "./brand";

const documentationLinks = [
  ["Docs", "/docs"],
  ["Architecture", "/architecture"],
  ["Learn", "/concepts"],
  ["Primitives", "/primitives"],
  ["Examples", "/examples"],
] as const;
const productLinks = [
  ["Why Cellule", "/#why-cellule"],
  ["Capabilities", "/#capabilities"],
  ["Use cases", "/#use-cases"],
  ["Learn", "/concepts"],
  ["Docs", "/docs"],
] as const;
export function SiteHeader() {
  const pathname = usePathname();
  const links = pathname === "/" ? productLinks : documentationLinks;
  const [open, setOpen] = useState(false);
  const { resolvedTheme, setTheme } = useTheme();
  const { setOpenSearch } = useSearchContext();
  return (
    <header className="site-header">
      <div className="header-inner">
        <Link href="/" aria-label="Cellule home">
          <CelluleLockup />
        </Link>
        <nav
          aria-label="Main navigation"
          className={open ? "main-nav is-open" : "main-nav"}
        >
          {links.map(([name, href]) => (
            <Link
              key={href}
              href={href}
              aria-current={
                (
                  href === "/concepts"
                    ? ["/concepts", "/topology", "/walkthroughs"].includes(
                        pathname,
                      )
                    : pathname.startsWith(href)
                )
                  ? "page"
                  : undefined
              }
              onClick={() => setOpen(false)}
            >
              {name}
            </Link>
          ))}
        </nav>
        <div className="header-actions">
          <button
            className="search-button"
            aria-label="Search documentation"
            onClick={() => setOpenSearch(true)}
          >
            <Search size={16} />
            <span>Search docs</span>
            <kbd>⌘ K</kbd>
          </button>
          <button
            className="icon-button"
            aria-label="Toggle color theme"
            onClick={() =>
              setTheme(resolvedTheme === "dark" ? "light" : "dark")
            }
          >
            <SunMoon size={18} />
          </button>
          <a
            className="icon-button github-header"
            href="https://github.com/crabbuild/cellule"
            aria-label="Cellule on GitHub"
          >
            <Github size={19} />
          </a>
          <button
            className="icon-button menu-button"
            aria-label={open ? "Close navigation" : "Open navigation"}
            aria-expanded={open}
            onClick={() => setOpen(!open)}
          >
            {open ? <X /> : <Menu />}
          </button>
        </div>
      </div>
    </header>
  );
}
