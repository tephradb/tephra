// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

// The site is a fully static build. No client framework, no runtime data fetching, no
// analytics. Search is Pagefind (bundled with Starlight, indexed at build time); Rust
// highlighting is Expressive Code / Shiki (bundled). Dark and light themes with a persisted
// toggle are Starlight defaults.
export default defineConfig({
  site: "https://tqwewe.github.io/tephra/",
  base: "/tephra/",
  trailingSlash: "always",
  integrations: [
    starlight({
      title: "Tephra",
      description:
        "An immutable event store with global ordering, built for the Dynamic Consistency Boundary.",
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/tqwewe/tephra",
        },
      ],
      customCss: ["./src/styles/custom.css"],
      // A single accent hue (a warm volcanic tone) is set in custom.css.
      sidebar: [
        { label: "Introduction", slug: "introduction" },
        { label: "Getting started", slug: "getting-started" },
        { label: "Embedded", slug: "embedded" },
        { label: "Core concepts", slug: "core-concepts" },
        {
          label: "Guides",
          items: [
            { label: "Decision models", slug: "guides/decision-models" },
            { label: "The uniqueness guard", slug: "guides/uniqueness-guard" },
            { label: "Subscriptions", slug: "guides/subscriptions" },
            { label: "Handling conflicts", slug: "guides/conflicts" },
          ],
        },
        { label: "Operations", slug: "operations" },
        { label: "Architecture", slug: "architecture" },
        { label: "Comparison", slug: "comparison" },
        { label: "Status", slug: "status" },
      ],
      lastUpdated: false,
      pagination: true,
    }),
  ],
});
