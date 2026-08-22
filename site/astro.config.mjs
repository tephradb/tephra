// @ts-check
import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";

// The site is a fully static build. No client framework, no runtime data fetching, no
// analytics. Search is Pagefind (bundled with Starlight, indexed at build time); Rust
// highlighting is Expressive Code / Shiki (bundled). Dark and light themes with a persisted
// toggle are Starlight defaults.
export default defineConfig({
  site: "https://tephra.tqwewe.com",
  trailingSlash: "always",
  integrations: [
    starlight({
      title: "Tephra",
      description:
        "An immutable event store with global ordering, built for the Dynamic Consistency Boundary.",
      favicon: "/favicon.ico",
      logo: {
        src: "./src/assets/tephra-mark.png",
        alt: "Tephra",
      },
      head: [
        {
          tag: "link",
          attrs: { rel: "apple-touch-icon", sizes: "180x180", href: "/apple-touch-icon.png" },
        },
        {
          tag: "meta",
          attrs: { property: "og:image", content: "https://tephra.tqwewe.com/og.png" },
        },
        { tag: "meta", attrs: { property: "og:image:width", content: "1200" } },
        { tag: "meta", attrs: { property: "og:image:height", content: "630" } },
        { tag: "meta", attrs: { name: "twitter:card", content: "summary_large_image" } },
        {
          tag: "meta",
          attrs: { name: "twitter:image", content: "https://tephra.tqwewe.com/og.png" },
        },
      ],
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/tephradb/tephra",
        },
      ],
      customCss: ["./src/styles/custom.css"],
      // A single accent hue (a warm volcanic tone) is set in custom.css.
      sidebar: [
        { label: "Introduction", slug: "introduction" },
        { label: "Getting started", slug: "getting-started" },
        {
          label: "Clients",
          items: [
            { label: "Overview", slug: "clients" },
            { label: "Go", slug: "clients/go" },
            { label: "JavaScript", slug: "clients/javascript" },
            { label: "Rust", slug: "clients/rust" },
          ],
        },
        { label: "Embedded", slug: "embedded" },
        { label: "Core concepts", slug: "core-concepts" },
        {
          label: "Guides",
          items: [
            { label: "Decision models", slug: "guides/decision-models" },
            { label: "The uniqueness guard", slug: "guides/uniqueness-guard" },
            { label: "Subscriptions", slug: "guides/subscriptions" },
            { label: "Handling conflicts", slug: "guides/conflicts" },
            { label: "Idempotency", slug: "guides/idempotency" },
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
