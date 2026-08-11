# Tephra site

The Tephra landing page and documentation, one Astro + Starlight project, built to a fully static
site.

## Develop

```sh
npm install
npm run dev      # local dev server
npm run build    # static build into dist/
npm run preview  # serve the built site
```

## Code examples

Every code sample on the site is a compiled, tested example in the `tephra-site-examples` crate
(a member of the workspace at the repository root). Pages import the exact source between
`// ANCHOR: name` and `// ANCHOR_END: name` markers, so a signature change breaks the build rather
than rotting the docs.

```sh
cargo test -p tephra-site-examples   # run from the repository root
```

## Deploy

`.github/workflows/site.yml` compiles and runs the examples, builds the site, and deploys to
GitHub Pages on push to `main`. The `base` in `astro.config.mjs` is `/tephra/` for a project-page
deploy; change it if the site moves to a domain root.

## Progress and decisions

See `PROGRESS.md` for the server-first decision, the engine changes made to support the docs, the
benchmark treatment, and the outstanding benchmark rerun.
