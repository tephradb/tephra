# Task: build the Tephra website and documentation site

Build the public site for **Tephra**, a DCB-compliant, immutable event store with global
ordering, written in Rust. The site is both the landing page and the docs. One codebase, one
deploy, one navigation.

Work in `site/` at the repository root unless a directory already exists for this. Do not
modify engine source except to add doc examples that are compiled and tested.

---

## 1. Ground truth

`CLAUDE.md` is the architecture document and `ROADMAP.md` is the status document. Read both
in full before writing a line of copy. Then read the actual source: `src/`,
`crates/tephra-types`, `crates/tephra-client`, `crates/tephra-proto`, the integration tests
in `tests/`, and the benches in `benches/`.

Three rules follow from that:

- **Never describe a feature that does not exist.** Everything in Phase 8 of `ROADMAP.md` is
  unbuilt: replication, retention and archival, crypto-shredding, index segment merging, the
  separate compressed blob region, persisted offset sidecars, the hash-chained audit log. If
  the site mentions any of them, it mentions them under a clearly labelled "not built yet"
  heading, in future or conditional tense, with no implication of a date.
- **Never invent an API.** Every type name, method signature, config field, and error variant
  on the site must be copied from the source. If you want to show something the API does not
  currently expose cleanly, say so in your final report rather than papering over it.
- **Do not soften the constraints.** Single logical writer per bounded context, no sharding,
  fsync as the throughput ceiling, single node. These are stated plainly in `CLAUDE.md`
  section 10 and they belong on the site with the same directness. An engineer who hits them
  after adopting the database is a worse outcome than one who reads them and walks away.

Where `CLAUDE.md` gives a rationale, the site can carry the rationale. That reasoning is the
most valuable thing the project has to publish and most databases never write it down.

---

## 2. Stack

Astro with the Starlight docs theme, TypeScript, deployed as a fully static build. No client
framework, no runtime data fetching, no analytics, no cookie banner. Ship HTML and CSS with a
few kilobytes of JavaScript for search and the theme toggle, nothing more.

If you have a concrete reason to prefer something else (mdBook, Zola, plain Astro without
Starlight), state the reason and the tradeoff in one paragraph before you build, then proceed
with your choice. Do not ask a question you can answer yourself from the constraints above.

Requirements regardless of stack:

- Full-text search over the docs, offline, no hosted service.
- Rust syntax highlighting with correct token colours for lifetimes, attributes, and macros.
- Dark and light themes, system default respected, choice persisted.
- Deploy target is static hosting. Produce a working GitHub Actions workflow that builds and
  deploys on push to the default branch.
- Lighthouse: 100 on accessibility, 100 on best practices, and no layout shift.

---

## 3. Site map

**Landing page.** One screen that says what Tephra is and who it is for, then a code block
showing an append with a condition and a read, then the four or five structural claims with a
sentence each, then the honest constraints, then install and next steps. No testimonials, no
logo wall, no "trusted by", no fabricated benchmark numbers. The only numbers permitted are
ones you can source from `benches/` output or from `ROADMAP.md`, and each must be labelled
with the hardware and the caveat (the condition-path bench numbers are on tmpfs, for example).

**Docs, in this order:**

1. *Introduction*: what a Dynamic Consistency Boundary is and the problem it removes. Work
   from `CLAUDE.md` section 1. Use the course-and-student example, since it is the canonical
   one in DCB material and readers arriving from Sara Pellegrini's work will recognise it.
   Show the aggregate-plus-saga version of the same decision alongside the DCB version.
2. *Getting started*: install, run the server, connect with `tephra-client`, append, read,
   subscribe. Must be executable start to finish by someone who has never seen the project.
3. *Core concepts*: events, types and tags, positions, queries (OR across items, AND within an
   item's tags), append conditions and the `after` bound, decision models.
4. *Guides*: modelling a decision model from small projections; the uniqueness guard pattern
   (`after` omitted); building a read model with subscriptions, including cursor persistence
   and resume; handling `ConflictSite::SameBatch` versus `ConflictSite::Durable`. That last one
   is a real obligation on the client and is easy to get wrong, so give it a full page: same-
   batch is advisory and retryable, durable is terminal until the client rebuilds its decision
   model. Show the retry loop.
5. *Operations*: configuration surface (`SegmentConfig`, `WriterConfig`, `ReadConfig` and their
   real fields), durability semantics, what happens on crash and what recovery does, startup
   outcomes (clean, recovered with rollback, corrupt and refusing to open), what a corrupt
   index does versus a corrupt log, backpressure behaviour, shutdown.
6. *Architecture*: the layer map and the design rationale, adapted from `CLAUDE.md`. This is a
   public-facing rewrite, not a copy and paste. Keep the rejected alternatives and why they
   were rejected, since that is the part readers cannot get anywhere else. Cover the log
   format, batch commit markers and the recovery rule, the split of high-cardinality tags into
   an inverted index against low-cardinality types into a dense column, position-disjoint
   segments and why merging is concatenation, the two-arm condition check, the append-only
   active tail and the published watermark, and the planner's invariant that it can change the
   speed but never the answer.
7. *Comparison*: UmaDB first, using `CLAUDE.md` section 15. Be fair to it. It reaches the same
   single-writer conclusion by a different route and its dual-header COW gives cleaner
   recovery than a scan. State where Tephra diverges and the workload assumption behind each
   divergence. Then a shorter section on why this is not EventStoreDB, Kafka, or Postgres, in
   terms of what each optimises for rather than in terms of who wins.
8. *Status*: what is built (phases 1 through 7), what is deferred, what would need to be true
   before the deferred work starts. Link to `ROADMAP.md`.

A `llms.txt` at the root, and a version-pinned note somewhere visible about which commit the
docs describe.

---

## 4. Writing style

The house style is the one already in `CLAUDE.md`: declarative, dense, technically specific,
willing to state a tradeoff without hedging it into mush. Match it. The site should read like
one engineer explaining a system to another, not like marketing copy and not like a model
imitating marketing copy.

**Hard rules:**

- **No em dashes anywhere.** Not in prose, code comments, alt text, meta descriptions, or
  commit messages. Use a colon, a comma, parentheses, or two sentences.
- No en dashes standing in for em dashes either.
- No "delve", "leverage", "robust", "seamless", "cutting-edge", "battle-tested",
  "blazingly fast", "game-changer", "unlock", "empower", "elevate", "journey", "landscape",
  "realm", "tapestry", "at its core", "under the hood", "the beauty of", "it's worth noting",
  "in today's world", "whether you're X or Y".
- No "not just X, but Y" and no "it's not about X, it's about Y".
- No sentence fragments used for emphasis on their own line. No rhetorical questions as
  section openers.
- No triads for rhythm. Three items in a list is fine when there are three things.
- No bold text scattered mid-sentence for emphasis. Bold is for defined terms and labels.
- No emoji.
- Prefer the concrete number, structure name, or code path over an adjective. "Rejects the
  batch if any record in the run fails CRC" beats "highly reliable".

Vary sentence length. Some paragraphs should be a single long sentence carrying a full
argument; others should be four words. Uniform medium-length sentences are the clearest tell
of generated prose.

Australian or British spelling is already used in the repo (`serialization` appears alongside
`optimisation`, so pick one and be consistent across the site). Choose British spelling and
apply it uniformly.

---

## 5. Code examples

Every code sample compiles. Put them in `site/examples/` as a real crate in the workspace, or
as doctests in the engine, and wire them into CI so a signature change breaks the build rather
than silently rotting the docs. Import them into the pages rather than retyping them.

Samples should be short and load-bearing. The full decision-model cycle (read, fold
projections, decide, append with the same query as the condition) is the one example that
matters most, since it is the thing DCB exists for and the thing a reader will copy. Show it
early and show it complete, including the retry on `SameBatch`.

Show errors being handled, not `.unwrap()` everywhere.

---

## 6. Design

Restrained and typographic. Serif or a high-quality grotesque for headings, a proper monospace
for code, generous line height, a measure of roughly 70 characters, real vertical rhythm.

Do not produce the default generated-site look: no purple to blue gradient hero, no glassmorphic
cards, no floating gradient orbs, no glowing borders, no animated particle background, no
three-column feature grid with rounded icons. If it looks like every AI startup landing page
from 2024, delete it and start again.

One accent colour, used sparingly. Ash grey and a warm volcanic tone would suit the name:
Tephra is named after tephrochronology, where volcanic ash layers deposited by a single
eruption form a global time marker across otherwise unrelated sediment records. That is exactly
what a globally ordered log does for events, and it is worth one short paragraph on the
landing page or an about page. Do not over-extend the metaphor beyond that paragraph.

Diagrams: hand-authored inline SVG, themed with CSS variables so they work in both colour
schemes. The segment layout, the record format, the two-arm condition check, and the
catch-up-to-live subscription cursor all earn a diagram. Nothing decorative.

---

## 7. Working method and subagents

Use subagents at the two ends of the job and nowhere in the middle.

**Before writing anything**, dispatch one subagent to build an API inventory. It reads `src/`,
all three crates under `crates/`, `tests/`, and `benches/`, and writes `site/API-NOTES.md`:
real type names, method signatures with their exact parameters and return types, every public
config struct and its fields and defaults, error enums and variants, and any benchmark figures
with the hardware and caveat attached to each. That is a large amount of reading whose useful
output is a few pages, which is the case where a subagent pays for itself. Write the site from
that file.

**After the site is drafted**, dispatch subagents in parallel for verification: link checking,
grepping the built output against the style rules in section 4, compiling and running the
examples, and cross-checking every capability claim on the site against Phase 8 of
`ROADMAP.md`. All mechanical, all parallel, none of it produces prose.

**Do not spawn subagents to write pages.** The work decomposes neatly by page and that is a
trap: each subagent writes from its own context, so the pages come out individually acceptable
and collectively inconsistent in voice, terminology, and level of detail. The docs have to read
as one person explaining one system. Write every page in the main thread, in one continuous
pass, in one voice.

Maintain `site/PROGRESS.md` as you go: which pages are drafted, which are verified, decisions
made and why, anything flagged for the human. Update it as you finish each page, not at the
end. Context will compact several times over a job this size and that file is what survives it.

---

## 8. Before you report done

- `npm run build` succeeds with no warnings.
- Every internal link resolves. Every external link returns 200.
- Every code sample compiles and, where it makes sense, runs.
- Grep the whole site output for em dashes and for the banned phrases in section 4. Zero hits.
- Read the rendered landing page and the getting started page aloud in your head. If a
  sentence sounds like it was written to fill space, cut it.
- Confirm against `ROADMAP.md` that nothing described as working is actually in Phase 8.

Report back with: the stack decision and its reasoning, the file tree, anything in
`CLAUDE.md` or `ROADMAP.md` that was ambiguous or that you think is now stale, any API that
was awkward to document (that is usually a signal about the API, not the docs), and a list of
claims you were unsure you could support so they can be checked.
