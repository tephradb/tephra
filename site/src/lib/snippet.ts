// Extracts a named region from a raw source file so a page can show the exact code that was
// compiled and tested in `tephra-site-examples`. A region is the lines between
// `// ANCHOR: name` and `// ANCHOR_END: name`. Marker lines are removed and the block is
// dedented to its own least indentation.
//
// Import the file with Vite's `?raw` suffix and pass it in:
//
//   import raw from "../../tephra-site-examples/tests/decision_model.rs?raw";
//   const code = anchor(raw, "cycle");

export function anchor(raw: string, name: string): string {
  const lines = raw.split("\n");
  const start = lines.findIndex((l) => l.includes(`// ANCHOR: ${name}`));
  const end = lines.findIndex((l) => l.includes(`// ANCHOR_END: ${name}`));
  if (start === -1 || end === -1 || end <= start) {
    throw new Error(`snippet region "${name}" not found`);
  }

  // Drop the marker lines, and any nested ANCHOR markers inside the region.
  const body = lines
    .slice(start + 1, end)
    .filter((l) => !l.includes("// ANCHOR"));

  // Dedent by the smallest indentation among non-blank lines.
  const indents = body
    .filter((l) => l.trim().length > 0)
    .map((l) => l.match(/^\s*/)?.[0].length ?? 0);
  const dedent = indents.length ? Math.min(...indents) : 0;

  return body
    .map((l) => l.slice(dedent))
    .join("\n")
    .replace(/^\n+/, "")
    .replace(/\n+$/, "");
}
