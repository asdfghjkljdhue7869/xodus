---
name: trace-to-docs
description: Turns a raw HTTP/SOAP/XML capture of a Microsoft/Xbox endpoint into a new doc page matching this repo's docs/xodus (narrative RE protocol flow with mermaid + sample request) or docs/xbox (terse endpoint reference) style, and wires it into the directory's README index. Use when asked to "document this trace/capture", "write up this endpoint", "add this to docs/xodus" or "docs/xbox", or given a raw request/response dump to turn into documentation.
---

# Turn a captured trace into a doc page

This repo has two distinct doc styles under `docs/`. Pick the right one before writing anything — don't default
to one.

## 1. Pick the style

- **`docs/xodus/` style** — narrative reverse-engineering writeup. Use when the capture is a multi-step exchange
  worth explaining the *why* of: auth handshakes, signing/derivation flows, anything where a future reader needs
  the reasoning, not just the shape. Examples: `device.md`, `login.md`, `clep.md`, `licenses.md`.
- **`docs/xbox/` style** — terse endpoint reference. Use for a single request/response worth recording as a quick
  lookup (a GET endpoint + JSON shape), no narrative needed. Examples: `xboxservices.md`, `gamepass.md`.

## 2. `docs/xodus/` structure

1. `# Title`
2. A mermaid diagram:
   - `sequenceDiagram` (with a `---\ntitle: ...\n---` block inside the fence) for a request/response exchange
     between actors — see `device.md`.
   - `flowchart TD` when it's data-derivation rather than back-and-forth (e.g. token + ContentId → license →
     content keys) — see `licenses.md`. Pick based on content shape, not by default.
3. A `##` section per endpoint/call (e.g. `## deviceaddcredential.srf`).
4. `### Sample request` — a fenced code block (```xml``` etc.) with real-shaped but **redacted** data. Never paste
   a real captured secret/token verbatim into a committed doc:
   - inline comments like `<!-- values are random -->` for regenerable random fields
   - `<!-- MODULUS -->` / `<!-- BLOB -->` style placeholders for large binary blobs
5. `### Components` — a flat bullet list, `- id - description`. Use `??` for fields whose purpose is unknown
   rather than omitting them (see `device.md`'s Components table for the pattern, including `??` and
   `error="-2147024894"` annotations for empty-but-valid fields).
6. Prose after each code block explaining **why**, not just what — cross-reference other docs where relevant.
7. If the same encoding/structure is shared across multiple call types, factor it into its own `##` section that
   other sections link back to (see `clep.md`'s "Shared encoding" section, referenced from three different places).
8. Cross-link with relative markdown links + `#anchor` fragments matching the target heading text lowercased and
   hyphenated, e.g. `[Device](./device.md#deviceaddcredentialsrf)`. Make links bidirectional: the doc that
   introduces a shared concept links out to where it's used, and the doc that uses it links back.
9. Use GitHub-style `> [!NOTE]` / `> [!CAUTION]` callouts for caveats that need to stand out.

## 3. `docs/xbox/` structure

1. `## EndpointName`
2. A fenced plain (no language) block: `GET https://host/path?query={PARAM}`
3. Optional ```json``` sample response.
4. 1-2 sentence note. Write uncertainty inline rather than guessing or silently omitting it — e.g. "Unsure how to
   correlate those values yet with anything meaningful."

## 4. Mandatory last step — update the README index

`docs/xodus/README.md` and `docs/xbox/README.md` are each just a flat `[Name](./file.md)` bullet list of the
pages in that directory. A new page not added to the relevant README is effectively undiscoverable — always add
it there as the final step.
