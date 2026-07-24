---
name: analyze-traffic-capture
description: Finds patterns and data flow in mitmproxy .flows captures or .har files of Microsoft/Xbox traffic (e.g. samples/*.flows in this repo) — grouping repeated calls to the same endpoint, diffing which fields vary vs. stay constant across occurrences, and extracting field values across many requests. Use when asked to "find patterns in this capture", "what does this .flows/.har file show", "which fields change between these requests", or given a mitmproxy/HAR trace to reverse-engineer.
---

# Analyze a mitmproxy/.har traffic capture

## 1. Identify what you have

- `.flows` — mitmproxy's native binary flow format. This repo has real ones checked in under `samples/`
  (`init.flows`, `devassoc.flows`, `update.flows`) — some are 100s of MB, so never try to `cat`/`Read` them
  directly; always go through `mitmdump` or `mitmproxy.io`.
- `.har` — plain JSON (`log.entries[]`, each with `request`/`response`). Small ones can be read directly; large
  ones should still be streamed/filtered with `jq` or Python rather than loaded whole.

Verify that `mitmdump`/`mitmproxy` (v12+) are installed on this machine — confirm via `which mitmdump`.

## 2. Quick CLI triage with `mitmdump`

```bash
# One-liner per matching flow: method, URL, response status/size.
# IMPORTANT: the filter expression is a *positional* arg and greedily swallows
# everything after it — always put it LAST on the command line, after all flags.
mitmdump -nr samples/init.flows --flow-detail 1 '~u login.live.com'
```

`--flow-detail` levels: `0` no output, `1` shortened URL + status, `2` adds full headers, `3` adds body content,
`4` very verbose (includes binary/truncation info). Start at `1` to scope down, then bump to `3` only on an
already-narrow filter — dumping full bodies unfiltered on a multi-hundred-MB file will flood your context.

Filter expressions (verified working; run `mitmdump --help` for the complete, version-current list):
- `~u <regex>` — URL matches
- `~m <method>` — HTTP method (e.g. `~m POST`)
- `~bq <regex>` — request body contains
- `~bs <regex>` — response body contains
- `~d <domain>` — domain matches
- combine with `&`, `|`, `!`, e.g. `'~u RST2.srf & ~m POST'`

Example — confirm every `deviceaddcredential.srf` request carries a `Membername`:

```bash
mitmdump -nr samples/init.flows --flow-detail 1 '~bq Membername'
```

## 3. Programmatic pattern mining (for anything beyond a quick look)

For finding patterns across *many* occurrences of the same call — which is most of what "understanding data
flow" means here (e.g. this repo's `docs/xodus/*.md` were written by diffing repeated `RST2.srf` calls to see
which XML fields are random per-request vs. structurally fixed) — use `mitmproxy.io.FlowReader` directly. It
streams flows one at a time, so it's safe on the large sample files:

```python
from collections import defaultdict
from mitmproxy.io import FlowReader

groups = defaultdict(list)
with open("samples/init.flows", "rb") as f:
    for flow in FlowReader(f).stream():
        key = (flow.request.method, flow.request.host, flow.request.path.split("?")[0])
        groups[key].append(flow)

# Which endpoints are hit, and how often
for key, flows in sorted(groups.items(), key=lambda kv: -len(kv[1])):
    print(len(flows), key)
```

To find which fields vary vs. stay constant across repeated calls to the same endpoint (the actual RE technique
— e.g. spotting that `MessageID`/`Timestamp`/`SignatureValue` differ per `RST2.srf` call but `AppliesTo` doesn't):

```python
bodies = [f.request.text for f in groups[("POST", "login.live.com", "/RST2.srf")]]
# Then either diff pairs of bodies directly (they're XML text), or regex out specific
# elements across all of them into a set to see which ones only ever have one value:
import re
for tag in ("MessageID", "AppliesTo", "InlineUX"):
    values = {m.group(1) for b in bodies for m in re.finditer(fr"<{tag}[^>]*>(.*?)</{tag}>", b)}
    print(tag, "varies" if len(values) > 1 else "constant", values if len(values) <= 3 else f"{len(values)} distinct")
```

`flow.request`/`flow.response` also expose `.headers` (a dict-like), `.content` (raw bytes), `.text` (decoded
str), `.status_code`, `.pretty_url` — enough for most extraction without needing a full XML parser first.

## 4. HAR files

HAR is just JSON:

```python
import json
har = json.load(open("capture.har"))
for entry in har["log"]["entries"]:
    req, resp = entry["request"], entry["response"]
    headers = {h["name"]: h["value"] for h in req["headers"]}
    body = req.get("postData", {}).get("text")
    # resp["content"]["text"] may be base64 if resp["content"].get("encoding") == "base64"
```

## 5. Handling what you find

- **These captures contain real secrets in cleartext** — device `Membername`/`Password`, RSA key material,
  `SPLicenseBlock`s, tokens. Never paste a raw excerpt into a committed doc, commit message, or new code without
  redacting it first (see the `trace-to-docs` skill's redaction convention: `<!-- values are random -->` comments,
  `<!-- MODULUS -->`/`<!-- BLOB -->` placeholders).
- If you find a captured `SPLicenseBlock`/`ClepSignState`/`ClepHmacState`/`EncryptedDeviceKey` blob worth
  decoding, hand it to the `decode-secret-blob` skill.
- If you've identified a new endpoint or a flow not yet covered in `docs/xodus/` or `docs/xbox/`, use the
  `trace-to-docs` skill to write it up in this repo's established style.
- Cross-check what you find against what xodus already implements before assuming it's new:
  `crates/xodus/src/models/soap/` (request/response shapes), `crates/xodus/src/api/live/` (the calls themselves),
  and `docs/xodus/README.md` / `docs/xbox/README.md` for existing writeups.
