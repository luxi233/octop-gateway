# Agent Note: wechatauto outbound media keeps the original filename

Status: implemented

## Problem

Recipients saw outbound files as `wca_out_<random>.pdf` instead of the
real name. `_deliver_media` staged every non-image payload through
`tempfile.mkstemp(prefix="wca_out_", suffix=ext)`, and the bridge's UIA
layer uses the path basename as the WeChat attachment label — so the
random temp name leaked to the UI.

## Decision

Two send paths in `WechatAutoChannel._deliver_media`:

1. `file://` media passes its real local path straight to the bridge —
   zero byte copy, real basename reaches the chat for free.
   `_local_media_path` rejects non-file schemes (`wca-media://`,
   `https://`) and paths that no longer exist.
2. Byte-backed media (`FileContent.data`, the shape
   `tool_media.block_to_content_part` actually emits for
   `send_file_to_user`) still stages a temp file, but inside a
   `wca_out_*` temp *directory* under the declared `filename`. The dir
   is random; the basename is the original — that is the part the
   recipient sees. Sanitizing is basename-only plus a `.`/`..` guard.

Cleanup removes both the staged file and its directory in `finally`;
send failure still degrades to the text-marker fallback.

## Alternatives considered

- Pass `filename` as a new bridge API field and let the bridge rename —
  rejected: it widens the bridge/channel protocol for a purely local
  staging detail. The bridge already derives the label from the path
  basename; giving it a correctly named path is the minimal fix.
- `mkstemp` + `os.rename` to the real name in the same temp dir —
  equivalent outcome, but `mkdtemp` keeps the random component at
  directory level so the basename is never touched; clearer ownership
  of cleanup (one dir, one file).
- Keep `file://` passthrough out because the current producer always
  inlines base64 — rejected: the shortcut is 10 lines, avoids a full
  copy of multi-MB manuals, and is correct for any future producer that
  emits real local URLs.

## Consequences

- Recipient sees the declared/original basename; WeChat no longer leaks
  `wca_out_*` internals.
- `file://` sends skip the bytes copy entirely; data-backed sends pay
  one extra directory create/remove per file — negligible.
- Filenames are sanitized to basename only; a filename that is `.`/`..`
  or empty falls back to `file.<ext>`.
- The temp dir cleanup is suppressed-best-effort (`os.rmdir` guarded) —
  a bridge still holding the file handle on Windows leaves an empty dir
  behind, which temp cleaning reaps later; this matches prior behavior.
