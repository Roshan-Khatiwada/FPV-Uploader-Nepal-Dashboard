# Nepal vendor upload

The uploader uploads the selected folder as one complete tree, preserving all child folders. Folder names are treated as opaque names; no device ID or naming convention is assumed.

```powershell
fpv-upload.cmd upload-vendor "E:\"
```

Each session is stored in `fpv-stereo-nepal` as:

```text
raw/cid-<content-id>/
  data/<original-session-folder>/<all original files and nested paths>
  metadata.json
  _control/source-manifest.json
  _control/_COMPLETE.json
```

`_uploads/cid-<content-id>.json` is a private duplicate registry. This bucket is not connected to the collection dashboard.

The uploader optionally accepts independent free-text labels with `--l1`, `--l2`, and `--l3`. No taxonomy or parent/child validation is applied. These labels are saved in `metadata.json` and do not affect the content ID.

The content ID hashes each non-video file's relative name, size, and complete contents. Video files are uploaded and size-verified, and video bytes are excluded whenever nonvideo files exist. When no non-video files exist, the ID hashes filenames, sizes, and 128 KiB samples from the beginning, middle, and end of every video (at most 384 KiB per video). This ID is the same across computers and ignores modification times. Sampling cannot detect changes outside these samples. The original session folder and everything below it are retained under `data/`. Retrying is safe: an already claimed content ID is reported as a duplicate, and files are never overwritten.

Files named `._*` and operating-system metadata directories are ignored.

The selected folder is now always uploaded as one complete tree; child folders are not split into sessions. Video-only folders use deterministic sampled IDs. Zero-byte files are supported.

## Nepal upload dashboard

- URL: https://fpvnepal.satpal-341.workers.dev
- Access: Cloudflare Access, verified `@fpvlabs.ai` identities only. Origin also
  validates JWT issuer/audience/signature and email allowlist. All routes are protected.
- One selected source folder = one SD card receipt; content ID coalesces retries.
- `_control/upload-status.json` records progress and measured duration. The broker
  writes `_monitor/cards/<content-id>.json` with R2 custom metadata for efficient
  listing. Operators cannot write the monitoring index directly.
- Verified requires a completion marker and a server-side count/byte comparison
  against `data/`. The uploader also verifies every expected path and size.
- Durations sum all video files, including simultaneous camera views. This is NOT
  unique activity/recording time. MP4/MOV use seek-based movie-header parsing;
  other formats use ffprobe when available. Unknown duration never blocks upload.
- Existing uploads lacking duration receipts are displayed with pending hours.
  Re-run this package on the original folder to add receipts without reuploading.
- For additional emails: create a Nepal-only Access policy (do not edit the shared
  company policy), attach it to application `dc27722a-1a69-4dc9-88a4-89fd8ee797a7`,
  and add the exact emails to `ACCESS_ALLOWED_EMAILS` in `wrangler.nepal.jsonc`.
- Deploy dashboard with `cd dashboard-worker && npx wrangler deploy --config wrangler.nepal.jsonc`.
  Generate Nepal types before checking: `npx wrangler types nepal-configuration.d.ts --env-interface NepalEnv --config wrangler.nepal.jsonc`.
