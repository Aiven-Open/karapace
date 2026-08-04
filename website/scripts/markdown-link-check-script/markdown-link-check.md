# What markdown-link-check does

Uses [markdown-link-check](https://github.com/tcort/markdown-link-check) to check links in `.md` files.

## Scripts you can use

Note: All scripts need to be run from the `website/` root and you have to set up
your [local development](../../README.md) in order to use them.

### ➡️ `pnpm markdown-link-check`

Runs the check for all Markdown files. It checks internal as well as external links (so it needs an
internet connection) and assets. Please be aware of the [limitation](#limitation).

### ➡️ `pnpm markdown-link-check -- -f "path/to/file.md"`

Runs the check for a specific Markdown file.

### Flags to use

- `pnpm markdown-link-check -- -q` runs in quiet mode and does not log every file checked, only the
  processes and the end result. Errors are still logged.
- `pnpm markdown-link-check -- -o` runs in offline mode and does not check links starting with
  http/https.

The flags can be combined and used with a single file or all files.

## Limitation

⚠️ When running the link check for all files without offline mode, the first command ("Checking links
in all markdown files in /docs") can return an exit code `1` even with no errors. Since we only use
offline mode in CI, we can live with that — check for `dead links found!` in the logs for real errors.
