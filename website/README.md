# Karapace website

The source for [karapace.io](https://www.karapace.io), built with
[Docusaurus](https://docusaurus.io/).

## Requirements

- [Node](https://nodejs.org/) — see [`.nvmrc`](.nvmrc) or the `engines` field in
  [`package.json`](package.json).
- [pnpm](https://pnpm.io/) as the package manager.

## Local development

Install dependencies:

```bash
pnpm install
```

Start the dev server on `http://localhost:3000/`:

```bash
pnpm start
```

Build the static site into `build/`:

```bash
pnpm build
pnpm serve   # preview the production build locally
```

## Linting

```bash
pnpm lint       # prettier + eslint + markdownlint checks
pnpm reformat   # apply fixes
```

A `pre-commit` git hook runs `lint-staged` on staged files. It is installed
automatically by the `prepare` script when you run `pnpm install` (which points
`core.hooksPath` at `.githooks/`).

## Spell and prose check (Vale)

[Vale](https://vale.sh/) checks spelling and prose. CI runs it at error level; run it
locally with the same rules before pushing. Install Vale first (for example
`brew install vale`).

```bash
pnpm spell:error   # errors only (same as CI)
pnpm spell:warn    # errors and warnings
pnpm spell:all     # errors, warnings and suggestions
```

Add project-specific terms to
[`.vale/styles/Karapace/spelling-karapace-vocabulary.txt`](.vale/styles/Karapace/spelling-karapace-vocabulary.txt).

## Checking links

```bash
pnpm markdown-link-check              # check all markdown links (needs internet)
pnpm markdown-link-check -- -o        # offline mode: skip http(s) links
pnpm markdown-link-check -- -q        # quiet mode
pnpm markdown-link-check -- -f docs/install.md   # a single file
```

## Redirects

Path redirects live in [`_redirects`](_redirects). A post-build plugin copies the file
into `build/` (for Cloudflare Pages) and writes `current-routes.json` with every generated
route, which helps spot links that need a redirect after restructuring.

## Content

Documentation pages live in [`docs/`](docs) as Markdown. The navigation is defined in
[`sidebars.js`](sidebars.js). The homepage is a React page in
[`src/pages/index.js`](src/pages/index.js).
