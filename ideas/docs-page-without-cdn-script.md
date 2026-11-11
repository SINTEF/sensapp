# The API docs page without an unpinned CDN script

`/docs` is served by `utoipa-scalar` 0.4.0. Its page (`res/scalar.html` of the crate) loads
`<script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>`: the latest version, from a CDN,
without Subresource Integrity. The review of November 2026 (task "auth, config and deployment hardening",
item 5) raised two problems:

- **Trust.** `/docs` is on the origin of the web UI, which keeps the token in `sessionStorage`. Whatever
  jsDelivr serves for `@scalar/api-reference` that day runs there and can read it.
- **Air-gapped installs.** Without internet access the page is blank.

## Why it is not done yet

Both fixes need a copy of the Scalar bundle (about 3 MB of JavaScript), fetched, reviewed and kept up to date, or
at least a version and its hash chosen by a person. That choice was left to a maintainer.

## Options

1. **Embed a pinned copy** in the binary (`include_bytes!`) and serve it under `/docs/scalar.js`, with a custom
   HTML page passed to `Scalar::custom_html`. Works offline; updating it is a manual step, like the frontend
   dependencies. Adds about 3 MB to the binary (less compressed).
2. **Pin a version with SRI**: `https://cdn.jsdelivr.net/npm/@scalar/api-reference@<version>` with an
   `integrity="sha384-…"` attribute and `crossorigin="anonymous"`, in a custom HTML page. Small change, still
   needs the internet.
3. **Serve `/docs` without the token's origin**, so the script cannot read the UI's storage: harder, as the UI and
   the API share one origin by design.
4. **Drop the rendered page** and serve only `/api-docs/openapi.json`, for people to open in their own tool.

Option 1 fits SensApp's offline and edge use best. Whichever is chosen, a `Content-Security-Policy` on `/docs`
(`script-src 'self'` for option 1, the CDN with the hash for option 2) keeps the next change honest.
