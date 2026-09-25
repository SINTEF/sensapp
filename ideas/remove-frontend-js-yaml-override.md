# Remove Frontend js-yaml Override

The OpenAPI generator's parser still resolves a vulnerable `js-yaml` release.
The frontend package currently overrides it to 4.3.2 or newer, and generation,
tests, and build pass. Once `@hey-api/json-schema-ref-parser` depends on a
patched version itself, remove the override and rerun `npm audit`, client
generation, frontend checks, and a live API smoke test.
