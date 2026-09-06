# Atoapi v1.5.5

## Codex model compatibility

- Codex injection no longer writes or replaces `model_catalog_json` with an
  Atoapi-maintained static model snapshot.
- Existing user-owned catalog paths are preserved; legacy Atoapi catalog files
  are removed during injection cleanup.
- Codex can therefore use its current official model catalog without requiring
  an Atoapi release whenever OpenAI adds a model.
- The optional Codex UI patch no longer contains a GPT-5.6-only model allowlist.

## Compatibility

- Atoapi still routes Codex requests through the selected local provider.
- Atoapi provider models remain available through the proxy model endpoint or
  explicit model configuration; the official Codex model catalog is not
  rewritten by injection.
