# Atoapi v1.5.3

## Oversized Responses FullReplay protection

- Added a local 16 MiB byte-size gate for native Responses FullReplay bodies.
- Oversized streaming requests now return a canonical `response.failed` event;
  synchronous requests return HTTP 413.
- The gate runs before gzip, cache waiting, and the one-shot upstream POST, so
  rejected requests record zero upstream attempts and do not add load.
- Added regression coverage proving the upstream receives zero requests.

Validation: 1,065 Rust tests passed (12 ignored); targeted oversized FullReplay
regression passed; `cargo check --locked` and the frontend build passed.
