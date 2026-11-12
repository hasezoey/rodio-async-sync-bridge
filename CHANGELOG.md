# Changelog

## [0.2.0]

Released on: 2026.11.12

### Breaking Changes

- *AudioSpec*: Rename "new_checked" to "try_new"
  - **BREAKING CHANGE**: "AudioSpec::new_checked" is renamed to "try_new"

- Rename "AsyncRingConsumer" to "SyncRingConsumer"
  - **BREAKING CHANGE**: "AsyncRingConsumer" is renamed to "SyncRingConsumer"

- Rename "SeekData" to "SeekRequest"
  - **BREAKING CHANGE**: Struct "SeekData" is renamed to "SeekRequest"

- *cargo.toml*: Remove "symphonia" as a default feature
  - **BREAKING CHANGE**: "symphonia" is no longer a default feature.

- Rename send functions to be more intuitive
  - **BREAKING CHANGE**: Functions got renamed:
"new_spec" -> "write_new_spec"
"new_eos" -> "write_eos"
"process_seek" -> "finish_seek"

### Features

- Rename "AsyncRingConsumer" to "SyncRingConsumer"
- Rename "SeekData" to "SeekRequest"
- Rename send functions to be more intuitive
- *AsyncRingProducer*: Rename "finish_seek" to "complete_seek" and "write_new_spec" to "write_spec"
- *AudioSpec*: Rename "new_checked" to "try_new"

### Bug Fixes

- *AsyncRingConsumer*: Actually apply the new span length
- *AsyncRingProducer*: Change "finish_seek" to align with other methods
- *AsyncRingProducer*: Return "None" when writing data bytes, but the channel closed
- *AsyncRingProducer*: Change "write_data" to be cancel safe
- *AsyncRingProducer*: Write header messages all or nothing
- *cargo.toml*: Remove "symphonia" as a default feature

### Miscellaneous Tasks

- Add a minimal and mock decoder example
- *release-plz*: Add newline after breaking changes
- *workflows/tests*: Run all target tests, including examples

### Refactor

- Rename "MessageDataFirst" to "MessageDataHeader" and "MessageDataActual" to "MessageDataPayload"
- Rename "RingMsgParse" and "RingMsgWrite" to not contain a "2" suffix

### Testing

- Add test that confirms cancel safety for writing data

### Documentation

- *README*: Update included example to reflect recent renames

### Commit Statistics

- 18 commit(s) contributed to the release.
- 0 day(s) passed between the first and last commit.
- 20769 day(s) passed between releases.

[0.2.0](https://github.com/hasezoey/rodio-async-sync-bridge/compare/0.1.3..0.2.0)

## [0.1.3]

Released on: 2026.11.10

### Bug Fixes

- Simplify "SeekData" docs

### Miscellaneous Tasks

- *cargo*: Set repository url
- *release-plz*: Remove unnecessary parser
- *workflows*: Unset registry token

### Commit Statistics

- 4 commit(s) contributed to the release.
- 0 day(s) passed between the first and last commit.
- 20767 day(s) passed between releases.

[0.1.3](https://github.com/hasezoey/rodio-async-sync-bridge/compare/0.1.2..0.1.3)

## [0.1.2]

Released on: 2026.11.10

### Dependencies

- *async-ringbuf*: Update to 0.4.0
- *rodio*: Upgrade to 0.23

### Miscellaneous Tasks

- *release-plz*: Gain back some spacing
- *release-plz*: Even more adjust spacing
- *release-plz*: Adjust spacing

### Commit Statistics

- 5 commit(s) contributed to the release.
- 0 day(s) passed between the first and last commit.
- 20767 day(s) passed between releases.

[0.1.2](https://github.com/hasezoey/rodio-async-sync-bridge/compare/0.1.1..0.1.2)

## [0.1.1]

Released on: 2026.11.10

### Miscellaneous Tasks

- fix requirements for cargo publish

## [0.1.0]

Released on: 2026.11.10

### Features

- Fix MSRV
- Change "SeekData" to be a struct
- Rename main structs
- Initial copy from termusic

### Miscellaneous Tasks

- Initial project setup
- *CHANGELOG*: Prepare for release-plz
- *release-plz*: Reduce header
- *release-plz*: Remove debug section
- *release-plz*: Dont output compare if there is no previous version
- *release-plz*: According to the docs, "remote" should be top-level available
- *release-plz*: Remove entire template for testing
- *release-plz*: Try to dump the entire context for debug
- *release-plz*: Try using "remote.*" instead of "remote.github.*"
- *release-plz*: Use a environment
- *release-plz*: "remote" is only available on "commit"
- *workflows/release-plz*: Provide default tokens
- *workflows/tests*: Use separate lint and clippy

### Refactor

- Change the callback type to be a newtype

### Revert

- Revert "chore(release-plz): remove entire template for testing"

### Commit Statistics

- 19 commit(s) contributed to the release.
- 0 day(s) passed between the first and last commit.
