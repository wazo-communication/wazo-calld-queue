# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.1.0] - 2026-06-18

### Changed
- **Require Wazo 26.06** and move the plugin to the `wazo` namespace under the
  `wazo-communication` organization.
- Remove the unused `AgentStatusHandler` and `QueueStatusHandler`.
- Replace a debug `print` with `logger.info` in `get_agents_status`.
- Use `load_default` instead of the deprecated `missing` argument in `QueueSchema`.

### Fixed
- Remove the recursive `__ne__` in `ArbitraryEvent` that could cause infinite recursion.
- Make `ArbitraryEvent.__eq__` robust to missing `required_acl` and foreign types.
- Avoid an `IndexError` when removing waiting calls in `_livestats`.

### Added
- Unit tests for `bus_consume`, `QueueService`, and the marshmallow schemas.
- `AGENTS.md` contributor guidance (with a `CLAUDE.md` pointer).

## [2.0.2] - 2025-12-11

### Fixed
- Add the `Loader` argument to `yaml.load()` for PyYAML 6.0 compatibility.

## [2.0.1] - 2025-10-24

### Fixed
- Correct event method handling in `bus_consume`.
- Handle the case where an agent does not belong to any queue.
- Ignore member events for `usersharedline`.
- Remove the unused `MY_TENANT` variable.

## [2.0.0] - 2025-10-22

### Added
- Multi-tenant support: resolve `tenant_uuid` from configuration.

### Fixed
- Correct an unused-variable bug.

## [1.1.1] - 2025-06-11

### Fixed
- Bug fixes.

## [1.1.0] - 2024-04-03

### Added
- Offline agent status.

## [1.0.0] - 2024-02-28

### Added
- Initial release: Queue REST API and bus events for Asterisk-based queue management.

[2.1.0]: https://github.com/wazo-communication/wazo-calld-queue/compare/v2.0.2...v2.1.0
[2.0.2]: https://github.com/wazo-communication/wazo-calld-queue/compare/v2.0.1...v2.0.2
[2.0.1]: https://github.com/wazo-communication/wazo-calld-queue/compare/v2.0.0...v2.0.1
[2.0.0]: https://github.com/wazo-communication/wazo-calld-queue/compare/v1.1.1...v2.0.0
[1.1.1]: https://github.com/wazo-communication/wazo-calld-queue/compare/24_02_v1.1.0...v1.1.1
[1.1.0]: https://github.com/wazo-communication/wazo-calld-queue/compare/24_02_v1.0.0...24_02_v1.1.0
[1.0.0]: https://github.com/wazo-communication/wazo-calld-queue/releases/tag/24_02_v1.0.0
