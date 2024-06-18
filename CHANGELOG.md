# Changelog

User-relevant changes in `symmetricjsonrpc3`.

## [0.5.0] - 2024-06-18

### Security

- **Tests:** don't bind listening TCP sockets to `0.0.0.0`.

### Added

- When debug-logging, include the remote address in the log
  if applicable for given object.
- `RPCError` exception type that is raised when awaiting a Response and
  an Error Response arrives. If Response fails due to an internal exception,
  re-raise that exception instead.
- **Docs:** added this CHANGELOG.md.
- **Docs:** added CONTRIBUTING.txt; move the "Conventions" section there.
- **Examples:** added a `pingspam_client` that tortures `ping_server`
  with many a ping.
- **Examples:** added even more verbose logging options to all examples
  when `-vv` is used.
- **Examples:** added `--timestamps` argument to all examples.

### Changed

- Replace the IO `wrappers` module with a more versatile and better
  tested `io`. Just like `wrappers`, `io` is an internal module.
- `params` in JSON-RPC Request is optional, so omit it if empty.
- `params` in JSON-RPC Request is now type-checked to see if it
  is a JSON structured type. If not, a `TypeError` is raised.
- **Tests:** shebangs now run pytest.

### Fixed

- JSON-RPC Response objects that lack the `'id'` attribute will be
  ignored now instead of failing an assertion.
- Various thread synchronization issues when sending RPC requests.
  Narrow the scopes of the existing locks, allowing threads to run
  concurrently where it causes no issues. Add locks to the *response
  awaiters* dict where these locks were missing, while this structure
  can be accessed concurrently by different threads.
- Concurrency deadlock: when waiting for a reply to a request, check
  if the reply hasn't already arrived before the wait is started. If
  yes, don't wait, just return.
- Fail all waiting requests when `RPCClient` breaks connection or
  fails otherwise, allowing their waiters to know that an error has
  happened and preventing them from getting stuck.
- Don't consume and drop the `BaseException` when it occurs when
  dispatching a JSON-RPC Notification. Let it escape. Keep consuming
  all other `Exception` types.
- The compact JSON separators weren't actually set when encoding
  JSON, resulting in a JSON that wasn't very compact.
- **Examples:** correct `ping_client` to say it works with
  `ping_server`, not with itself.
- **Tests:** allow the OS to pick the port for the listening socket;
  don't hardcode port 4712 for all tests anymore.

## [0.4.0] - 2024-06-06

### Security

- **Examples:** the server example no longer binds to `0.0.0.0` by
  default; it uses `localhost` instead. This means it doesn't
  open itself to everyone on the network anymore.

### Added

- Add `__version__` dunder to the package.
- **Docs:** add "Source origin" section to README to explain the origins
  of this project.
- **Examples:** enhance the client and server examples with more
  command-line arguments: `--host`, `--port`, `--verbose`, `--quiet`.
- **Examples:** log messages with the verbosity prefix (I, E, C or D).
- **Examples:** log when the other end sends something unexpected.

### Changed

- **Reduce the library's scope to the JSON-RPC 2.0 standard,**
  thus dropping the notion of JSON-RPC 1.x.
- Change the JSON-RPC "response" and "error" objects to fit
  the JSON-RPC 2.0 standard.
- Use Python's built-in `json` module, significantly rewriting our own
  `json` module to retain the possibility to scan multiple documents
  from a single stream and to retain compact JSON encoding when writing.
- Unify the names of `f` and `file` attributes in `wrappers` by renaming
  both to `stream`.
- **Examples:** rename `client.py` and `server.py` to `ping_client.py`
  and `ping_server.py` respectively.

### Fixed

- **Examples:** bad shebang in `client_emulate.py`.

### Removed

- **JSON-RPC 1.x support.**
- There's no P2P in JSON-RPC 2.0, so remove the `RPCP2PNode` class.
- Remove the `__jsonclass__` feature because it's not a part of JSON-RPC 2.0.
- Remove the `ReIterator` class because it was not needed anymore after
  the conversion to the built-in `json`.

## [0.3.0] - 2024-06-03

### Changed

- The `print()`-based logging converted to a `logging`-based logging.

### Fixed

- Improve the real file (`fileno()`) checks in `wrappers` to check
  if `fileno()` actually works; this enables more file-likes to work.
- **Tests:** remove `Test*` prefix from non-tests, preventing the
  runners from picking them up incorrectly.
- **Tests:** the JSON encoding test captured too wide exception
  spectrum, and **PASS-ed** the test on an actual bug.
- **Tests:** `SomeObj` encoding test assumed that Python `dict`s are
  ordered; they are not. Use `OrderedDict` in this test instead.

## [0.2.0] - 2024-05-29

This release includes changes from the original maintainer
from 2014 and before, however these are not listed here.

### Added

- **Python 3 support.**
- **Docs:** say in README that this library is dependency free.
- **Docs:** add project's "Conventions" to README.

### Changed

- **Rename the package to `symmetricjsonrpc3`.**
- **Convert the project scheme to a pyproject.toml project with src layout.**
- **Docs:** explain in README that this is a fork of the original
  symmetricjsonrpc.
- **Docs:** add .txt extension to README so that it's clear that
  the format is plain-text (tools deduce that).

### Fixed

- **Docs:** correct the URL to https://jsonrpc.org in README.

### Removed

- **Removed Python 2 support.**
- **Docs:** removed subjective, bold claims from README.
- **Build:** removed the `debian/` directory; Python wheels and
  PyPI are the way to distribute Python packages.

## [0.1.0] - 2011-09-16

Original maintainer release.

[0.5.0]: https://github.com/Zalewa/python-symmetricjsonrpc3/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/Zalewa/python-symmetricjsonrpc3/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/Zalewa/python-symmetricjsonrpc3/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/Zalewa/python-symmetricjsonrpc3/compare/release-0.1...v0.2.0
[0.1.0]: https://github.com/niligulmohar/python-symmetric-jsonrpc/releases/tag/release-0.1
