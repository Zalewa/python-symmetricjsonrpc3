Python Symmetric JSON-RPC 2.0, v3
=================================

"A symmetric, transport-layer agnostic JSON-RPC 2.0 implementation in Python."

A JSON-RPC 2.0 (see https://jsonrpc.org) implementation for Python with
the following features:

 * Symmetric - both the connecting and the listening processes can send
   and receive method calls. There is no specific "server" or "client"
   process, and no difference between the two connection ends apart
   from who initiates the connection.

 * Asynchronous - calls can be interleaved with new calls initiated
   before a previous call has returned.

 * Thread-safe - calls to the remote side can be done from multiple
   threads without any locking.

 * Transport agnostic - can run on top of anything that resembles a
   socket in the slightest (e.g. OpenSSL)

 * Dependency free

This library implements the full specification of JSON-RPC 2.0 over sockets.

  This is a fork of niligulmohar's "symmetricjsonrpc" with the intent
  of bringing it up-to-date with current Python and publishing it
  to PyPI.

For usage details, look at the examples in the "examples" directory.

Conventions
===========

The conventions apply only after the point of forking from the original
repository.

This project adheres to:

- Conventional Commits 1.0.0 (https://www.conventionalcommits.org/)
- PyPA version scheme (https://packaging.python.org/en/latest/specifications/version-specifiers/)

Git tags are prefixed with 'v'.

The package's version is automatically deduced from the git tag in the
"no-guess-dev" mode.
