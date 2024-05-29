Python Symmetric JSON-RPC 3
===========================

"A symmetric, transport-layer agnostic JSON-RPC implemenation in Python."

A JSON-RPC (see https://jsonrpc.org) implementation for Python, with
the following features:

 * Symmetric - both the connecting and the listening process can send
   and receive method calls, there is no specific "server" or "client"
   process, and no difference between the two connection ends apart
   from who initiates the connection.

 * Asynchronous - calls can be interlieved with new calls initiated
   before a previous call has returned.

 * Thread-safe - calls to the remote side can be done from multiple
   threads without any locking.

 * Transport agnostic - can run on top of anything that resembles a
   socket the slightest (e.g. OpenSSL)

 * Dependency free

What this really drills down to is that this library implements the
full specification of JSON-RPC over sockets.

  This is a fork of niligulmohar's "symmetricjsonrpc" with the intent
  of bringing it up-to-date with current Python and publishing it
  to PyPI.

For usage details, look at the examples in the "examples" directory.

Conventions
===========

The conventions apply only since forking from the original repository.

This project uses/follows/adheres to:

- Conventional Commits 1.0.0 (https://www.conventionalcommits.org/)
- PyPA version scheme (https://packaging.python.org/en/latest/specifications/version-specifiers/)

Git tags are prefixed with 'v'.

Module's version is automatically deduced from the git tag in the
"no-guess-dev" mode.
