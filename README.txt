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

Source origin
=============

This is built upon a library 'symmetricjsonrpc' that had its last work
done in 2014 (10 years before this fork).

  https://github.com/niligulmohar/python-symmetric-jsonrpc/

Don't ask me why I added '3' to it, instead of '2', because I haven't
got a clue. I also wanted to keep the 'symmetric' part because it gives
off the function of this lib pretty well.

I realize this looks /a bit/ like typo-squatting, but I hope to keep the
name 'symmetricjsonrpc3' for the time being.

  ~ Robikz, June 2024
