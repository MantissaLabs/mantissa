@0xf08183b16f9253e7;

interface RestAdmin {
  # RestAdmin controls the node-local HTTP REST facade.
  #
  # This capability is intended only for local admin sessions. It exposes the
  # bearer token used by the optional loopback REST listener and must not be
  # granted to remote peer sessions.

  showToken @0 () -> (token :Text);
  # Returns the current REST bearer token.

  rotateToken @1 () -> (token :Text);
  # Rotates the REST bearer token and invalidates the previous value.

  validateToken @2 (token :Text) -> (valid :Bool);
  # Validates a presented REST bearer token without returning the stored token.
}
