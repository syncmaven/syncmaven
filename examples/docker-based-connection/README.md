# Example of syncmaven connection

This package implements a sync-maven connection as a docker image. See [syncmaven](https://syncmaven.sh/fundamentals/protocol) 
protocol.

The connector does nothing, it:
 * Exposes one stream called example
 * The stream requires a row to have a param called `email`