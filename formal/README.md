# AgentENV formal models

`AgentENVLeaseMigration.tla` is an original executable model of the ownership,
lease, and migration cutover protocol. It intentionally models the destination
as inert until the source is quiesced, durable checkpoint coverage exists, and
the ownership generation has advanced. A stale runtime generation cannot
become executing.

Run it with the official TLA+ tools:

```bash
java -XX:+UseParallelGC -jar tla2tools.jar \
  -config formal/AgentENVLeaseMigration.cfg \
  formal/AgentENVLeaseMigration.tla
```

The checked safety predicate covers type correctness, at most one executing
runtime, execution only by the current leased generation, destination fencing
during preparation, and no source quiesce before durable checkpoint coverage.
The Rust state-machine tests add storage-specific idempotency, CAS, crash, and
retry cases; the model does not replace those tests.
