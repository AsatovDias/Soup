# Training queue design

PostgreSQL stores the queue and the simulated training effect. Its row locks let independent worker processes claim different jobs concurrently. A single transaction can commit the effect and the final state together.

**Claim.** Under READ COMMITTED, a worker selects a queued job or a running job whose lease expired, using FOR UPDATE SKIP LOCKED. The same statement marks it running, increments its attempt counter (the fencing token), and sets a lease using the database clock. The worker commits the claim before doing work. A claim may be retried after an ambiguous client error by waiting for the lease to expire.

**Recovery.** A lease is five seconds by default. Simulated work is bounded to one second, so the demo needs no renewal. Other workers can reclaim an expired job. Completion first locks the row, then checks state, fencing token, and lease against current database time. A worker that resumes after its lease has expired cannot publish its result even if nobody has reclaimed the job yet. Real training would need lease renewal and execution outside this bounded simulator.

**Cancellation.** Cancellation updates the same job row and makes queued or running jobs terminal immediately. A running worker may continue local computation until it reaches the completion check. If cancellation wins the row lock, completion cannot publish. If completion wins and commits, cancellation returns the existing terminal state. Repeated cancellation returns the current terminal state.

**Guarantee.** Attempts can repeat. Each succeeded job has exactly one committed effect row, and each failed or cancelled job has zero. A successful completion writes the effect and updates state in the same transaction. The effect primary key makes duplicate insertion an explicit error. An external side effect would need a separate idempotency protocol. A unique request key maps equal POST requests to one job. Reuse with a different payload returns HTTP 409.

**Time and isolation.** Each application session uses PostgreSQL READ COMMITTED. Claim expiry and completion checks use PostgreSQL wall time, so application clock skew cannot grant ownership. A DB clock step forward can expire leases early. A backward step can delay recovery. Fencing rejects superseded workers in either case.
