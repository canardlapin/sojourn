# ADR 0003: Depend downward on the provider-neutral kernel

Status: accepted (2026-07-24)

Sojourn's pure contracts and filesystem mechanics must not be owned by a scheduler backend.
`sojourn-core` therefore depends only on `remote-exec-kernel` for validated identifiers, codecs,
diagnostics, freshness, retry provenance, failure reports, and content identity. It has no
dependency on any scala-slurm artifact.

`sojourn-runtime` imports atomic publication directly from the same kernel. It may still compose
scala-slurm protocol and worker execution at the executable boundary, but worker policy does not
own the filesystem primitive. Local and Slurm sites remain peers implementing Sojourn's site
surface.

The neutral kernel contains no scheduler, site, queue, spool, lease, worker runtime, or retry
policy. Its atomic operations require private `CREATE_NEW` staging, force-before-publish,
`ATOMIC_MOVE`, and typed refusal when atomic rename is unavailable; no copy/delete fallback is
permitted. Existing canonical spool encodings remain byte-identical because Scala package names
are not encoded.

Operation identity follows the same rule: the kernel's `OperationDescriptor` is the sole stored
id/version/input-schema/output-schema tuple. Typed references wrap it; Sojourn registrations add
codecs, retry safety, and an implementation without copying identity fields. Catalogs, byte
handlers, one-shot worker tasks, and pilot execution are derived views of those registrations.
