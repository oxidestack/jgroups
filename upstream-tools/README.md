# upstream-tools

Small standalone tools that call the *real* upstream library to produce test fixtures for
`jgroups-rs`, kept out of `src/` because they aren't part of the crate and out of `upstream/`
because that submodule is upstream's own pinned source, read-only (see the repo README).

## vector-harness

Produces `tests/vectors/wire.vectors`: byte vectors for the wire codec (`src/wire/`), captured by
calling `ClassConfigurator` (protocol and magic ids, `ids.*` lines), `org.jgroups.util.Bits`, `Version`, `UUID`/`ExtendedUUID`/`IpAddress`, every in-scope
concrete `Header`, and `Util.writeMessage`/`writeMessageListHeader` on the **real**
`jgroups-5.2.28.Final` jar — not hand-derived. This is the primary reason `src/wire/`'s tests are
trustworthy: they assert against what upstream's own code actually emits, not against a
re-implementation's idea of what it should emit.

### Regenerating

```sh
cd upstream-tools/vector-harness
./regenerate.sh                              # writes ../../tests/vectors/wire.vectors
./regenerate.sh /path/to/other/output.vectors # or write elsewhere, e.g. to diff by hand
```

Requires a JDK (17+) and Maven. The jar comes from Maven Central
(`org.jgroups:jgroups:5.2.28.Final`) via the ordinary dependency-resolution path in
`vector-harness/pom.xml` — the first run needs network access; afterwards it's cached under
`~/.m2/repository/org/jgroups/jgroups/5.2.28.Final/` and every later run is offline.

To confirm the checked-in vectors still match:

```sh
./regenerate.sh /tmp/wire.vectors.check
cmp /tmp/wire.vectors.check ../../tests/vectors/wire.vectors && echo "byte-identical"
```

### If the pinned JGroups version ever changes

`upstream/` is pinned to `jgroups-5.2.28.Final`. If that pin is ever
bumped, update `vector-harness/pom.xml`'s `<version>` to match, rerun `regenerate.sh`, and
expect `src/wire/version.rs`'s `VERSION` constant (and any byte-length assertions that happen to
depend on it) to need a matching update — the vectors are the source of truth, not the constant.

### Why a separate Maven project instead of building against `upstream/`

`upstream/` is JGroups' own source tree (Ant/Maven build scripts included for reference, per its
license), but building it standalone pulls in its full dependency set (logging frameworks, a JMX
bridge, etc.) for no benefit here — all this harness needs is the classes at runtime. Depending on
the published jar from Maven Central is simpler and just as faithful: it's the same
jgroups-5.2.28.Final bytecode either way.
