# jgroups

<!-- languages:begin -->
**English** · [Русский](README.ru.md) · [中文](README.zh-CN.md) · [Español](README.es.md)
<!-- languages:end -->

An unofficial Rust port of [JGroups](https://github.com/belaban/JGroups) 5.2.28.Final, the Java toolkit for reliable
cluster communication: channels, group membership (GMS), failure detection, message reliability and the TCP
transport. It is not affiliated with or endorsed by the JGroups project, whose code is licensed under the Apache
License 2.0 and is pinned under `upstream/` for reference.

It is built to interoperate with JGroups on the wire: a Rust node and a Java node of the same version join one
cluster. [mock-server](https://github.com/oxidestack/mock-server) uses it for clustering.

Status: pre-release. The crate is not published on crates.io yet; its first release will be `0.1.0`.

## What it covers

Not everything in JGroups is ported: the scope is the protocols and the configuration of the stacks it was written
for, and within that scope a difference from JGroups's behaviour is a bug.

- **Transports**: `TCP`, and `SHARED_LOOPBACK`, an in-process transport for tests.
- **Discovery**: `TCPPING`, `DNS_PING`.
- **Membership and failure detection**: `GMS`, `MERGE3`, `FD_ALL3`, `VERIFY_SUSPECT2`.
- **Reliable delivery, flow control, fragmentation**: `NAKACK2`, `UNICAST3`, `STABLE`, `UFC`, `MFC`, `FRAG2`.
- **Configuration**: the JGroups stack XML, with `${VAR:default}` substitution as JGroups does it.

A configuration that names a protocol outside this list is refused with a message naming it.

## Install

Not published yet. Once `0.1.0` is on crates.io, the dependency will be:

```toml
[dependencies]
jgroups = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Until then, depend on the repository: `jgroups = { git = "https://github.com/oxidestack/jgroups" }`. The crate needs
Rust 1.88 or later and runs on [Tokio](https://tokio.rs).

## Example

Two channels on the in-process transport join one cluster, and one sends the other a message:

```rust
use jgroups::channel::JChannel;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), jgroups::channel::ChannelError> {
    let config = r#"<config>
        <SHARED_LOOPBACK/><SHARED_LOOPBACK_PING/>
        <NAKACK2/><UNICAST3/><STABLE/><GMS print_local_addr="false"/>
    </config>"#;

    let a = JChannel::new(config).await?;
    let b = JChannel::new(config).await?;
    a.connect("demo").await?;
    b.connect("demo").await?;

    b.send(Some(a.local_address().clone()), b"hello".to_vec()).await?;
    let (from, payload) = a.receive().await.expect("a message");
    println!("{from:?} sent {}", String::from_utf8_lossy(&payload));

    b.close().await;
    a.close().await;
    Ok(())
}
```

Over the network the stack names `TCP` and a discovery protocol instead, as any JGroups configuration does:
`<TCP bind_port="7800"/><TCPPING initial_hosts="10.0.0.1[7800],10.0.0.2[7800]"/>`.

## Documentation

- The guides, in English, Russian, Chinese and Spanish: <https://oxidestack.github.io/jgroups/>, with
  [upgrading](docs/guide/upgrading.md) between releases.
- The API documentation: on docs.rs once the crate is published; until then, `cargo doc --open`.
- What changed in each release: [CHANGELOG.md](CHANGELOG.md).

## Build and test

```sh
git submodule update --init upstream   # the pinned JGroups sources some tests read
cargo test
```

`make lint` runs clippy, `make deny` checks licences and advisories, `make public-check` is the publication gate and
`make site` builds the documentation site. [CONTRIBUTING.md](CONTRIBUTING.md) says how to contribute; commits are
signed off (DCO).

## Getting help

A question about how the crate works can go to [DeepWiki](https://deepwiki.com/oxidestack/jgroups) first: it answers
from this repository's code and documentation, and where it disagrees with the documentation, the documentation is
right. Questions it does not settle go to [Discussions](https://github.com/oxidestack/jgroups/discussions), bugs to
[issues](https://github.com/oxidestack/jgroups/issues).

## Licence

Code: Apache-2.0 ([LICENSE](LICENSE)). Documentation: CC-BY-4.0 ([LICENSE-docs](LICENSE-docs)). What the crate ports
is in [NOTICE](NOTICE).
