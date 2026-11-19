# jgroups

<!-- languages:begin -->
[English](README.md) · [Русский](README.ru.md) · **中文** · [Español](README.es.md)
<!-- languages:end -->
<!-- translation of README.md: 6e486bef40db -->

[JGroups](https://github.com/belaban/JGroups) 5.2.28.Final 的非官方 Rust 移植。JGroups 是用于可靠集群通信的 Java
工具包：通道、组成员管理（GMS）、故障检测、消息可靠性以及 TCP 传输。本项目与 JGroups 项目没有关联，也未获得其认可；JGroups
的代码采用 Apache License 2.0 许可，并固定在 `upstream/` 下以供参考。

它在线路协议层面与 JGroups 互通：同一版本的 Rust 节点和 Java 节点可以加入同一个集群。
[mock-server](https://github.com/oxidestack/mock-server) 用它实现集群功能。

状态：尚未发布。该 crate 还没有在 crates.io 上发布；首个版本将是 `0.1.0`。

## 涵盖范围

JGroups 并未全部移植：范围是本 crate 所针对的协议栈中的协议和配置，在此范围内，任何与 JGroups 行为不一致之处都是缺陷。

- **传输**：`TCP`，以及用于测试的进程内传输 `SHARED_LOOPBACK`。
- **发现（discovery）**：`TCPPING`、`DNS_PING`。
- **成员管理与故障检测**：`GMS`、`MERGE3`、`FD_ALL3`、`VERIFY_SUSPECT2`。
- **可靠投递、流量控制、分片**：`NAKACK2`、`UNICAST3`、`STABLE`、`UFC`、`MFC`、`FRAG2`。
- **配置**：JGroups 协议栈 XML，并按 JGroups 的方式进行 `${VAR:default}` 替换。

如果配置中包含此列表之外的协议，会被拒绝，并给出指明该协议的消息。

## 安装

尚未发布。`0.1.0` 发布到 crates.io 之后，依赖将是：

```toml
[dependencies]
jgroups = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

在此之前，请直接依赖仓库：`jgroups = { git = "https://github.com/oxidestack/jgroups" }`。该 crate 需要 Rust 1.88
或更高版本，运行在 [Tokio](https://tokio.rs) 之上。

## 示例

两个使用进程内传输的通道加入同一个集群，其中一个向另一个发送消息：

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

在网络上，协议栈则改为指定 `TCP` 和一个发现协议，与任何 JGroups 配置相同：
`<TCP bind_port="7800"/><TCPPING initial_hosts="10.0.0.1[7800],10.0.0.2[7800]"/>`。

## 文档

- 指南（英文、俄文、中文和西班牙文）：<https://oxidestack.github.io/jgroups/>，包括版本之间的
  [升级](docs/i18n/zh/guide/upgrading.md)。
- API 文档：crate 发布后位于 docs.rs；在此之前请使用 `cargo doc --open`。
- 每个版本的变更：[CHANGELOG.md](CHANGELOG.md)。

## 构建与测试

```sh
git submodule update --init upstream   # the pinned JGroups sources some tests read
cargo test
```

`make lint` 运行 clippy，`make deny` 检查许可证和安全公告，`make public-check` 是发布前的检查，`make site`
构建文档站点。如何参与贡献见 [CONTRIBUTING.md](CONTRIBUTING.md)；提交需要签署（DCO）。

## 获取帮助

关于本 crate 如何工作的问题，可以先问 [DeepWiki](https://deepwiki.com/oxidestack/jgroups)：它根据本仓库的代码和文档作答，
当它与文档不一致时，以文档为准。它解决不了的问题请到 [Discussions](https://github.com/oxidestack/jgroups/discussions)，
缺陷请提交到 [issues](https://github.com/oxidestack/jgroups/issues)。

## 许可证

代码：Apache-2.0（[LICENSE](LICENSE)）。文档：CC-BY-4.0（[LICENSE-docs](LICENSE-docs)）。本 crate 移植了哪些内容，见
[NOTICE](NOTICE)。
