# jgroups

<!-- languages:begin -->
[English](README.md) · **Русский** · [中文](README.zh-CN.md) · [Español](README.es.md)
<!-- languages:end -->
<!-- translation of README.md: 6e486bef40db -->

Неофициальный порт на Rust библиотеки [JGroups](https://github.com/belaban/JGroups) 5.2.28.Final — Java-инструментария
для надёжного группового взаимодействия в кластере: каналы, членство в группе (GMS), обнаружение сбоев, надёжная
доставка сообщений и транспорт TCP. Проект не связан с проектом JGroups и не одобрен им; код JGroups распространяется
по лицензии Apache License 2.0 и закреплён в `upstream/` для справки.

Порт совместим с JGroups на уровне протокола: узел на Rust и узел на Java одной версии входят в один кластер.
[mock-server](https://github.com/oxidestack/mock-server) использует его для кластеризации.

Статус: до первого выпуска. Крейт ещё не опубликован на crates.io; первым выпуском станет `0.1.0`.

## Что входит

Портирован не весь JGroups: в объёме — протоколы и конфигурация тех стеков, для которых крейт написан, и в этих
пределах любое отличие от поведения JGroups — ошибка.

- **Транспорты**: `TCP` и `SHARED_LOOPBACK` — транспорт внутри процесса для тестов.
- **Обнаружение (discovery)**: `TCPPING`, `DNS_PING`.
- **Членство и обнаружение сбоев**: `GMS`, `MERGE3`, `FD_ALL3`, `VERIFY_SUSPECT2`.
- **Надёжная доставка, управление потоком, фрагментация**: `NAKACK2`, `UNICAST3`, `STABLE`, `UFC`, `MFC`, `FRAG2`.
- **Конфигурация**: XML стека JGroups с подстановкой `${VAR:default}`, как её делает JGroups.

Конфигурация, в которой есть протокол вне этого списка, отклоняется с сообщением, называющим его.

## Установка

Ещё не опубликован. Когда `0.1.0` появится на crates.io, зависимость будет такой:

```toml
[dependencies]
jgroups = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

До тех пор зависьте от репозитория: `jgroups = { git = "https://github.com/oxidestack/jgroups" }`. Крейту нужен
Rust 1.88 или новее, он работает на [Tokio](https://tokio.rs).

## Пример

Два канала на транспорте внутри процесса входят в один кластер, и один отправляет другому сообщение:

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

По сети в стеке вместо этого указываются `TCP` и протокол обнаружения, как в любой конфигурации JGroups:
`<TCP bind_port="7800"/><TCPPING initial_hosts="10.0.0.1[7800],10.0.0.2[7800]"/>`.

## Документация

- Руководства на английском, русском, китайском и испанском: <https://oxidestack.github.io/jgroups/>, включая
  [обновление](docs/i18n/ru/guide/upgrading.md) между выпусками.
- Документация API: на docs.rs, когда крейт будет опубликован; до тех пор — `cargo doc --open`.
- Что изменилось в каждом выпуске: [CHANGELOG.md](CHANGELOG.md).

## Сборка и тесты

```sh
git submodule update --init upstream   # the pinned JGroups sources some tests read
cargo test
```

`make lint` запускает clippy, `make deny` проверяет лицензии и уязвимости, `make public-check` — проверка перед
публикацией, `make site` собирает сайт документации. Как внести вклад, описано в [CONTRIBUTING.md](CONTRIBUTING.md);
коммиты подписываются (DCO).

## Помощь

Вопрос о том, как работает крейт, можно сначала задать [DeepWiki](https://deepwiki.com/oxidestack/jgroups): он отвечает
по коду и документации этого репозитория, а где он расходится с документацией, права документация. Вопросы, которые он
не решил, — в [Discussions](https://github.com/oxidestack/jgroups/discussions), ошибки — в
[issues](https://github.com/oxidestack/jgroups/issues).

## Лицензия

Код: Apache-2.0 ([LICENSE](LICENSE)). Документация: CC-BY-4.0 ([LICENSE-docs](LICENSE-docs)). Что портирует крейт,
указано в [NOTICE](NOTICE).
