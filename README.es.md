# jgroups

<!-- languages:begin -->
[English](README.md) · [Русский](README.ru.md) · [中文](README.zh-CN.md) · **Español**
<!-- languages:end -->
<!-- translation of README.md: 6e486bef40db -->

Un port no oficial a Rust de [JGroups](https://github.com/belaban/JGroups) 5.2.28.Final, el conjunto de herramientas
Java para la comunicación fiable en clúster: canales, pertenencia al grupo (GMS), detección de fallos, fiabilidad de
los mensajes y el transporte TCP. No está afiliado al proyecto JGroups ni cuenta con su respaldo; el código de JGroups
se distribuye bajo la Apache License 2.0 y está fijado en `upstream/` como referencia.

Está hecho para interoperar con JGroups a nivel de protocolo: un nodo Rust y un nodo Java de la misma versión se unen
a un mismo clúster. [mock-server](https://github.com/oxidestack/mock-server) lo usa para su modo en clúster.

Estado: previo a la primera versión. El crate todavía no está publicado en crates.io; su primera versión será `0.1.0`.

## Qué cubre

No se ha portado todo JGroups: el alcance son los protocolos y la configuración de las pilas para las que se escribió,
y dentro de ese alcance cualquier diferencia con el comportamiento de JGroups es un error.

- **Transportes**: `TCP` y `SHARED_LOOPBACK`, un transporte dentro del proceso para pruebas.
- **Descubrimiento (discovery)**: `TCPPING`, `DNS_PING`.
- **Pertenencia y detección de fallos**: `GMS`, `MERGE3`, `FD_ALL3`, `VERIFY_SUSPECT2`.
- **Entrega fiable, control de flujo, fragmentación**: `NAKACK2`, `UNICAST3`, `STABLE`, `UFC`, `MFC`, `FRAG2`.
- **Configuración**: el XML de pila de JGroups, con la sustitución `${VAR:default}` tal como la hace JGroups.

Una configuración que nombra un protocolo fuera de esta lista se rechaza con un mensaje que lo nombra.

## Instalación

Todavía no publicado. Cuando `0.1.0` esté en crates.io, la dependencia será:

```toml
[dependencies]
jgroups = "0.1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Hasta entonces, dependa del repositorio: `jgroups = { git = "https://github.com/oxidestack/jgroups" }`. El crate
necesita Rust 1.88 o posterior y funciona sobre [Tokio](https://tokio.rs).

## Ejemplo

Dos canales sobre el transporte dentro del proceso se unen a un mismo clúster y uno envía un mensaje al otro:

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

En red, la pila nombra en su lugar `TCP` y un protocolo de descubrimiento, como cualquier configuración de JGroups:
`<TCP bind_port="7800"/><TCPPING initial_hosts="10.0.0.1[7800],10.0.0.2[7800]"/>`.

## Documentación

- Las guías, en inglés, ruso, chino y español: <https://oxidestack.github.io/jgroups/>, con la
  [actualización](docs/i18n/es/guide/upgrading.md) entre versiones.
- La documentación de la API: en docs.rs cuando el crate se publique; hasta entonces, `cargo doc --open`.
- Qué cambió en cada versión: [CHANGELOG.md](CHANGELOG.md).

## Compilar y probar

```sh
git submodule update --init upstream   # the pinned JGroups sources some tests read
cargo test
```

`make lint` ejecuta clippy, `make deny` comprueba licencias y avisos de seguridad, `make public-check` es la
comprobación previa a la publicación y `make site` construye el sitio de documentación.
[CONTRIBUTING.md](CONTRIBUTING.md) explica cómo contribuir; los commits llevan sign-off (DCO).

## Ayuda

Una pregunta sobre cómo funciona el crate puede ir primero a [DeepWiki](https://deepwiki.com/oxidestack/jgroups):
responde a partir del código y la documentación de este repositorio, y donde no coincide con la documentación, la
documentación tiene razón. Las preguntas que no resuelva van a
[Discussions](https://github.com/oxidestack/jgroups/discussions), y los errores a
[issues](https://github.com/oxidestack/jgroups/issues).

## Licencia

Código: Apache-2.0 ([LICENSE](LICENSE)). Documentación: CC-BY-4.0 ([LICENSE-docs](LICENSE-docs)). Lo que porta el
crate figura en [NOTICE](NOTICE).
