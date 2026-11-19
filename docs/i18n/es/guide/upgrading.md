# Actualización

<!-- languages:begin -->
[English](../../../guide/upgrading.md) · [Русский](../../ru/guide/upgrading.md) · [中文](../../zh/guide/upgrading.md) · **Español**
<!-- languages:end -->
<!-- translation of docs/guide/upgrading.md: ec4fcb04d2e5 -->

Cómo pasar un proyecto de la versión de jgroups de la que depende a una más reciente. Busque la sección de cada
versión posterior a la suya y aplíquelas de la más antigua a la más reciente: pasar de 0.1.0 a una versión posterior
significa aplicar la sección de cada versión intermedia y luego la de la versión que quiere. Una versión que no
requiere cambios lo dice. Después, [actualice](#cómo-actualizar) a la versión más reciente en un solo paso: nunca hace
falta instalar antes una versión intermedia.

Cada sección enumera lo que la versión pide a su código o a su clúster: en **Action required**, un cambio que hay que
hacer antes de la actualización o junto con ella; en **Deprecated**, algo que todavía funciona y dejará de hacerlo en
una versión posterior; en **Removed**, algo que ya ha dejado de funcionar. El
[registro de cambios](https://github.com/oxidestack/jgroups/blob/main/CHANGELOG.md) recoge todo lo demás que cambió
cada versión. Solo las versiones tienen sección: una versión candidata (`-rc.N`) se describe en su release de GitHub, y
lo que cambia se escribe aquí bajo la versión que lo incluye.

La documentación de la versión que usa también está en este sitio: el selector de versión de la parte superior de
cada página cambia de una versión a otra, y una página de una versión antigua enlaza la misma página en la más reciente.

## Cómo actualizar

Las versiones siguen el [versionado semántico](https://semver.org/lang/es/spec/v2.0.0.html). Antes de 1.0, una nueva
versión menor (de 0.1 a 0.2) puede cambiar la API; una nueva versión de parche (de 0.1.0 a 0.1.1), no.

1. Lea las secciones siguientes de cada versión posterior a la suya y aplique sus cambios a su código y a sus
   configuraciones de pila.
2. Cambie la versión en su `Cargo.toml` (`jgroups = "0.2"`) o, para una versión de parche, ejecute
   `cargo update -p jgroups`.
3. Ejecute `cargo build` y sus pruebas. Un cambio de **Action required** que el compilador puede detectar aparece
   aquí; los demás son de comportamiento, y la sección dice cómo comprobarlos.
4. Un clúster se actualiza nodo a nodo. Cada versión habla el formato de transmisión (wire format) de la versión de
   JGroups que porta, de modo que sus nodos, los de la versión anterior y los nodos Java de esa versión de JGroups
   forman un mismo clúster. Una versión que porta otra versión de JGroups lo dice en **Action required**, con lo que
   eso significa para un clúster mixto.

## Actualizar a X.Y.Z (primera versión, número y fecha aún sin fijar)

<!-- Placeholder: replace "X.Y.Z" in this heading with the first release's number when it is cut (expected 0.1.0).
     Every later release gets its own section above this one. -->

La primera versión de jgroups: un port del canal, la pertenencia al grupo, la detección de fallos, la entrega fiable y
el transporte TCP de JGroups 5.2.28.Final, interoperable a nivel de protocolo con los nodos Java de esa versión. No hay
nada desde lo que actualizar.

### Action required

Nada.

### Deprecated

Nada.

### Removed

Nada.
