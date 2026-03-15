# gsrd

`gsrd` is a command-line spatial audio decoder and renderer built around a
plugin bridge architecture.

It takes an input bitstream, loads a format bridge at runtime, decodes the
stream, and can then:

- stream decoded audio to realtime outputs (`pipewire`, `asio`)
- stream audio to a realtime backend (`pipewire` on Linux, `asio` on Windows)
- emit OSC metadata and metering
- render objects to speaker feeds with VBAP

The repository also contains the rendering stack used by the binary:

- `renderer`: VBAP engine, speaker layouts, OSC output, runtime config
- `audio_output`: PipeWire and ASIO backends
- `spdif`: IEC61937 / S/PDIF parsing helpers
- `bridge_api`: ABI-stable interface for external bridge plugins
- `sys`: platform integration, including Windows service support

## Status

`gsrd` is still an engineering build. The CLI, rendering path, config system and
platform backends are usable, but the project should still be treated as alpha.

## Build

Rust `1.87.0` or newer is required.

Minimal build:

```bash
cargo build --release
```

Linux with PipeWire output:

```bash
cargo build --release --features pipewire
```

Linux or Windows with runtime VBAP table generation:

```bash
export SAF_ROOT="/path/to/Spatial_Audio_Framework"
cargo build --release --features sparta
```

Windows with ASIO output:

```bash
set CPAL_ASIO_DIR=C:\path\to\asio_sdk
cargo build --release --features asio
```

See [BUILD.md](BUILD.md) and [BUILDING_WINDOWS.md](BUILDING_WINDOWS.md) for the
full dependency setup.

## Core Model

`gsrd` does not hardcode a single container or codec frontend in the binary
itself. Decoding is delegated to a bridge plugin loaded at runtime.

Bridge lookup order:

1. `--bridge-path <FILE>`
2. `render.bridge_path` in the config file
3. first `lib*_bridge.so`, `lib*_bridge.dll` or `lib*_bridge.dylib` found next
   to the executable

Without a bridge plugin, `gsrd` will not start.

## Commands

`gsrd` currently exposes these commands:

- default command: render an input stream to a realtime backend
- `generate-vbap`: generate a binary VBAP table from a speaker layout
- `list-asio-devices`: list available ASIO output devices on Windows builds

Inspect the exact CLI supported by your build with:

```bash
gsrd --help
gsrd --help
```

## Render Workflow

The default runtime flow can combine several subsystems in one run:

- input from a file, `stdin`, or a continuous stream
- dynamic bridge loading
- optional VBAP rendering from a YAML speaker layout
- realtime output backend
- OSC metadata broadcast and registration listener
- optional OSC metering
- config persistence through `--save-config`

Typical examples:

```bash
# Decode from stdin
cat input.bin | gsrd - --bridge-path ./libformat_bridge.so

# Linux realtime output via PipeWire
gsrd input.bin \
  --bridge-path ./libformat_bridge.so \
  --output-backend pipewire \
  --sink gsrd_router

# Enable VBAP rendering and OSC output
gsrd input.bin \
  --bridge-path ./libformat_bridge.so \
  --enable-vbap \
  --speaker-layout layouts/7.1.4.yaml \
  --osc \
  --osc-host 127.0.0.1 \
  --osc-port 9000
```

## VBAP

Speaker layouts live in [`layouts/`](layouts/) and are used either directly at
runtime or to precompute a `.vbap` table.

Generate a table:

```bash
gsrd generate-vbap \
  --speaker-layout layouts/7.1.4.yaml \
  --output 7.1.4.vbap \
  --az-res 2 \
  --el-res 2 \
  --spread-res 0.25
```

Use a precomputed table:

```bash
gsrd input.bin \
  --bridge-path ./libformat_bridge.so \
  --enable-vbap \
  --vbap-table ./7.1.4.vbap
```

## Configuration

Global and render settings can be loaded from a YAML config file.

Default path:

- Linux: `~/.config/gsrd/config.yaml`
- Windows: `%APPDATA%\\gsrd\\config.yaml`

You can point to another file with `--config`, and persist the current effective
settings with `--save-config`.

Example:

```yaml
global:
  loglevel: info
  log_format: plain

render:
  bridge_path: /opt/gsrd/plugins/libformat_bridge.so
  enable_vbap: true
  speaker_layout: /opt/gsrd/layouts/7.1.4.yaml
  osc: true
  osc_host: 127.0.0.1
  osc_port: 9000
```

## Repository Pointers

- [BUILD.md](BUILD.md): build profiles and feature flags
- [OSC_PROTOCOL.md](OSC_PROTOCOL.md): OSC message surface
- [QUICKSTART.md](QUICKSTART.md): local bring-up notes
- [layouts/README.md](layouts/README.md): speaker layout format

## Related Tools

- [SpatialVisualizer](https://github.com/mgth/SpatialVisualizer): companion UI
  for `gsrd`, used to inspect spatial metadata, monitor renderer state, and
  send live OSC control changes. `gsrd` provides the realtime decode/render
  engine; `SpatialVisualizer` is the supervision and control surface on top.

## License

GNU GPL v3. See [LICENSE](LICENSE).
