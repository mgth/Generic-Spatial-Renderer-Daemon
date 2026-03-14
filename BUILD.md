# Building gsrd

## Platform-Specific Build Instructions

gsrd has different build profiles for different platforms. You **must** specify the appropriate feature when building.

### Linux

On Linux, use the `pipewire` feature for audio streaming output and optionally `sparta` for VBAP table generation:

```bash
# SAF_ROOT must point to the Spatial Audio Framework source tree
# (default: ../SPARTA/SDKs/Spatial_Audio_Framework — adjust if needed)
export SAF_ROOT="/path/to/Spatial_Audio_Framework"

# Full build: SPARTA + PipeWire
cargo build --release --features sparta,pipewire

# PipeWire only (no VBAP generation)
cargo build --release --features pipewire
```

Prerequisites (install via package manager):
- `libopenblas-dev` and `liblapacke-dev` (dynamic libraries linked at build time)
- SAF built as `build/framework/libsaf.a` inside `SAF_ROOT`
- `libpipewire-0.3-dev` (for PipeWire audio output)

This enables:
- Runtime VBAP table generation (with `sparta`)
- `generate-vbap` command for creating .vbap files
- PipeWire audio streaming output (with `pipewire`)
- All runtime rendering functionality

### Windows

On Windows, use the `asio` feature for ASIO audio output and optionally `sparta` (VBAP generation).

> **Prerequisites:** Building with `sparta` or `asio` requires several native dependencies (MSVC, SAF, OpenBLAS, ASIO SDK). See **[BUILDING_WINDOWS.md](BUILDING_WINDOWS.md)** for the full setup procedure.

```bash
# Full build: SPARTA + ASIO + Windows Service
export SAF_ROOT="C:/dev/SAF"
export VCPKG_ROOT="C:/dev/vcpkg"
export CPAL_ASIO_DIR="C:/dev/asio_sdk"
cargo build --release --features sparta,asio

# ASIO only (no VBAP generation)
cargo build --release --features asio
```

Full build enables:
- Runtime VBAP table generation (`generate-vbap` command)
- ASIO audio output (`--output-backend asio`, `list-asio-devices`)
- Windows Service Control Manager integration

### Building Without Platform Features

You can build without any platform-specific features:

```bash
cargo build --release
```

This minimal build:
- Can process supported bridge-provided streams
- Can load pre-generated VBAP tables
- Cannot generate VBAP tables at runtime
- No ASIO support (Windows will use default WASAPI/DirectSound)

## Workflow: VBAP + ASIO on Windows

Generate a VBAP table (Linux or Windows with `sparta`) and use it for playback with ASIO:

```bash
# Generate a VBAP table
gsrd generate-vbap \
  --speaker-layout layouts/7.1.4.yaml \
  --output 7.1.4.vbap \
  --az-res 5 \
  --el-res 5 \
  --spread-res 0.05

# List available ASIO devices
gsrd.exe list-asio-devices

# Decode with ASIO + VBAP
gsrd.exe input.thd \
  --output-backend asio \
  --asio-device-name "Your ASIO Device" \
  --enable-vbap \
  --vbap-table 7.1.4.vbap
```

## Feature Flags

| Feature | Description | Platforms |
|---------|-------------|-----------|
| `sparta` | Enable VBAP table generation via SPARTA SAF | Linux, Windows |
| `asio` | Enable ASIO audio output (requires ASIO SDK) | Windows only |
| `pipewire` | Enable PipeWire audio streaming output | Linux only |

## ASIO Devices (Windows Only)

When building with the `asio` feature, you get access to ASIO audio output and the `list-asio-devices` command.

### Listing Available ASIO Devices

```powershell
gsrd.exe list-asio-devices
```

This will show all ASIO devices installed on your system:

```
Available ASIO devices:
  1. FlexASIO
  2. ASIO4ALL V2
  3. Focusrite USB ASIO
```

### Using a Specific ASIO Device

```powershell
gsrd.exe input.thd --output-backend asio --asio-device-name "FlexASIO"
```

**Note:** The device name must match exactly as shown by `list-asio-devices`.

### Common ASIO Drivers

If you don't have any ASIO devices, install one of these:
- **FlexASIO** - Universal ASIO driver with flexible configuration
- **ASIO4ALL** - Universal ASIO driver for most audio hardware
- **Manufacturer drivers** - Check your audio interface manufacturer's website

## Troubleshooting

### "VBAP table generation not available"

This means you're trying to use runtime VBAP generation without the `sparta` feature. Either:
- Rebuild with `--features sparta` (Linux only)
- Use a pre-generated .vbap file with `--vbap-table`

### Missing ASIO options on Windows

You need to rebuild with `--features asio` to enable ASIO support.

### SPARTA build fails on Windows

SPARTA requires native dependencies (SAF, OpenBLAS). Follow the setup steps in [BUILDING_WINDOWS.md](BUILDING_WINDOWS.md).
