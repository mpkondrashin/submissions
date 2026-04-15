# Submissions Downloader (Trend Micro Deep Discovery Analyzer)

This project is a small wizard application that downloads submission data from **Trend Micro Deep Discovery Analyzer (DDAn)** and exports it to a **CSV** file.

It supports:

- **GUI mode** (wizard)
- **CLI mode** for automation

## Downloads

Prebuilt binaries are published on the **GitHub Releases** page.

- Download the zip for your platform:
  - `submissions_windows_<version>.zip`
  - `submissions_macos_<version>.zip`

After downloading:

- Extract the zip
- Run the executable inside

## Installation

### Windows

1. Download `submissions_windows_<version>.zip` from Releases.
2. Extract it to a folder (for example `C:\Tools\submissions\`).
3. Ensure the folder contains:

- `submissions.exe`
- `opengl32.dll`

4. Run `submissions.exe`.

If you move the executable, keep `opengl32.dll` in the **same directory** as `submissions.exe`.

### macOS

1. Download `submissions_macos_<version>.zip` from Releases.
2. Extract it.
3. Run the binary:

```bash
./submissions
```

If macOS blocks the binary (Gatekeeper), you can allow it in:

- System Settings
- Privacy & Security

or remove the quarantine attribute:

```bash
xattr -dr com.apple.quarantine ./submissions
```

## Usage (GUI)

Run the application with no arguments to start the wizard.

### What the wizard asks for

- **Analyzer URL** (for example `https://ddan.company.local`)
- **API key**
- **Time interval** (start/end date)
- **Output folder** and output CSV name

When the download completes, the wizard shows the generated CSV path and lets you open it.

### Saved settings

The wizard stores some settings locally (for convenience):

- Analyzer URL
- Ignore TLS setting
- Output directory

The API key is stored using the OS keyring where available.

## Usage (CLI)

CLI mode is intended for automation and headless usage.

### Required environment variable

- `ANALYZER_API_KEY` (your DDAn API key)

### Basic example

```bash
ANALYZER_API_KEY="<your_api_key>" \
  ./submissions \
  --cli \
  --analyzer-url "https://ddan.company.local" \
  --start "2026-01-01" \
  --end "2026-01-31" \
  --output "/path/to/submissions.csv"
```

### Options

- `--cli`
  - Run in CLI mode (no GUI)
- `--verbose`
  - Enable verbose DDAn SDK logging
- `--analyzer-url <url>`
  - DDAn analyzer base URL
- `--ignore-tls`
  - Ignore TLS verification errors
- `--start <YYYY-MM-DD>`
  - Start date
- `--end <YYYY-MM-DD>`
  - End date
- `--output <path>`
  - Output CSV file path (must end with `.csv`)
- `--uuid <uuid>`
  - Optional client UUID (if not provided, one is generated/saved)

## Building from source

This is a Go project.

```bash
go build -o submissions .
```

Note: the project depends on a private Go module (`github.com/mpkondrashin/ddan`). If you don’t have access, the build will fail.
