# blockchair-dl

Blazing-fast parallel downloader for [Blockchair](https://blockchair.com) blockchain data dumps.

Splits large `.tsv.gz` files into chunks and downloads them concurrently via HTTP `Range` requests, bypassing per-connection rate limits.

## Usage

```bash
# Download and auto-decompress the latest addresses dump (16 parallel connections)
blockchair-dl -c 16 -d \
  https://gz.blockchair.com/bitcoin/addresses/blockchair_bitcoin_addresses_latest.tsv.gz

# Download a specific date's outputs
blockchair-dl -c 8 \
  https://gz.blockchair.com/bitcoin/outputs/blockchair_bitcoin_outputs_20240101.tsv.gz \
  -o ./data

# Batch download from a file list
blockchair-dl urls.txt -o ./data

# Resume interrupted download
blockchair-dl --resume -c 8 <url>
```

## Options

| Flag | Description | Default |
|------|-------------|---------|
| `-c` | Parallel connections per file | `8` |
| `--min-chunk-size` | Minimum chunk size (e.g. `10MB`, `1GB`) | `10MB` |
| `-d` | Decompress `.gz` after download | off |
| `--resume` | Resume partial downloads | off |
| `--retries` | Max retries per chunk | `5` |
| `-o` | Output directory | current dir |
| `-f` | Override output filename | auto-detected |

## Installation

```bash
cargo install --git https://github.com/sinescode/blockchair-dl
```

Or build from source:

```bash
git clone https://github.com/sinescode/blockchair-dl.git
cd blockchair-dl
cargo build --release
cp target/release/blockchair-dl ~/.local/bin/
```

## How it works

1. Sends a `HEAD` request to detect file size and `Accept-Ranges` support.
2. Divides the file into chunks (min `10MB` each, up to `-c` chunks).
3. Spawns one async task per chunk, each issuing a `Range: bytes=…` GET request.
4. Retries failed chunks up to `--retries` times with exponential backoff.
5. Assembles chunks in order into the final file.
6. Optionally decompresses `.gz` output via `flate2`.

Blockchair's CDN allows many concurrent TCP connections, so `-c 16` typically yields ~160 kB/s when single-curl is throttled to 10 kB/s.

## Hardening (June 2026)

| Area | Issue | Fix |
|------|-------|-----|
| `parse_size` return | `f64` → 53‑bit precision loss for files > 900 TB | Changed to `u64` |
| Request timeout | No timeout → hang on stalled connection | Added 30s `Duration` on `ClientBuilder` |
| Chunk retry | All chunks retried even if only one failed | Retry only failed chunks |
| Partial output cleanup | Leftover `.part` files on concatenation error | Clean up partial files on error |
| Decompression cleanup | `.tsv` left behind if `.gz` concatenation fails | Clean up incomplete `.tsv` on error |
