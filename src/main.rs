use anyhow::{Context, Result, bail};
use bytesize::ByteSize;
use clap::Parser;
use futures_util::StreamExt;
use indicatif::{HumanDuration, MultiProgress, ProgressBar, ProgressStyle};
use reqwest::header::{HeaderMap, HeaderValue, RANGE};
use std::cmp::min;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[derive(Parser)]
#[command(
    name = "blockchair-dl",
    version,
    about = "Blazing-fast Blockchair data downloader with parallel chunking & resume"
)]
struct Args {
    #[arg(required = true)]
    sources: Vec<String>,

    #[arg(short, long)]
    output: Option<PathBuf>,

    #[arg(short, long, default_value = "8")]
    connections: usize,

    #[arg(long, default_value = "10MB")]
    min_chunk_size: String,

    #[arg(long, default_value = "5")]
    retries: usize,

    #[arg(short, long)]
    decompress: bool,

    #[arg(long)]
    resume: bool,

    #[arg(short, long)]
    filename: Option<String>,
}

#[derive(Clone)]
struct DownloadConfig {
    connections: usize,
    min_chunk_size: u64,
    retries: usize,
    decompress: bool,
    resume: bool,
}

struct DownloadState {
    bytes_downloaded: Arc<AtomicU64>,
    progress: ProgressBar,
}

impl DownloadState {
    fn new(progress: ProgressBar) -> Self {
        Self {
            bytes_downloaded: Arc::new(AtomicU64::new(0)),
            progress,
        }
    }

    fn add(&self, n: u64) {
        self.bytes_downloaded.fetch_add(n, Ordering::Relaxed);
        self.progress.inc(n);
    }
}

fn parse_size(s: &str) -> Result<u64> {
    let s = s.trim().to_uppercase();
    let (num_str, unit) = if s.ends_with("GB") {
        (&s[..s.len() - 2], 1_000_000_000u64)
    } else if s.ends_with("MB") {
        (&s[..s.len() - 2], 1_000_000u64)
    } else if s.ends_with("KB") {
        (&s[..s.len() - 2], 1_000u64)
    } else if s.ends_with("B") {
        (&s[..s.len() - 1], 1u64)
    } else {
        (s.trim(), 1u64)
    };
    let num: f64 = num_str.parse().context("Invalid size format")?;
    Ok((num * unit as f64) as u64)
}

async fn head_request(client: &reqwest::Client, url: &str, retries: usize) -> Result<(u64, bool)> {
    let mut last_err = None;
    for attempt in 0..=retries {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_secs(2u64.pow(attempt as u32))).await;
        }
        match client.head(url).send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    last_err = Some(anyhow::anyhow!("HEAD {} returned {}", url, resp.status()));
                    continue;
                }
                let headers = resp.headers();
                let length = headers
                    .get(reqwest::header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok());
                let accepts_range = headers
                    .get("accept-ranges")
                    .and_then(|v| v.to_str().ok())
                    .map(|v| v.contains("bytes"))
                    .unwrap_or(false);
                if let Some(len) = length {
                    return Ok((len, accepts_range));
                }
                last_err = Some(anyhow::anyhow!("HEAD {} missing Content-Length", url));
            }
            Err(e) => {
                last_err = Some(anyhow::anyhow!("HEAD {} failed: {}", url, e));
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("HEAD failed after {} retries", retries)))
}

fn build_client() -> Result<reqwest::Client> {
    let mut headers = HeaderMap::new();
    headers.insert(
        "User-Agent",
        HeaderValue::from_static("Mozilla/5.0 (X11; Linux x86_64) blockchair-dl/0.1"),
    );
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .no_deflate()
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .connect_timeout(Duration::from_secs(30))
        .build()?;
    Ok(client)
}

#[allow(clippy::too_many_arguments)]
async fn stream_chunk_to_file(
    client: &reqwest::Client,
    url: &str,
    start_byte: u64,
    end_byte: u64,
    path: &Path,
    retries: usize,
    state: &DownloadState,
    require_206: bool,
) -> Result<()> {
    let range_header = format!("bytes={}-{}", start_byte, end_byte);
    let expected_len = end_byte - start_byte + 1;
    let mut last_err = None;

    for attempt in 0..=retries {
        if attempt > 0 {
            let delay = Duration::from_secs(2u64.pow(attempt as u32));
            tokio::time::sleep(delay).await;
        }

        let mut req_headers = HeaderMap::new();
        req_headers.insert(RANGE, HeaderValue::from_str(&range_header)?);

        match client.get(url).headers(req_headers).send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                if require_206 && status != 206 {
                    last_err =
                        Some(anyhow::anyhow!("HTTP {} (expected 206 Partial Content)", status));
                    continue;
                }
                if !resp.status().is_success() {
                    last_err = Some(anyhow::anyhow!("HTTP {}", status));
                    continue;
                }
                let mut f = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(path)
                    .await?;
                let mut stream = resp.bytes_stream();
                let mut received: u64 = 0;
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk.with_context(|| {
                        format!("Failed to read response body for range {}-{}", start_byte, end_byte)
                    })?;
                    f.write_all(&chunk).await?;
                    let len = chunk.len() as u64;
                    received += len;
                    state.add(len);
                }
                if received != expected_len {
                    bail!(
                        "Expected {} bytes for range {}-{}, got {}",
                        expected_len,
                        start_byte,
                        end_byte,
                        received
                    );
                }
                f.flush().await?;
                f.sync_all().await?;
                return Ok(());
            }
            Err(e) => {
                last_err = Some(anyhow::anyhow!(e));
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("Download failed after {} retries", retries)))
}

async fn concatenate_chunks(output_path: &Path, chunk_files: &[PathBuf]) -> Result<()> {
    let mut out = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(output_path)
        .await?;

    let mut buf = vec![0u8; 1_048_576];
    for cf in chunk_files {
        let mut f = File::open(cf).await?;
        loop {
            let n = f.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            out.write_all(&buf[..n]).await?;
        }
    }
    out.flush().await?;
    out.sync_all().await?;
    Ok(())
}

fn decompress_gz(input: &Path, output: &Path) -> Result<()> {
    let mut file_in = std::fs::File::open(input)?;
    let mut decoder = flate2::read::GzDecoder::new(&mut file_in);
    let mut file_out = std::fs::File::create(output)?;
    let mut buf = [0u8; 262144];
    loop {
        let n = decoder.read(&mut buf)?;
        if n == 0 {
            break;
        }
        file_out.write_all(&buf[..n])?;
    }
    file_out.flush()?;
    Ok(())
}

fn expand_sources(sources: &[String]) -> Result<Vec<String>> {
    let mut urls = Vec::new();
    for src in sources {
        let lower = src.to_lowercase();
        if lower.starts_with("http://") || lower.starts_with("https://") {
            urls.push(src.clone());
        } else {
            let content = std::fs::read_to_string(src)
                .with_context(|| format!("Failed to read source file: {}", src))?;
            for line in content.lines() {
                let line = line.trim();
                if !line.is_empty() && !line.starts_with('#') {
                    urls.push(line.to_string());
                }
            }
        }
    }
    Ok(urls)
}

fn url_filename(url: &str) -> String {
    if let Ok(parsed) = url::Url::parse(url) {
        parsed
            .path_segments()
            .and_then(|mut s| s.next_back())
            .unwrap_or("download")
            .to_string()
    } else {
        url.rsplit('/')
            .next()
            .unwrap_or("download")
            .split('?')
            .next()
            .unwrap_or("download")
            .to_string()
    }
}

struct FileTask<'a> {
    client: &'a reqwest::Client,
    url: &'a str,
    output_dir: &'a Path,
    config: &'a DownloadConfig,
    mp: &'a MultiProgress,
    file_index: usize,
    file_total: usize,
    override_filename: Option<&'a str>,
}

async fn download_file(task: FileTask<'_>) -> Result<()> {
    let (total_size, supports_range) =
        match head_request(task.client, task.url, task.config.retries).await {
            Ok(r) => r,
            Err(e) => {
                eprintln!("HEAD failed ({}), falling back to single-connection download", e);
                let resp = task.client.get(task.url).send().await?;
                let total_size = resp
                    .headers()
                    .get(reqwest::header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok())
                    .and_then(|v| v.parse::<u64>().ok())
                    .context("No Content-Length in GET response")?;
                let filename = task
                    .override_filename
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| url_filename(task.url));
                let output_path = task.output_dir.join(&filename);
                let pb = task.mp.add(ProgressBar::new(total_size));
                pb.set_style(
                    ProgressStyle::default_bar()
                        .template("{prefix:40.cyan.bold} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
                        .unwrap()
                        .progress_chars("##-"),
                );
                pb.set_prefix(filename.clone());
                let state = DownloadState::new(pb.clone());
                let mut f = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&output_path)
                    .await?;
                let mut stream = resp.bytes_stream();
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk?;
                    f.write_all(&chunk).await?;
                    state.add(chunk.len() as u64);
                }
                f.flush().await?;
                f.sync_all().await?;
                pb.finish_and_clear();
                task.mp.remove(&pb);
                return Ok(());
            }
        };

    if total_size == 0 {
        let filename = task
            .override_filename
            .map(|s| s.to_string())
            .unwrap_or_else(|| url_filename(task.url));
        let output_path = task.output_dir.join(&filename);
        File::create(&output_path).await?;
        return Ok(());
    }

    let filename = task
        .override_filename
        .map(|s| s.to_string())
        .unwrap_or_else(|| url_filename(task.url));
    let output_path = task.output_dir.join(&filename);
    let partial_dir = task.output_dir.join(format!(".{}.parts", filename));

    let chunk_count = if supports_range && task.config.connections > 1 {
        let ideal = total_size / task.config.min_chunk_size;
        min(ideal, task.config.connections as u64).max(1) as usize
    } else {
        1
    };

    let chunk_size = total_size / chunk_count as u64;
    let header = if task.file_total > 1 {
        format!("[{}/{}] {}", task.file_index, task.file_total, &filename)
    } else {
        filename.clone()
    };

    let pb = task.mp.add(ProgressBar::new(total_size));
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{prefix:40.cyan.bold} [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})")
            .unwrap()
            .progress_chars("##-"),
    );
    pb.set_prefix(header);

    if supports_range && chunk_count > 1 {
        tokio::fs::create_dir_all(&partial_dir).await?;

        let mut handles = Vec::new();
        for i in 0..chunk_count {
            let start = i as u64 * chunk_size;
            let end = if i == chunk_count - 1 {
                total_size - 1
            } else {
                (i as u64 + 1) * chunk_size - 1
            };
            let chunk_path = partial_dir.join(format!("part-{:06}", i));

            if task.config.resume && chunk_path.exists() {
                let metadata = match tokio::fs::metadata(&chunk_path).await {
                    Ok(m) => m,
                    Err(_) => continue,
                };
                let existing_len = metadata.len();
                if existing_len == (end - start + 1) {
                    pb.inc(existing_len);
                    continue;
                }
            }

            let client = task.client.clone();
            let url = task.url.to_string();
            let chunk_path2 = chunk_path.clone();
            let retries = task.config.retries;
            let state = DownloadState::new(pb.clone());

            handles.push(tokio::spawn(async move {
                stream_chunk_to_file(
                    &client, &url, start, end, &chunk_path2, retries, &state, true,
                )
                .await
            }));
        }

        let results: Vec<Result<()>> =
            futures_util::future::join_all(handles).await.into_iter().map(|r| match r {
                Ok(inner) => inner,
                Err(e) => Err(anyhow::anyhow!("Task panicked: {}", e)),
            }).collect();

        let mut failed = false;
        for r in &results {
            if let Err(e) = r {
                failed = true;
                eprintln!("Chunk download failed ({}), falling back to single connection", e);
                break;
            }
        }

        if failed {
            let _ = tokio::fs::remove_dir_all(&partial_dir).await;
            pb.reset();
            pb.set_length(total_size);
            let state = DownloadState::new(pb.clone());
            stream_chunk_to_file(
                task.client, task.url, 0, total_size - 1, &output_path,
                task.config.retries, &state, false,
            )
            .await?;
            pb.finish_and_clear();
            task.mp.remove(&pb);
            return Ok(());
        }

        pb.finish_and_clear();

        let mut chunk_files: Vec<PathBuf> = (0..chunk_count)
            .map(|i| partial_dir.join(format!("part-{:06}", i)))
            .collect();
        chunk_files.sort();

        if let Err(e) = concatenate_chunks(&output_path, &chunk_files).await {
            let _ = tokio::fs::remove_dir_all(&partial_dir).await;
            return Err(e);
        }

        tokio::fs::remove_dir_all(&partial_dir).await?;
        task.mp.remove(&pb);
    } else {
        let state = DownloadState::new(pb.clone());
        stream_chunk_to_file(
            task.client,
            task.url,
            0,
            total_size - 1,
            &output_path,
            task.config.retries,
            &state,
            false,
        )
        .await?;
        pb.finish_and_clear();
        task.mp.remove(&pb);
    }

    if task.config.decompress && filename.ends_with(".gz") {
        let decompressed_path = output_path.with_extension("");
        let start = Instant::now();
        let decomp_pb = task.mp.add(ProgressBar::new_spinner());
        decomp_pb.set_style(
            ProgressStyle::default_spinner()
                .template("{spinner:.green} {prefix} ({elapsed})")
                .unwrap(),
        );
        decomp_pb.set_prefix(format!("Decompressing {}", filename));

        let input = output_path.clone();
        let output = decompressed_path.clone();
        let result = tokio::task::spawn_blocking(move || decompress_gz(&input, &output)).await;

        match result {
            Ok(Ok(())) => {
                tokio::fs::remove_file(&output_path).await?;
                decomp_pb.finish_with_message(format!(
                    "Decompressed {} -> {} in {}",
                    ByteSize::b(total_size),
                    ByteSize::b(
                        std::fs::metadata(&decompressed_path)
                            .map(|m| m.len())
                            .unwrap_or(0)
                    ),
                    HumanDuration(start.elapsed())
                ));
            }
            Ok(Err(e)) => {
                let _ = tokio::fs::remove_file(&decompressed_path).await;
                decomp_pb.finish_with_message("Decompression failed");
                task.mp.remove(&decomp_pb);
                return Err(e);
            }
            Err(e) => {
                let _ = tokio::fs::remove_file(&decompressed_path).await;
                decomp_pb.finish_with_message("Decompression panicked");
                task.mp.remove(&decomp_pb);
                return Err(anyhow::anyhow!("Decompression task failed: {}", e));
            }
        }
        task.mp.remove(&decomp_pb);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    if args.sources.len() > 1 && args.filename.is_some() {
        bail!("--filename can only be used with a single URL");
    }

    let config = DownloadConfig {
        connections: args.connections,
        min_chunk_size: parse_size(&args.min_chunk_size)?,
        retries: args.retries,
        decompress: args.decompress,
        resume: args.resume,
    };

    let output_dir = args
        .output
        .unwrap_or_else(|| std::env::current_dir().unwrap());
    tokio::fs::create_dir_all(&output_dir).await?;

    let urls = expand_sources(&args.sources)?;
    if urls.is_empty() {
        bail!("No URLs to download");
    }

    let client = build_client()?;
    let mp = MultiProgress::new();
    let total_files = urls.len();

    for (file_index, url) in urls.iter().enumerate() {
        let file_index = file_index + 1;
        let override_name = if total_files == 1 {
            args.filename.as_deref()
        } else {
            None
        };
        if let Err(e) = download_file(FileTask {
            client: &client,
            url,
            output_dir: &output_dir,
            config: &config,
            mp: &mp,
            file_index,
            file_total: total_files,
            override_filename: override_name,
        })
        .await
        {
            eprintln!("\nError downloading {}: {:#}", url, e);
            if args.sources.len() == 1 {
                std::process::exit(1);
            }
        }
    }

    Ok(())
}
