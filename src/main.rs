use anyhow::{Context, Result, bail};
use bytesize::ByteSize;
use clap::Parser;
use indicatif::{HumanDuration, MultiProgress, ProgressBar, ProgressStyle};
use reqwest::header::{ACCEPT_ENCODING, HeaderMap, HeaderValue, RANGE};
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
    /// URL(s) to download (or paths to .txt files containing URLs)
    #[arg(required = true)]
    sources: Vec<String>,

    /// Output directory (default: current dir)
    #[arg(short, long)]
    output: Option<PathBuf>,

    /// Number of parallel connections per file
    #[arg(short, long, default_value = "8")]
    connections: usize,

    /// Minimum chunk size in bytes (e.g. 10MB, 1GB)
    #[arg(long, default_value = "10MB")]
    min_chunk_size: String,

    /// Max retries per chunk
    #[arg(long, default_value = "5")]
    retries: usize,

    /// Decompress .gz files after download (removes .gz extension)
    #[arg(short, long)]
    decompress: bool,

    /// Resume partial downloads (checks for existing files)
    #[arg(long)]
    resume: bool,

    /// Override filename (only valid with single URL)
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
}

impl DownloadState {
    fn new() -> Self {
        Self {
            bytes_downloaded: Arc::new(AtomicU64::new(0)),
        }
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

async fn head_request(client: &reqwest::Client, url: &str) -> Result<(u64, bool)> {
    let resp = client.head(url).send().await?;
    if !resp.status().is_success() {
        bail!("HEAD {} returned {}", url, resp.status());
    }
    let headers = resp.headers();
    let length = headers
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .context("Missing Content-Length")?;
    let accepts_range = headers
        .get("accept-ranges")
        .and_then(|v| v.to_str().ok())
        .map(|v| v.contains("bytes"))
        .unwrap_or(false);
    Ok((length, accepts_range))
}

fn build_client() -> Result<reqwest::Client> {
    let mut headers = HeaderMap::new();
    headers.insert(
        "User-Agent",
        HeaderValue::from_static("Mozilla/5.0 (X11; Linux x86_64) blockchair-dl/0.1"),
    );
    headers.insert(ACCEPT_ENCODING, HeaderValue::from_static("identity"));
    let client = reqwest::Client::builder()
        .default_headers(headers)
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .timeout(Duration::from_secs(300))
        .connect_timeout(Duration::from_secs(30))
        .build()?;
    Ok(client)
}

async fn download_chunk(
    client: &reqwest::Client,
    url: &str,
    start_byte: u64,
    end_byte: u64,
    retries: usize,
    state: &DownloadState,
) -> Result<Vec<u8>> {
    let range_header = format!("bytes={}-{}", start_byte, end_byte);
    let mut last_err = None;

    for attempt in 0..=retries {
        if attempt > 0 {
            let delay = Duration::from_secs(2u64.pow(attempt as u32));
            tokio::time::sleep(delay).await;
        }

        let mut headers = HeaderMap::new();
        headers.insert(RANGE, HeaderValue::from_str(&range_header)?);

        match client.get(url).headers(headers).send().await {
            Ok(resp) => {
                if !resp.status().is_success() && resp.status().as_u16() != 206 {
                    last_err = Some(anyhow::anyhow!("HTTP {}", resp.status()));
                    continue;
                }
                let mut buf = Vec::with_capacity((end_byte - start_byte + 1) as usize);
                let mut stream = resp.bytes_stream();
                use futures_util::StreamExt;
                while let Some(chunk) = stream.next().await {
                    let chunk = chunk?;
                    state
                        .bytes_downloaded
                        .fetch_add(chunk.len() as u64, Ordering::Relaxed);
                    buf.extend_from_slice(&chunk);
                }
                return Ok(buf);
            }
            Err(e) => {
                last_err = Some(anyhow::anyhow!(e));
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("Download failed after {} retries", retries)))
}

async fn concatenate_chunks(
    output_path: &Path,
    chunk_files: &[PathBuf],
    _total_size: u64,
) -> Result<()> {
    let mut out = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(output_path)
        .await?;

    for cf in chunk_files {
        let mut f = File::open(cf).await?;
        let mut buf = Vec::with_capacity(1_048_576);
        let n = f.read_to_end(&mut buf).await?;
        if n > 0 {
            out.write_all(&buf).await?;
        }
    }
    out.flush().await?;
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
        if src.starts_with("http://") || src.starts_with("https://") {
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
    url.rsplit('/')
        .next()
        .unwrap_or("download")
        .split('?')
        .next()
        .unwrap_or("download")
        .to_string()
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
    let (total_size, supports_range) = head_request(task.client, task.url).await?;

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

    let state = Arc::new(DownloadState::new());

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
                let metadata = tokio::fs::metadata(&chunk_path).await?;
                let existing_len = metadata.len();
                if existing_len == (end - start + 1) {
                    state
                        .bytes_downloaded
                        .fetch_add(existing_len, Ordering::Relaxed);
                    pb.inc(existing_len);
                    continue;
                }
            }

            let client = task.client.clone();
            let url = task.url.to_string();
            let state = state.clone();
            let chunk_path2 = chunk_path.clone();
            let retries = task.config.retries;

            handles.push(tokio::spawn(async move {
                let data = download_chunk(&client, &url, start, end, retries, &state).await?;
                let mut f = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&chunk_path2)
                    .await?;
                f.write_all(&data).await?;
                f.flush().await?;
                Ok::<_, anyhow::Error>(())
            }));
        }

        for handle in handles {
            handle.await??;
        }

        pb.finish_and_clear();

        let mut chunk_files: Vec<PathBuf> = (0..chunk_count)
            .map(|i| partial_dir.join(format!("part-{:06}", i)))
            .collect();
        chunk_files.sort();

        concatenate_chunks(&output_path, &chunk_files, total_size).await?;

        tokio::fs::remove_dir_all(&partial_dir).await?;
    } else {
        let data = download_chunk(
            task.client,
            task.url,
            0,
            total_size - 1,
            task.config.retries,
            &state,
        )
        .await?;
        pb.finish_and_clear();

        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&output_path)
            .await?;
        f.write_all(&data).await?;
        f.flush().await?;
    }

    task.mp.remove(&pb);

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

        decompress_gz(&output_path, &decompressed_path)?;

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
