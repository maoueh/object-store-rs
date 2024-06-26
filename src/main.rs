use anyhow::Context;
use futures_util::TryStreamExt;
use object_store_rs::store;
use std::{
    env,
    time::{Duration, Instant},
};

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let arguments = env::args().collect::<Vec<String>>();
    if arguments.len() != 3 {
        eprintln!(
            "Usage: {} <merged_blocks_store_url> <block_offset>",
            arguments.first().expect("arguments is len 3")
        );
        std::process::exit(1);
    }

    let blocks_store = store::new(arguments.get(1).expect("arguments is len 3"))?;
    let block_offset = read_block_offset(arguments.get(2).expect("arguments is len 3"))?;

    let start = std::time::Instant::now();
    let test_duration = Duration::from_secs(120);
    let mut total_bytes = 0;

    let mut total_fetch_time = Duration::new(0, 0);
    let mut total_fetch_count = 0;

    let mut window_start = Instant::now();
    let window_period = Duration::from_secs(5);
    let mut window_bytes = 0;

    println!(
        "Starting read test (transfer rate will be printed each {:?})",
        window_period,
    );

    for i in 0..1000 {
        let block_number = (i * 100) + block_offset;
        let filename = format!("{:010}.dbin.zst", block_number);

        let open_start = Instant::now();
        let mut reader = blocks_store
            .object_reader(&filename)
            .await
            .context(format!("read file {}", filename))?;

        total_fetch_time += open_start.elapsed();
        total_fetch_count += 1;

        while let Some(item) = reader.try_next().await? {
            if window_start.elapsed() > window_period {
                println!("{}", bytes_rate(window_bytes, window_period));
                window_bytes = 0;
                window_start = Instant::now();
            }

            total_bytes += item.len();
            window_bytes += item.len();
        }

        if start.elapsed() > test_duration {
            break;
        }
    }

    println!();
    println!(
        "Overall average fetch time: {:?} ({} fetches, {:.2}% of total time)",
        total_fetch_time / total_fetch_count,
        total_fetch_count,
        percentage_of_total_time(total_fetch_time, test_duration)
    );
    println!(
        "Overall transfer rate: {} ({} bytes in {:?})",
        bytes_rate(total_bytes, test_duration),
        total_bytes,
        start.elapsed()
    );

    Ok(())
}

fn percentage_of_total_time(counter: Duration, total: Duration) -> f64 {
    counter.as_secs_f64() / total.as_secs_f64() * 100.0
}

fn read_block_offset(input: &str) -> Result<u64, anyhow::Error> {
    input
        .parse()
        .with_context(|| format!("Invalid block offset: {}", input))
}

fn bytes_rate(byte_count: usize, period: Duration) -> String {
    let rate = byte_count as f64 / period.as_secs_f64();

    format!(
        "{}/s",
        humansize::format_size(rate as u64, humansize::BINARY)
    )
}
