use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod, Runtime};
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio_postgres::NoTls;

type BenchmarkError = Box<dyn Error + Send + Sync>;

struct Measurement {
    durations: Vec<Duration>,
    elapsed: Duration,
}

impl Measurement {
    fn new() -> Self {
        Self {
            durations: Vec::new(),
            elapsed: Duration::ZERO,
        }
    }

    fn add(&mut self, mut measurement: Self) {
        self.durations.append(&mut measurement.durations);
        self.elapsed += measurement.elapsed;
    }

    fn percentile(&mut self, percentile: usize) -> Duration {
        self.durations.sort_unstable();
        let index = (self.durations.len() - 1) * percentile / 100;
        self.durations[index]
    }

    fn operations_per_second(&self) -> f64 {
        self.durations.len() as f64 / self.elapsed.as_secs_f64()
    }
}

fn parse_argument(name: &str, position: usize, default: usize) -> Result<usize, BenchmarkError> {
    let Some(value) = std::env::args().nth(position) else {
        return Ok(default);
    };
    let value = value.parse::<usize>()?;
    if value == 0 {
        return Err(format!("{name} must be greater than zero").into());
    }
    Ok(value)
}

fn create_pool(
    database_url: &str,
    max_size: usize,
    recycling_method: RecyclingMethod,
) -> Result<Pool, BenchmarkError> {
    let postgres_config = database_url.parse()?;
    let manager = Manager::from_config(postgres_config, NoTls, ManagerConfig { recycling_method });
    Ok(Pool::builder(manager)
        .max_size(max_size)
        .runtime(Runtime::Tokio1)
        .build()?)
}

async fn run_operations(
    pool: &Pool,
    operation_count: usize,
    concurrency: usize,
) -> Result<Measurement, BenchmarkError> {
    let next_operation = Arc::new(AtomicUsize::new(0));
    let started = Instant::now();
    let mut workers = Vec::with_capacity(concurrency);

    for _ in 0..concurrency {
        let pool = pool.clone();
        let next_operation = Arc::clone(&next_operation);

        workers.push(tokio::spawn(async move {
            let mut durations = Vec::new();

            loop {
                let operation = next_operation.fetch_add(1, Ordering::Relaxed);
                if operation >= operation_count {
                    break;
                }

                let started = Instant::now();
                let client = pool.get().await?;
                client.query_one("SELECT 1::BIGINT", &[]).await?;
                drop(client);
                durations.push(started.elapsed());
            }

            Ok::<_, BenchmarkError>(durations)
        }));
    }

    let mut durations = Vec::with_capacity(operation_count);
    for worker in workers {
        durations.extend(worker.await??);
    }

    Ok(Measurement {
        durations,
        elapsed: started.elapsed(),
    })
}

async fn measure(
    database_url: &str,
    recycling_method: RecyclingMethod,
    operation_count: usize,
    concurrency: usize,
) -> Result<Measurement, BenchmarkError> {
    let pool = create_pool(database_url, concurrency, recycling_method)?;
    let warmup_operations = 100.max(concurrency * 4);
    run_operations(&pool, warmup_operations, concurrency).await?;
    let measurement = run_operations(&pool, operation_count, concurrency).await?;
    pool.close();
    Ok(measurement)
}

fn print_measurement(name: &str, measurement: &mut Measurement) {
    println!(
        "{name:<5} {:>12.0} {:>11.1} {:>11.1}",
        measurement.operations_per_second(),
        measurement.percentile(50).as_secs_f64() * 1_000_000.0,
        measurement.percentile(95).as_secs_f64() * 1_000_000.0,
    );
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), BenchmarkError> {
    let database_url = std::env::var("DNP_SESSION_RESET_BENCHMARK_DATABASE_URL")?;
    let operation_count = parse_argument("operations", 1, 3000)?;
    let concurrency = parse_argument("concurrency", 2, 1)?;
    let rounds = parse_argument("rounds", 3, 3)?;

    let mut fast = Measurement::new();
    let mut clean = Measurement::new();

    for round in 0..rounds {
        if round % 2 == 0 {
            fast.add(
                measure(
                    &database_url,
                    RecyclingMethod::Fast,
                    operation_count,
                    concurrency,
                )
                .await?,
            );
            clean.add(
                measure(
                    &database_url,
                    RecyclingMethod::Clean,
                    operation_count,
                    concurrency,
                )
                .await?,
            );
        } else {
            clean.add(
                measure(
                    &database_url,
                    RecyclingMethod::Clean,
                    operation_count,
                    concurrency,
                )
                .await?,
            );
            fast.add(
                measure(
                    &database_url,
                    RecyclingMethod::Fast,
                    operation_count,
                    concurrency,
                )
                .await?,
            );
        }
    }

    println!(
        "operations={} concurrency={} rounds={}",
        operation_count, concurrency, rounds
    );
    println!("method        ops/second     p50 (us)    p95 (us)");
    print_measurement("fast", &mut fast);
    print_measurement("clean", &mut clean);

    let throughput_change =
        (clean.operations_per_second() / fast.operations_per_second() - 1.0) * 100.0;
    println!("clean throughput change: {throughput_change:.1}%");

    Ok(())
}
