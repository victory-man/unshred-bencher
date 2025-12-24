use hdrhistogram::sync::Recorder;
use hdrhistogram::{Histogram, SyncHistogram};
use std::cell::RefCell;
use std::sync::{LazyLock, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{error, info, trace};
use unshred::{TransactionEvent, TransactionHandler, UnshredProcessor};

static HIST_GLOBAL: LazyLock<RwLock<SyncHistogram<u64>>> = LazyLock::new(|| {
    let hist = Histogram::<u64>::new_with_max(10_000_000, 3).unwrap();
    RwLock::new(SyncHistogram::from(hist))
});

thread_local! {
    static HIST: RefCell<Recorder<u64>> = RefCell::new(HIST_GLOBAL.read().unwrap().recorder());
}

struct FromShredTxHandler;

impl TransactionHandler for FromShredTxHandler {
    fn handle_transaction(&self, event: &TransactionEvent) -> anyhow::Result<()> {
        let shred_spent = if let Some(received_at_micros) = event.received_at_micros {
            let now_micros = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as u64;
            Some(Duration::from_micros(now_micros - received_at_micros))
        } else {
            None
        };
        trace!("shred_spent: {:?}", &shred_spent);
        if let Some(shred_spent) = shred_spent {
            HIST.with(|x| {
                let mut recorder = x.borrow_mut();
                if let Err(err) = recorder.record(shred_spent.as_micros() as u64) {
                    error!("record shred_spent error: {:?}", err);
                }
            })
        }
        Ok(())
    }
}

fn main() {
    tracing_subscriber::fmt::init();

    let processor = UnshredProcessor::builder()
        .handler(FromShredTxHandler)
        .bind_address("0.0.0.0:7999")
        .build()
        .unwrap();

    let async_runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let handle0 = async_runtime.spawn(async move { processor.run().await });

    let handle1 = async_runtime.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        interval.tick().await;
        {
            let mut guard = HIST_GLOBAL.write().unwrap();
            guard.refresh_timeout(Duration::from_secs(15));
        }
        loop {
            interval.tick().await;
            let mut guard = HIST_GLOBAL.write().unwrap();
            guard.refresh_timeout(Duration::from_secs(15));
            info!(
                "P99: {:?}",
                Duration::from_micros(guard.value_at_percentile(99f64))
            );
            info!(
                "P50: {:?}",
                Duration::from_micros(guard.value_at_percentile(50f64))
            );
            info!(
                "P25: {:?}",
                Duration::from_micros(guard.value_at_percentile(25f64))
            );
            info!(
                "P10: {:?}",
                Duration::from_micros(guard.value_at_percentile(10f64))
            );
            guard.clear();
            info!("----------------")
        }
    });

    info!("Ctrl+C结束程序");

    async_runtime.block_on(async {
        tokio::signal::ctrl_c().await.unwrap();
    });

    info!("Shutdown complete");

    handle0.abort();
    handle1.abort();

    async_runtime.shutdown_timeout(Duration::from_secs(1));
}
