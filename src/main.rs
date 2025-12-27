use hdrhistogram::sync::Recorder;
use hdrhistogram::{Histogram, SyncHistogram};
use std::cell::RefCell;
use std::sync::{LazyLock, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{error, info, trace};

static HIST_GLOBAL: LazyLock<RwLock<SyncHistogram<u64>>> = LazyLock::new(|| {
    let hist = Histogram::<u64>::new_with_max(10_000_000, 3).unwrap();
    RwLock::new(SyncHistogram::from(hist))
});

thread_local! {
    static HIST: RefCell<Recorder<u64>> = RefCell::new(HIST_GLOBAL.read().unwrap().recorder());
}

struct FromShredTxHandlerAsync;
struct FromShredTxHandlerOrigin;

#[cfg(feature = "async_version")]
impl unshred::TransactionHandler for FromShredTxHandlerAsync {
    fn handle_transaction(&self, event: &unshred::TransactionEvent) -> anyhow::Result<()> {
        let now_micros = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as u64;
        let shred_spent = Duration::from_micros(now_micros - event.processed_at_micros);
        trace!("shred_spent: {:?}", &shred_spent);
        HIST.with(|x| {
            let mut recorder = x.borrow_mut();
            if let Err(err) = recorder.record(shred_spent.as_micros() as u64) {
                error!("record shred_spent error: {:?}", err);
            }
        });
        Ok(())
    }
}

#[cfg(feature = "origin_version")]
impl unshred_0::TransactionHandler for FromShredTxHandlerOrigin {
    fn handle_transaction(&self, event: &unshred_0::TransactionEvent) -> anyhow::Result<()> {
        let now_micros = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as u64;
        let shred_spent = Duration::from_micros(now_micros - event.processed_at_micros);
        trace!("shred_spent: {:?}", &shred_spent);
        HIST.with(|x| {
            let mut recorder = x.borrow_mut();
            if let Err(err) = recorder.record(shred_spent.as_micros() as u64) {
                error!("record shred_spent error: {:?}", err);
            }
        });
        Ok(())
    }
}

#[cfg(feature = "wincode_version")]
impl unshred_1::TransactionHandler for FromShredTxHandlerOrigin {
    fn handle_transaction(&self, event: &unshred_1::TransactionEvent) -> anyhow::Result<()> {
        let now_micros = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as u64;
        let shred_spent = Duration::from_micros(now_micros - event.processed_at_micros);
        trace!("shred_spent: {:?}", &shred_spent);
        HIST.with(|x| {
            let mut recorder = x.borrow_mut();
            if let Err(err) = recorder.record(shred_spent.as_micros() as u64) {
                error!("record shred_spent error: {:?}", err);
            }
        });
        Ok(())
    }
}

fn main() {
    tracing_subscriber::fmt::init();

    #[cfg(feature = "async_version")]
    {
        info!("current feature async_version");
    }

    #[cfg(feature = "origin_version")]
    {
        info!("current feature origin_version");
    }

    #[cfg(feature = "wincode_version")]
    {
        info!("current feature wincode_version");
    }

    #[cfg(feature = "async_version")]
    let processor = unshred::UnshredProcessor::builder()
        .handler(FromShredTxHandlerAsync)
        .bind_address("0.0.0.0:7999")
        .build()
        .unwrap();

    #[cfg(feature = "origin_version")]
    let processor = unshred_0::UnshredProcessor::builder()
        .handler(FromShredTxHandlerOrigin)
        .bind_address("0.0.0.0:7999")
        .build()
        .unwrap();

    #[cfg(feature = "wincode_version")]
    let processor = unshred_1::UnshredProcessor::builder()
        .handler(FromShredTxHandlerOrigin)
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
            info!("MAX: {:?}", Duration::from_micros(guard.max()));
            info!("MIN: {:?}", Duration::from_micros(guard.min()));
            // guard.clear();
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
