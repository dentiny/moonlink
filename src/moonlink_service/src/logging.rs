use std::path::Path;

use flexi_logger::{
    trc::{self, FormatConfig, SpecFileNotifier},
    writers::{FileLogWriter, FileLogWriterHandle},
    Cleanup, Criterion, FileSpec, LogSpecification, Naming, WriteMode,
};

/// Default log file size upper bound (change as needed).
pub const DEFAULT_LOG_FILE_SIZE: u64 = 4 * 1024 * 1024; // 4 MiB
/// Default max number of log files to retain (change as needed).
pub const DEFAULT_MAX_LOG_FILE_COUNT: usize = 1024;

/// Keep these alive for the lifetime of your program/test to keep logging active.
pub struct LoggingGuard {
    _writer: FileLogWriterHandle,
    _spec_notifier: SpecFileNotifier,
}

/// Initialize logging.
/// - If [`log_directory`] is specified, install a global tracing subscriber that writes to **rotating files** (size-based), with retention limits.
/// - If directory is not specified, logs will be streamed to stdout and stderr.
///
/// Returns a guard which needs to be kept alive.
pub fn init_logging(log_directory: Option<impl AsRef<Path>>) -> Option<LoggingGuard> {
    match log_directory {
        Some(dir) => {
            let spec = LogSpecification::env_or_parse("info").unwrap();
            let flwb = FileLogWriter::builder(
                FileSpec::default()
                    .directory(dir.as_ref())
                    .basename("moonlink")
                    .suffix("log"),
            )
            .rotate(
                Criterion::Size(DEFAULT_LOG_FILE_SIZE),
                Naming::Numbers,
                Cleanup::KeepLogFiles(DEFAULT_MAX_LOG_FILE_COUNT),
            )
            .write_mode(WriteMode::Async);

            let fmt_cfg = FormatConfig::default()
                .with_time(true)
                .with_file(true)
                .with_line_number(true)
                .with_target(false)
                .with_ansi(false);
            let (writer_handle, spec_notifier) =
                trc::setup_tracing(spec, None, flwb, &fmt_cfg).unwrap();

            Some(LoggingGuard {
                _writer: writer_handle,
                _spec_notifier: spec_notifier,
            })
        }
        None => {
            use tracing_subscriber::{
                fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Registry,
            };

            let env_filter =
                EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

            let fmt_layer = fmt::layer()
                .with_timer(time::ChronoLocal::new("%Y-%m-%d %H:%M:%S%:z".to_string()))
                .with_target(false)
                .with_file(true)
                .with_line_number(true)
                .with_ansi(false)
                .with_filter(env_filter);

            // Compose the subscriber.
            let subscriber = Registry::default().with(fmt_layer);

            #[cfg(feature = "profiling")]
            let subscriber = subscriber.with(console_subscriber::spawn());

            let _ = subscriber.try_init();

            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{init_logging, DEFAULT_LOG_FILE_SIZE};
    use more_asserts as ma;
    use tempfile::tempdir;
    use tracing::info;

    #[cfg(feature = "log-rotation-test")]
    #[test]
    fn test_log_rotation() {
        let temp_dir = tempdir().unwrap();
        let _guard = init_logging(Some(temp_dir.path())).unwrap();

        // Write enough data to force rotation.
        let target_size = (DEFAULT_LOG_FILE_SIZE as usize) / 4;
        for _ in 0..400 {
            let s = "a".repeat(target_size);
            info!("{}", s);
        }

        let entries: Vec<_> = std::fs::read_dir(temp_dir.path())
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        ma::assert_gt!(entries.len(), 1);
    }
}
