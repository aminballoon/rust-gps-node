use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

pub struct PpkLogger {
    log_dir: PathBuf,
    rotation_hours: u64,
    current_file: Option<File>,
    current_file_path: Option<PathBuf>,
    opened_time: Option<DateTime<Utc>>,
}

impl PpkLogger {
    pub fn new<P: AsRef<Path>>(log_dir: P, rotation_hours: u64) -> Self {
        Self {
            log_dir: log_dir.as_ref().to_path_buf(),
            rotation_hours,
            current_file: None,
            current_file_path: None,
            opened_time: None,
        }
    }

    fn check_rotation(&mut self) -> Result<()> {
        let now = Utc::now();
        let need_rotation = match self.opened_time {
            None => true,
            Some(opened) => {
                let duration = now.signed_duration_since(opened);
                duration >= Duration::hours(self.rotation_hours as i64)
            }
        };

        if need_rotation {
            self.rotate(now)?;
        }
        Ok(())
    }

    fn rotate(&mut self, now: DateTime<Utc>) -> Result<()> {
        fs::create_dir_all(&self.log_dir)
            .context("Failed to create log directory")?;

        let filename = format!("gps_raw_{}.bin", now.format("%Y%m%d_%H%M%S"));
        let file_path = self.log_dir.join(filename);

        log::info!("Rotating PPK log to: {:?}", file_path);

        // Drop current file to close it
        self.current_file = None;

        let file = File::create(&file_path)
            .context(format!("Failed to create log file {:?}", file_path))?;

        self.current_file = Some(file);
        self.current_file_path = Some(file_path);
        self.opened_time = Some(now);

        Ok(())
    }

    pub fn write(&mut self, data: &[u8]) -> Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        self.check_rotation()?;
        if let Some(ref mut file) = self.current_file {
            file.write_all(data)
                .context("Failed to write to PPK raw log file")?;
            // Ensure data is immediately persisted on disk
            file.flush().context("Failed to flush PPK raw log file")?;
        }
        Ok(())
    }
}
