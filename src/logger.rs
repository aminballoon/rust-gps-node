use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

pub struct PpkLogger {
    log_dir: PathBuf,
    rotation_hours: u64,
    utc_offset_hours: i32,
    current_file: Option<File>,
    current_file_path: Option<PathBuf>,
    opened_time: Option<DateTime<Utc>>,
}

impl PpkLogger {
    pub fn new<P: AsRef<Path>>(log_dir: P, rotation_hours: u64, utc_offset_hours: i32) -> Self {
        Self {
            log_dir: log_dir.as_ref().to_path_buf(),
            rotation_hours,
            utc_offset_hours,
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

        use chrono::FixedOffset;
        let tz = FixedOffset::east_opt(self.utc_offset_hours * 3600).unwrap_or_else(|| FixedOffset::east_opt(0).unwrap());
        let local_now = now.with_timezone(&tz);

        let filename = format!("gps_raw_{}.bin", local_now.format("%Y%m%d_%H%M%S"));
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
            // fdatasync(2): commit file data to disk so the log survives
            // sudden power loss (PX4-style durable logging). This is the
            // app-level guarantee — independent of whether /mnt/sd is
            // mounted with `sync`.
            file.sync_data()
                .context("Failed to fdatasync PPK raw log file")?;
        }
        Ok(())
    }

    pub fn current_file_name(&self) -> Option<String> {
        self.current_file_path.as_ref()
            .and_then(|p| p.file_name())
            .map(|n| n.to_string_lossy().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_ppk_logger_timezone_filename() {
        // Create a logger with offset +7 (GMT+7)
        let mut logger = PpkLogger::new(".", 1, 7);
        // Let's call rotate directly with a specific Utc time
        // 2026-06-08 04:03:36 UTC
        let utc_time = Utc.with_ymd_and_hms(2026, 6, 8, 4, 3, 36).unwrap();
        logger.rotate(utc_time).unwrap();
        
        let name = logger.current_file_name().unwrap();
        println!("Generated filename: {}", name);
        // Clean up
        if let Some(ref path) = logger.current_file_path {
            let _ = std::fs::remove_file(path);
        }
        
        // Expected time in GMT+7: 04:03:36 + 7 hours = 11:03:36
        assert_eq!(name, "gps_raw_20260608_110336.bin");
    }
}

