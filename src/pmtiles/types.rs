use indicatif::{ProgressBar, ProgressDrawTarget, ProgressStyle};
use std::time::Duration;

pub const HEADER_SIZE: usize = 127;
pub const MAGIC: &[u8; 7] = b"PMTiles";
pub const VERSION: u8 = 3;

#[derive(Debug, Clone)]
pub struct Header {
    pub root_offset: u64,
    pub root_length: u64,
    pub metadata_offset: u64,
    pub metadata_length: u64,
    pub leaf_offset: u64,
    pub leaf_length: u64,
    pub data_offset: u64,
    pub data_length: u64,
    pub n_addressed_tiles: u64,
    pub n_tile_entries: u64,
    pub n_tile_contents: u64,
    pub clustered: u8,
    pub internal_compression: u8,
    pub tile_compression: u8,
    pub tile_type: u8,
    pub min_zoom: u8,
    pub max_zoom: u8,
    pub min_longitude: i32,
    pub min_latitude: i32,
    pub max_longitude: i32,
    pub max_latitude: i32,
    pub center_zoom: u8,
    pub center_longitude: i32,
    pub center_latitude: i32,
}

#[derive(Debug, Clone)]
pub struct Entry {
    pub tile_id: u64,
    pub offset: u64,
    pub length: u32,
    pub run_length: u32,
}

pub struct ProgressTracker {
    pub bar: ProgressBar,
    pub total: u64,
    pub is_bar: bool,
    pub processed: u64,
}

impl ProgressTracker {
    pub fn new(message: &str, total: u64, use_bar: bool) -> Self {
        let bar = if use_bar && total > 0 {
            let bar = make_progress_bar(total);
            bar.set_message(message.to_string());
            bar
        } else {
            make_spinner(message)
        };
        Self {
            bar,
            total,
            is_bar: use_bar && total > 0,
            processed: 0,
        }
    }

    pub fn inc(&mut self, delta: u64) {
        self.processed = self.processed.saturating_add(delta);
        if self.is_bar {
            let cap = self.total.saturating_sub(1);
            let pos = self.processed.min(cap);
            self.bar.set_position(pos);
        } else {
            self.bar.inc(delta);
        }
    }

    pub fn finish(self) {
        if self.is_bar {
            self.bar.set_position(self.total);
        }
        self.bar.finish_and_clear();
    }
}

fn make_progress_bar(total: u64) -> ProgressBar {
    let bar = ProgressBar::with_draw_target(Some(total), ProgressDrawTarget::stderr_with_hz(10));
    bar.set_style(
        ProgressStyle::with_template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
            .unwrap()
            .progress_chars("=>-"),
    );
    bar.enable_steady_tick(Duration::from_millis(200));
    bar
}

fn make_spinner(message: &str) -> ProgressBar {
    let spinner = ProgressBar::new_spinner();
    spinner.set_draw_target(ProgressDrawTarget::stderr_with_hz(20));
    spinner.set_style(
        ProgressStyle::with_template("{spinner:.cyan} {msg} ({pos} tiles processed)")
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]),
    );
    spinner.set_message(message.to_string());
    spinner.enable_steady_tick(Duration::from_millis(80));
    spinner
}

pub fn progress_for_phase(
    message: &str,
    total: u64,
    use_bar: bool,
    no_progress: bool,
) -> Option<ProgressTracker> {
    if no_progress {
        None
    } else {
        Some(ProgressTracker::new(message, total, use_bar))
    }
}
