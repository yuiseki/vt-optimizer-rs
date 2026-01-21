use crate::mbtiles::MbtilesStats;
use std::collections::HashSet;

#[derive(Debug, Clone, Copy)]
pub struct StatAccum {
    pub tile_count: u64,
    pub total_bytes: u64,
    pub max_bytes: u64,
}

impl StatAccum {
    pub fn add_tile(&mut self, length: u64) {
        self.tile_count += 1;
        self.total_bytes += length;
        self.max_bytes = self.max_bytes.max(length);
    }

    pub fn into_stats(self) -> MbtilesStats {
        let avg_bytes = if self.tile_count == 0 {
            0
        } else {
            self.total_bytes / self.tile_count
        };
        MbtilesStats {
            tile_count: self.tile_count,
            total_bytes: self.total_bytes,
            max_bytes: self.max_bytes,
            avg_bytes,
        }
    }
}

pub struct LayerAccum {
    pub feature_count: u64,
    pub vertex_count: u64,
    pub property_keys: HashSet<String>,
    pub property_values: HashSet<String>,
}

impl LayerAccum {
    pub fn new() -> Self {
        Self {
            feature_count: 0,
            vertex_count: 0,
            property_keys: HashSet::new(),
            property_values: HashSet::new(),
        }
    }
}
