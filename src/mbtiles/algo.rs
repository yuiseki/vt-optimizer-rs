use anyhow::Result;
use geo_types::{
    Coord, Geometry, Line, LineString, MultiLineString, MultiPoint, MultiPolygon, Polygon,
};
use mvt::{GeomData, GeomEncoder, GeomType};

use crate::mbtiles::stats::HistogramBucket;
use crate::mbtiles::types::SampleSpec;

pub fn histogram_bucket_index(
    value: u64,
    min_len: Option<u64>,
    max_len: Option<u64>,
    buckets: usize,
) -> Option<usize> {
    if buckets == 0 {
        return None;
    }
    let min_len = min_len?;
    let max_len = max_len?;
    if min_len > max_len {
        return None;
    }
    let range = (max_len - min_len).max(1);
    let bucket_size = ((range as f64) / buckets as f64).ceil() as u64;
    let mut bucket = ((value.saturating_sub(min_len)) / bucket_size) as usize;
    if bucket >= buckets {
        bucket = buckets - 1;
    }
    Some(bucket)
}

pub fn count_vertices(geometry: &geo_types::Geometry<f32>) -> usize {
    match geometry {
        geo_types::Geometry::Point(_) => 1,
        geo_types::Geometry::MultiPoint(points) => points.len(),
        geo_types::Geometry::LineString(line) => ring_coords(line).len(),
        geo_types::Geometry::MultiLineString(lines) => {
            lines.iter().map(|l| ring_coords(l).len()).sum()
        }
        geo_types::Geometry::Line(_) => 2,
        geo_types::Geometry::Polygon(polygon) => {
            let mut count = ring_coords(polygon.exterior()).len();
            for ring in polygon.interiors() {
                count += ring_coords(ring).len();
            }
            count
        }
        geo_types::Geometry::MultiPolygon(polygons) => polygons
            .iter()
            .map(|polygon| {
                let mut count = ring_coords(polygon.exterior()).len();
                for ring in polygon.interiors() {
                    count += ring_coords(ring).len();
                }
                count
            })
            .sum(),
        geo_types::Geometry::Rect(_rect) => 4,
        geo_types::Geometry::Triangle(_) => 3,
        geo_types::Geometry::GeometryCollection(collection) => {
            collection.iter().map(count_vertices).sum()
        }
    }
}

pub fn format_property_value(value: &mvt_reader::feature::Value) -> String {
    match value {
        mvt_reader::feature::Value::String(text) => text.clone(),
        mvt_reader::feature::Value::Float(val) => val.to_string(),
        mvt_reader::feature::Value::Double(val) => val.to_string(),
        mvt_reader::feature::Value::Int(val) => val.to_string(),
        mvt_reader::feature::Value::UInt(val) => val.to_string(),
        mvt_reader::feature::Value::SInt(val) => val.to_string(),
        mvt_reader::feature::Value::Bool(val) => val.to_string(),
        mvt_reader::feature::Value::Null => "null".to_string(),
    }
}

pub fn encode_linestring(encoder: &mut GeomEncoder<f32>, line: &LineString<f32>) -> Result<()> {
    for coord in ring_coords(line) {
        encoder
            .add_point(coord.x, coord.y)
            .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))?;
    }
    Ok(())
}

pub fn ring_coords(line: &LineString<f32>) -> &[geo_types::Coord<f32>] {
    let coords = line.0.as_slice();
    if coords.len() > 1 && coords.first() == coords.last() {
        &coords[..coords.len() - 1]
    } else {
        coords
    }
}

pub fn encode_geometry(geometry: &Geometry<f32>) -> Result<GeomData> {
    match geometry {
        Geometry::Point(point) => {
            let encoder = GeomEncoder::new(GeomType::Point)
                .point(point.x(), point.y())
                .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))?;
            encoder
                .encode()
                .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))
        }
        Geometry::MultiPoint(MultiPoint(points)) => {
            let mut encoder = GeomEncoder::new(GeomType::Point);
            for point in points {
                encoder
                    .add_point(point.x(), point.y())
                    .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))?;
            }
            encoder
                .encode()
                .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))
        }
        Geometry::LineString(line) => {
            let mut encoder = GeomEncoder::new(GeomType::Linestring);
            encode_linestring(&mut encoder, line)?;
            encoder
                .encode()
                .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))
        }
        Geometry::Line(Line { start, end }) => {
            let line = LineString::from(vec![(start.x, start.y), (end.x, end.y)]);
            let mut encoder = GeomEncoder::new(GeomType::Linestring);
            encode_linestring(&mut encoder, &line)?;
            encoder
                .encode()
                .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))
        }
        Geometry::MultiLineString(MultiLineString(lines)) => {
            let mut encoder = GeomEncoder::new(GeomType::Linestring);
            for (idx, line) in lines.iter().enumerate() {
                encode_linestring(&mut encoder, line)?;
                if idx + 1 < lines.len() {
                    encoder
                        .complete_geom()
                        .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))?;
                }
            }
            encoder
                .encode()
                .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))
        }
        Geometry::Polygon(polygon) => {
            let mut encoder = GeomEncoder::new(GeomType::Polygon);
            let mut rings: Vec<&LineString<f32>> =
                Vec::with_capacity(1 + polygon.interiors().len());
            rings.push(polygon.exterior());
            for ring in polygon.interiors() {
                rings.push(ring);
            }
            for (idx, ring) in rings.iter().enumerate() {
                encode_linestring(&mut encoder, ring)?;
                if idx + 1 < rings.len() {
                    encoder
                        .complete_geom()
                        .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))?;
                }
            }
            encoder
                .encode()
                .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))
        }
        Geometry::MultiPolygon(MultiPolygon(polygons)) => {
            let mut encoder = GeomEncoder::new(GeomType::Polygon);
            for (poly_idx, polygon) in polygons.iter().enumerate() {
                let mut rings: Vec<&LineString<f32>> =
                    Vec::with_capacity(1 + polygon.interiors().len());
                rings.push(polygon.exterior());
                for ring in polygon.interiors() {
                    rings.push(ring);
                }
                for (idx, ring) in rings.iter().enumerate() {
                    encode_linestring(&mut encoder, ring)?;
                    if idx + 1 < rings.len() || poly_idx + 1 < polygons.len() {
                        encoder
                            .complete_geom()
                            .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))?;
                    }
                }
            }
            encoder
                .encode()
                .map_err(|err| anyhow::anyhow!("encode geometry: {err}"))
        }
        Geometry::GeometryCollection(_) => {
            anyhow::bail!("geometry collections are not supported for pruning");
        }
        Geometry::Rect(rect) => {
            let exterior = LineString::from(vec![
                (rect.min().x, rect.min().y),
                (rect.max().x, rect.min().y),
                (rect.max().x, rect.max().y),
                (rect.min().x, rect.max().y),
                (rect.min().x, rect.min().y),
            ]);
            let polygon = Polygon::new(exterior, Vec::new());
            encode_geometry(&Geometry::Polygon(polygon))
        }
        Geometry::Triangle(tri) => {
            let exterior = LineString::from(vec![
                (tri.0.x, tri.0.y),
                (tri.1.x, tri.1.y),
                (tri.2.x, tri.2.y),
                (tri.0.x, tri.0.y),
            ]);
            let polygon = Polygon::new(exterior, Vec::new());
            encode_geometry(&Geometry::Polygon(polygon))
        }
    }
}

pub fn simplify_geometry(geometry: &Geometry<f32>, tolerance: f32) -> Geometry<f32> {
    if tolerance <= 0.0 {
        return geometry.clone();
    }

    match geometry {
        Geometry::LineString(line) => {
            let simplified = simplify_line(&line.0, tolerance);
            Geometry::LineString(LineString::from(simplified))
        }
        Geometry::MultiLineString(lines) => {
            let simplified = lines
                .0
                .iter()
                .map(|line| LineString::from(simplify_line(&line.0, tolerance)))
                .collect::<Vec<_>>();
            Geometry::MultiLineString(MultiLineString(simplified))
        }
        Geometry::Polygon(polygon) => {
            let exterior = simplify_ring(&polygon.exterior().0, tolerance);
            let interiors = polygon
                .interiors()
                .iter()
                .map(|ring| simplify_ring(&ring.0, tolerance))
                .map(LineString::from)
                .collect::<Vec<_>>();
            Geometry::Polygon(Polygon::new(LineString::from(exterior), interiors))
        }
        Geometry::MultiPolygon(polygons) => {
            let simplified = polygons
                .0
                .iter()
                .map(|polygon| {
                    let exterior = simplify_ring(&polygon.exterior().0, tolerance);
                    let interiors = polygon
                        .interiors()
                        .iter()
                        .map(|ring| simplify_ring(&ring.0, tolerance))
                        .map(LineString::from)
                        .collect::<Vec<_>>();
                    Polygon::new(LineString::from(exterior), interiors)
                })
                .collect::<Vec<_>>();
            Geometry::MultiPolygon(MultiPolygon(simplified))
        }
        _ => geometry.clone(),
    }
}

fn simplify_ring(points: &[Coord<f32>], tolerance: f32) -> Vec<Coord<f32>> {
    if points.len() <= 4 {
        return points.to_vec();
    }

    let closed = points.first() == points.last();
    let core = if closed {
        points[..points.len() - 1].to_vec()
    } else {
        points.to_vec()
    };
    let simplified = simplify_line(&core, tolerance);
    if simplified.len() < 3 {
        return points.to_vec();
    }
    let mut out = simplified;
    if closed {
        out.push(out[0]);
    }
    out
}

fn simplify_line(points: &[Coord<f32>], tolerance: f32) -> Vec<Coord<f32>> {
    if points.len() <= 2 {
        return points.to_vec();
    }
    let sq_tolerance = tolerance * tolerance;
    let mut reduced = simplify_radial_dist(points, sq_tolerance);
    if reduced.len() <= 2 {
        return reduced;
    }
    reduced = simplify_douglas_peucker(&reduced, sq_tolerance);
    reduced
}

fn simplify_radial_dist(points: &[Coord<f32>], sq_tolerance: f32) -> Vec<Coord<f32>> {
    let mut prev = points[0];
    let mut out = vec![prev];
    for point in points.iter().skip(1) {
        if get_sq_dist(*point, prev) > sq_tolerance {
            out.push(*point);
            prev = *point;
        }
    }
    if prev != *points.last().unwrap() {
        out.push(*points.last().unwrap());
    }
    out
}

// Ramer–Douglas–Peucker algorithm
fn simplify_douglas_peucker(points: &[Coord<f32>], sq_tolerance: f32) -> Vec<Coord<f32>> {
    let last = points.len() - 1;
    let mut simplified = vec![points[0]];
    simplify_dp_step(points, 0, last, sq_tolerance, &mut simplified);
    simplified.push(points[last]);
    simplified
}

fn simplify_dp_step(
    points: &[Coord<f32>],
    first: usize,
    last: usize,
    sq_tolerance: f32,
    simplified: &mut Vec<Coord<f32>>,
) {
    let mut max_sq_dist = sq_tolerance;
    let mut index = None;

    for i in (first + 1)..last {
        let sq_dist = get_sq_seg_dist(points[i], points[first], points[last]);
        if sq_dist > max_sq_dist {
            index = Some(i);
            max_sq_dist = sq_dist;
        }
    }

    if let Some(idx) = index {
        if idx - first > 1 {
            simplify_dp_step(points, first, idx, sq_tolerance, simplified);
        }
        simplified.push(points[idx]);
        if last - idx > 1 {
            simplify_dp_step(points, idx, last, sq_tolerance, simplified);
        }
    }
}

fn get_sq_dist(p1: Coord<f32>, p2: Coord<f32>) -> f32 {
    let dx = p1.x - p2.x;
    let dy = p1.y - p2.y;
    dx * dx + dy * dy
}

fn get_sq_seg_dist(p: Coord<f32>, p1: Coord<f32>, p2: Coord<f32>) -> f32 {
    let mut x = p1.x;
    let mut y = p1.y;
    let dx = p2.x - x;
    let dy = p2.y - y;

    if dx != 0.0 || dy != 0.0 {
        let t = ((p.x - x) * dx + (p.y - y) * dy) / (dx * dx + dy * dy);
        if t > 1.0 {
            x = p2.x;
            y = p2.y;
        } else if t > 0.0 {
            x += dx * t;
            y += dy * t;
        }
    }

    let dx = p.x - x;
    let dy = p.y - y;
    dx * dx + dy * dy
}

pub fn include_sample(index: u64, total: u64, spec: Option<&SampleSpec>) -> bool {
    match spec {
        None => true,
        Some(SampleSpec::Count(count)) => index <= *count,
        Some(SampleSpec::Ratio(ratio)) => {
            if *ratio >= 1.0 {
                return true;
            }
            if *ratio <= 0.0 {
                return false;
            }
            let threshold = (ratio * u64::MAX as f64) as u64;
            let hash = splitmix64(index ^ total);
            hash <= threshold
        }
    }
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    z ^ (z >> 31)
}

pub fn build_histogram_from_sizes(
    tile_sizes: &[u64],
    total_tiles_used: u64,
    total_bytes_used: u64,
    buckets: usize,
    min_len: u64,
    max_len: u64,
    max_tile_bytes: u64,
) -> Vec<HistogramBucket> {
    if buckets == 0 || min_len > max_len {
        return Vec::new();
    }

    let range = (max_len - min_len).max(1);
    let bucket_size = ((range as f64) / buckets as f64).ceil() as u64;
    let mut counts = vec![0u64; buckets];
    let mut bytes = vec![0u64; buckets];

    for &length in tile_sizes {
        let mut bucket = ((length.saturating_sub(min_len)) / bucket_size) as usize;
        if bucket >= buckets {
            bucket = buckets - 1;
        }
        counts[bucket] += 1;
        bytes[bucket] += length;
    }

    let mut result = Vec::with_capacity(buckets);
    let mut accum_count = 0u64;
    let mut accum_bytes = 0u64;
    let limit_threshold = (max_tile_bytes as f64) * 0.9;

    for i in 0..buckets {
        let b_min = min_len + bucket_size * i as u64;
        let b_max = if i + 1 == buckets {
            max_len
        } else {
            (min_len + bucket_size * (i as u64 + 1)).saturating_sub(1)
        };
        accum_count += counts[i];
        accum_bytes += bytes[i];
        let running_avg = if accum_count == 0 {
            0
        } else {
            accum_bytes / accum_count
        };
        let pct_tiles = if total_tiles_used == 0 {
            0.0
        } else {
            counts[i] as f64 / total_tiles_used as f64
        };
        let pct_level_bytes = if total_bytes_used == 0 {
            0.0
        } else {
            bytes[i] as f64 / total_bytes_used as f64
        };
        let accum_pct_tiles = if total_tiles_used == 0 {
            0.0
        } else {
            accum_count as f64 / total_tiles_used as f64
        };
        let accum_pct_level_bytes = if total_bytes_used == 0 {
            0.0
        } else {
            accum_bytes as f64 / total_bytes_used as f64
        };
        let avg_over_limit = max_tile_bytes > 0 && (running_avg as f64) > max_tile_bytes as f64;
        let avg_near_limit =
            max_tile_bytes > 0 && !avg_over_limit && (running_avg as f64) >= limit_threshold;
        result.push(HistogramBucket {
            min_bytes: b_min,
            max_bytes: b_max,
            count: counts[i],
            total_bytes: bytes[i],
            running_avg_bytes: running_avg,
            pct_tiles,
            pct_level_bytes,
            accum_pct_tiles,
            accum_pct_level_bytes,
            avg_near_limit,
            avg_over_limit,
        });
    }
    result
}
