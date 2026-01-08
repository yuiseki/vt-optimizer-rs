use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde_json::Value;

const PAINT_PROPERTIES_TO_CHECK: &[&str] = &[
    "fill-opacity",
    "fill-outline-color",
    "line-opacity",
    "line-width",
    "icon-size",
    "text-size",
    "text-max-width",
    "text-opacity",
    "raster-opacity",
    "circle-radius",
    "circle-opacity",
    "fill-extrusion-opacity",
    "heatmap-opacity",
];

#[derive(Debug, Clone)]
enum PaintValue {
    Number(f64),
    Stops(Vec<(u8, f64)>),
}

impl PaintValue {
    fn is_nonzero_at_zoom(&self, zoom: u8) -> bool {
        match self {
            PaintValue::Number(value) => *value != 0.0,
            PaintValue::Stops(stops) => {
                if let Some((_, value)) = stops.iter().find(|(z, _)| *z == zoom) {
                    *value != 0.0
                } else {
                    true
                }
            }
        }
    }
}

#[derive(Debug, Clone)]
struct MapboxStyleLayer {
    minzoom: Option<f64>,
    maxzoom: Option<f64>,
    visibility: Option<String>,
    paint: HashMap<String, PaintValue>,
    filter: Option<Filter>,
}

impl MapboxStyleLayer {
    fn is_visible_on_zoom(&self, zoom: u8) -> bool {
        self.check_layout_visibility()
            && self.check_zoom_underflow(zoom)
            && self.check_zoom_overflow(zoom)
    }

    fn check_layout_visibility(&self) -> bool {
        !matches!(self.visibility.as_deref(), Some("none"))
    }

    fn check_zoom_underflow(&self, zoom: u8) -> bool {
        self.minzoom.is_none_or(|minzoom| (zoom as f64) >= minzoom)
    }

    fn check_zoom_overflow(&self, zoom: u8) -> bool {
        self.maxzoom.is_none_or(|maxzoom| maxzoom > (zoom as f64))
    }

    fn is_rendered(&self, zoom: u8) -> bool {
        for prop in PAINT_PROPERTIES_TO_CHECK {
            if !self.check_paint_property_not_zero(prop, zoom) {
                return false;
            }
        }
        true
    }

    fn check_paint_property_not_zero(&self, property: &str, zoom: u8) -> bool {
        match self.paint.get(property) {
            Some(value) => value.is_nonzero_at_zoom(zoom),
            None => true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct MapboxStyle {
    layers_by_source_layer: HashMap<String, Vec<MapboxStyleLayer>>,
}

impl MapboxStyle {
    pub fn source_layers(&self) -> HashSet<String> {
        self.layers_by_source_layer.keys().cloned().collect()
    }

    pub fn is_layer_visible_on_zoom(&self, layer_name: &str, zoom: u8) -> bool {
        self.layers_by_source_layer
            .get(layer_name)
            .map(|layers| {
                layers
                    .iter()
                    .any(|layer| layer.is_visible_on_zoom(zoom) && layer.is_rendered(zoom))
            })
            .unwrap_or(false)
    }

    pub fn should_keep_feature(
        &self,
        layer_name: &str,
        zoom: u8,
        feature: &mvt_reader::feature::Feature,
        unknown_counter: &mut usize,
    ) -> FilterResult {
        let Some(layers) = self.layers_by_source_layer.get(layer_name) else {
            return FilterResult::False;
        };
        let mut saw_unknown = false;
        for layer in layers {
            if !layer.is_visible_on_zoom(zoom) || !layer.is_rendered(zoom) {
                continue;
            }
            let result = match layer.filter.as_ref() {
                None => FilterResult::True,
                Some(filter) => filter.evaluate(feature, zoom),
            };
            match result {
                FilterResult::True => return FilterResult::True,
                FilterResult::Unknown => {
                    saw_unknown = true;
                    *unknown_counter += 1;
                }
                FilterResult::False => {}
            }
        }
        if saw_unknown {
            FilterResult::Unknown
        } else {
            FilterResult::False
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterResult {
    True,
    False,
    Unknown,
}

#[derive(Debug, Clone)]
enum FilterValue {
    String(String),
    Number(f64),
    Bool(bool),
}

impl FilterValue {
    fn equals(&self, other: &FilterValue) -> bool {
        match (self, other) {
            (FilterValue::String(a), FilterValue::String(b)) => a == b,
            (FilterValue::Number(a), FilterValue::Number(b)) => (*a - *b).abs() < f64::EPSILON,
            (FilterValue::Bool(a), FilterValue::Bool(b)) => a == b,
            _ => false,
        }
    }
}

#[derive(Debug, Clone)]
enum FilterKey {
    Property(String),
    Type,
    Zoom,
}

#[derive(Debug, Clone)]
enum Expr {
    Literal(FilterValue),
    Get(String),
    Zoom,
    Type,
    Coalesce(Vec<Expr>),
    Match {
        input: Box<Expr>,
        cases: Vec<(FilterValue, Expr)>,
        fallback: Box<Expr>,
    },
    Case {
        branches: Vec<(Filter, Expr)>,
        fallback: Box<Expr>,
    },
}

#[derive(Debug, Clone)]
enum Filter {
    Eq(Expr, Expr),
    Neq(Expr, Expr),
    In(FilterKey, Vec<FilterValue>),
    NotIn(FilterKey, Vec<FilterValue>),
    Has(FilterKey),
    NotHas(FilterKey),
    All(Vec<Filter>),
    Any(Vec<Filter>),
    None(Vec<Filter>),
    Not(Box<Filter>),
    Unknown,
}

impl Filter {
    fn evaluate(&self, feature: &mvt_reader::feature::Feature, zoom: u8) -> FilterResult {
        match self {
            Filter::Eq(left, right) => match (
                eval_expr(left, feature, zoom),
                eval_expr(right, feature, zoom),
            ) {
                (Some(actual), Some(expected)) => FilterResult::from_bool(actual.equals(&expected)),
                _ => FilterResult::Unknown,
            },
            Filter::Neq(left, right) => match (
                eval_expr(left, feature, zoom),
                eval_expr(right, feature, zoom),
            ) {
                (Some(actual), Some(expected)) => {
                    FilterResult::from_bool(!actual.equals(&expected))
                }
                _ => FilterResult::Unknown,
            },
            Filter::In(key, values) => match feature_value_by_key(feature, key, zoom) {
                Some(actual) => FilterResult::from_bool(values.iter().any(|v| actual.equals(v))),
                None => FilterResult::Unknown,
            },
            Filter::NotIn(key, values) => match feature_value_by_key(feature, key, zoom) {
                Some(actual) => FilterResult::from_bool(!values.iter().any(|v| actual.equals(v))),
                None => FilterResult::Unknown,
            },
            Filter::Has(key) => FilterResult::from_bool(feature_has_key(feature, key)),
            Filter::NotHas(key) => FilterResult::from_bool(!feature_has_key(feature, key)),
            Filter::All(filters) => {
                let mut saw_unknown = false;
                for filter in filters {
                    match filter.evaluate(feature, zoom) {
                        FilterResult::True => {}
                        FilterResult::False => return FilterResult::False,
                        FilterResult::Unknown => saw_unknown = true,
                    }
                }
                if saw_unknown {
                    FilterResult::Unknown
                } else {
                    FilterResult::True
                }
            }
            Filter::Any(filters) => {
                let mut saw_unknown = false;
                for filter in filters {
                    match filter.evaluate(feature, zoom) {
                        FilterResult::True => return FilterResult::True,
                        FilterResult::False => {}
                        FilterResult::Unknown => saw_unknown = true,
                    }
                }
                if saw_unknown {
                    FilterResult::Unknown
                } else {
                    FilterResult::False
                }
            }
            Filter::None(filters) => {
                let mut saw_unknown = false;
                for filter in filters {
                    match filter.evaluate(feature, zoom) {
                        FilterResult::True => return FilterResult::False,
                        FilterResult::False => {}
                        FilterResult::Unknown => saw_unknown = true,
                    }
                }
                if saw_unknown {
                    FilterResult::Unknown
                } else {
                    FilterResult::True
                }
            }
            Filter::Not(filter) => match filter.evaluate(feature, zoom) {
                FilterResult::True => FilterResult::False,
                FilterResult::False => FilterResult::True,
                FilterResult::Unknown => FilterResult::Unknown,
            },
            Filter::Unknown => FilterResult::Unknown,
        }
    }
}

impl FilterResult {
    fn from_bool(value: bool) -> Self {
        if value {
            FilterResult::True
        } else {
            FilterResult::False
        }
    }
}

fn feature_has_key(feature: &mvt_reader::feature::Feature, key: &FilterKey) -> bool {
    match key {
        FilterKey::Type | FilterKey::Zoom => true,
        FilterKey::Property(name) => feature
            .properties
            .as_ref()
            .map(|props| props.contains_key(name))
            .unwrap_or(false),
    }
}

fn feature_value_by_key(
    feature: &mvt_reader::feature::Feature,
    key: &FilterKey,
    zoom: u8,
) -> Option<FilterValue> {
    match key {
        FilterKey::Type => Some(FilterValue::String(feature_type(feature).to_string())),
        FilterKey::Zoom => Some(FilterValue::Number(zoom as f64)),
        FilterKey::Property(name) => {
            let props = feature.properties.as_ref()?;
            let value = props.get(name)?;
            match value {
                mvt_reader::feature::Value::String(text) => Some(FilterValue::String(text.clone())),
                mvt_reader::feature::Value::Float(val) => Some(FilterValue::Number(*val as f64)),
                mvt_reader::feature::Value::Double(val) => Some(FilterValue::Number(*val)),
                mvt_reader::feature::Value::Int(val) => Some(FilterValue::Number(*val as f64)),
                mvt_reader::feature::Value::UInt(val) => Some(FilterValue::Number(*val as f64)),
                mvt_reader::feature::Value::SInt(val) => Some(FilterValue::Number(*val as f64)),
                mvt_reader::feature::Value::Bool(val) => Some(FilterValue::Bool(*val)),
                mvt_reader::feature::Value::Null => None,
            }
        }
    }
}

fn feature_type(feature: &mvt_reader::feature::Feature) -> &'static str {
    use geo_types::Geometry;
    match feature.geometry {
        Geometry::Point(_) | Geometry::MultiPoint(_) => "Point",
        Geometry::LineString(_) | Geometry::MultiLineString(_) | Geometry::Line(_) => "LineString",
        Geometry::Polygon(_)
        | Geometry::MultiPolygon(_)
        | Geometry::Rect(_)
        | Geometry::Triangle(_) => "Polygon",
        Geometry::GeometryCollection(_) => "Unknown",
    }
}

fn parse_paint_value(value: &Value) -> Option<PaintValue> {
    if let Some(number) = value.as_f64() {
        return Some(PaintValue::Number(number));
    }
    let stops = value.get("stops")?.as_array()?;
    let mut parsed = Vec::new();
    for stop in stops {
        let arr = stop.as_array()?;
        if arr.len() < 2 {
            continue;
        }
        let zoom = arr[0].as_f64()? as i64;
        let value = arr[1].as_f64()?;
        if !(0..=255).contains(&zoom) {
            continue;
        }
        parsed.push((zoom as u8, value));
    }
    if parsed.is_empty() {
        None
    } else {
        Some(PaintValue::Stops(parsed))
    }
}

fn parse_filter(value: &Value) -> Option<Filter> {
    let array = value.as_array()?;
    if array.is_empty() {
        return None;
    }
    let op = array[0].as_str()?;
    match op {
        "!" => {
            if array.len() < 2 {
                return Some(Filter::Unknown);
            }
            let inner = parse_filter(&array[1]).unwrap_or(Filter::Unknown);
            Some(Filter::Not(Box::new(inner)))
        }
        "==" | "!=" => {
            if array.len() < 3 {
                return Some(Filter::Unknown);
            }
            let left = parse_filter_lhs(&array[1])?;
            let right = parse_expr(&array[2])?;
            if op == "==" {
                Some(Filter::Eq(left, right))
            } else {
                Some(Filter::Neq(left, right))
            }
        }
        "in" | "!in" => {
            if array.len() < 3 {
                return Some(Filter::Unknown);
            }
            let key = parse_filter_key(&array[1])?;
            let mut values = Vec::new();
            if let Some(list) = array[2].as_array() {
                for item in list {
                    if let Some(value) = parse_filter_value(item) {
                        values.push(value);
                    } else {
                        return Some(Filter::Unknown);
                    }
                }
            } else {
                for item in &array[2..] {
                    if let Some(value) = parse_filter_value(item) {
                        values.push(value);
                    } else {
                        return Some(Filter::Unknown);
                    }
                }
            }
            if op == "in" {
                Some(Filter::In(key, values))
            } else {
                Some(Filter::NotIn(key, values))
            }
        }
        "has" | "!has" => {
            if array.len() < 2 {
                return Some(Filter::Unknown);
            }
            let key = parse_filter_key(&array[1])?;
            if op == "has" {
                Some(Filter::Has(key))
            } else {
                Some(Filter::NotHas(key))
            }
        }
        "all" | "any" | "none" => {
            let mut filters = Vec::new();
            for item in &array[1..] {
                if let Some(filter) = parse_filter(item) {
                    filters.push(filter);
                } else {
                    filters.push(Filter::Unknown);
                }
            }
            match op {
                "all" => Some(Filter::All(filters)),
                "any" => Some(Filter::Any(filters)),
                _ => Some(Filter::None(filters)),
            }
        }
        _ => Some(Filter::Unknown),
    }
}

fn parse_filter_value(value: &Value) -> Option<FilterValue> {
    if let Some(text) = value.as_str() {
        return Some(FilterValue::String(text.to_string()));
    }
    if let Some(number) = value.as_f64() {
        return Some(FilterValue::Number(number));
    }
    if let Some(boolean) = value.as_bool() {
        return Some(FilterValue::Bool(boolean));
    }
    None
}

fn parse_expr(value: &Value) -> Option<Expr> {
    if let Some(text) = value.as_str() {
        return Some(Expr::Literal(FilterValue::String(text.to_string())));
    }
    if let Some(number) = value.as_f64() {
        return Some(Expr::Literal(FilterValue::Number(number)));
    }
    if let Some(boolean) = value.as_bool() {
        return Some(Expr::Literal(FilterValue::Bool(boolean)));
    }
    let array = value.as_array()?;
    if array.is_empty() {
        return None;
    }
    let op = array[0].as_str()?;
    match op {
        "get" => {
            let key = array.get(1)?.as_str()?;
            Some(Expr::Get(key.to_string()))
        }
        "zoom" => Some(Expr::Zoom),
        "geometry-type" => Some(Expr::Type),
        "coalesce" => {
            let mut items = Vec::new();
            for item in array.iter().skip(1) {
                items.push(parse_expr(item)?);
            }
            if items.is_empty() {
                None
            } else {
                Some(Expr::Coalesce(items))
            }
        }
        "match" => {
            if array.len() < 4 {
                return None;
            }
            let input = parse_expr(&array[1])?;
            let mut cases = Vec::new();
            let mut idx = 2;
            while idx + 1 < array.len() - 1 {
                let match_value = parse_filter_value(&array[idx])?;
                let output = parse_expr(&array[idx + 1])?;
                cases.push((match_value, output));
                idx += 2;
            }
            let fallback = parse_expr(array.last()?)?;
            Some(Expr::Match {
                input: Box::new(input),
                cases,
                fallback: Box::new(fallback),
            })
        }
        "case" => {
            if array.len() < 4 {
                return None;
            }
            let mut branches = Vec::new();
            let mut idx = 1;
            while idx + 1 < array.len() - 1 {
                let condition = parse_filter(&array[idx]).unwrap_or(Filter::Unknown);
                let output = parse_expr(&array[idx + 1])?;
                branches.push((condition, output));
                idx += 2;
            }
            let fallback = parse_expr(array.last()?)?;
            Some(Expr::Case {
                branches,
                fallback: Box::new(fallback),
            })
        }
        _ => None,
    }
}

fn parse_filter_key(value: &Value) -> Option<FilterKey> {
    if let Some(name) = value.as_str() {
        return Some(match name {
            "$type" | "geometry-type" => FilterKey::Type,
            "zoom" => FilterKey::Zoom,
            _ => FilterKey::Property(name.to_string()),
        });
    }
    let array = value.as_array()?;
    if array.is_empty() {
        return None;
    }
    let op = array[0].as_str()?;
    match op {
        "get" => {
            let key = array.get(1)?.as_str()?;
            Some(FilterKey::Property(key.to_string()))
        }
        "zoom" => Some(FilterKey::Zoom),
        "geometry-type" => Some(FilterKey::Type),
        _ => None,
    }
}

fn parse_filter_lhs(value: &Value) -> Option<Expr> {
    if let Some(key) = parse_filter_key(value) {
        return Some(expr_from_key(key));
    }
    parse_expr(value)
}

fn expr_from_key(key: FilterKey) -> Expr {
    match key {
        FilterKey::Property(name) => Expr::Get(name),
        FilterKey::Zoom => Expr::Zoom,
        FilterKey::Type => Expr::Type,
    }
}

fn eval_expr(expr: &Expr, feature: &mvt_reader::feature::Feature, zoom: u8) -> Option<FilterValue> {
    match expr {
        Expr::Literal(value) => Some(value.clone()),
        Expr::Get(name) => feature_value_by_key(feature, &FilterKey::Property(name.clone()), zoom),
        Expr::Zoom => Some(FilterValue::Number(zoom as f64)),
        Expr::Type => Some(FilterValue::String(feature_type(feature).to_string())),
        Expr::Coalesce(items) => {
            for item in items {
                if let Some(value) = eval_expr(item, feature, zoom) {
                    return Some(value);
                }
            }
            None
        }
        Expr::Match {
            input,
            cases,
            fallback,
        } => {
            let input_value = eval_expr(input, feature, zoom)?;
            for (match_value, output) in cases {
                if input_value.equals(match_value) {
                    return eval_expr(output, feature, zoom);
                }
            }
            eval_expr(fallback, feature, zoom)
        }
        Expr::Case { branches, fallback } => {
            for (condition, output) in branches {
                match condition.evaluate(feature, zoom) {
                    FilterResult::True => return eval_expr(output, feature, zoom),
                    FilterResult::False => {}
                    FilterResult::Unknown => return None,
                }
            }
            eval_expr(fallback, feature, zoom)
        }
    }
}

pub fn read_style(path: &Path) -> Result<MapboxStyle> {
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read style file: {}", path.display()))?;
    let value: Value = serde_json::from_str(&contents).context("parse style json")?;
    let layers = value
        .get("layers")
        .and_then(|layers| layers.as_array())
        .ok_or_else(|| anyhow::anyhow!("style json missing layers array"))?;

    let mut layers_by_source_layer: HashMap<String, Vec<MapboxStyleLayer>> = HashMap::new();
    for layer in layers {
        if layer.get("source").is_none() {
            continue;
        }
        let Some(source_layer) = layer.get("source-layer").and_then(|v| v.as_str()) else {
            continue;
        };
        let minzoom = layer.get("minzoom").and_then(|v| v.as_f64());
        let maxzoom = layer.get("maxzoom").and_then(|v| v.as_f64());
        let visibility = layer
            .get("layout")
            .and_then(|layout| layout.get("visibility"))
            .and_then(|v| v.as_str())
            .map(|v| v.to_string());
        let mut paint = HashMap::new();
        if let Some(props) = layer.get("paint").and_then(|paint| paint.as_object()) {
            for (key, value) in props {
                if let Some(parsed) = parse_paint_value(value) {
                    paint.insert(key.clone(), parsed);
                }
            }
        }
        let filter = layer.get("filter").and_then(parse_filter);
        layers_by_source_layer
            .entry(source_layer.to_string())
            .or_default()
            .push(MapboxStyleLayer {
                minzoom,
                maxzoom,
                visibility,
                paint,
                filter,
            });
    }

    if layers_by_source_layer.is_empty() {
        anyhow::bail!("style json contains no source-layer entries");
    }
    Ok(MapboxStyle {
        layers_by_source_layer,
    })
}

pub fn read_style_source_layers(path: &Path) -> Result<HashSet<String>> {
    Ok(read_style(path)?.source_layers())
}
