# SPEC.md — tile-prune

* Project: **tile-prune**
* Type: Single-binary CLI + Rust SDK (library)
* Status: Draft (living spec)
* Compatibility baseline: vt-optimizer 準拠（入出力境界・メタデータ方針・警告方針・簡略化の挙動の方向性）

## 0. 概要

tile-prune は、**Mapbox Vector Tiles (MVT)** を格納した **MBTiles / PMTiles** を対象に、以下を行うツールチェイン（CLI/SDK）です。

1. **検査 (inspect)**: タイルサイズ・分布・レイヤー/フィーチャー統計を可観測化
2. **最適化 (prune)**: Style（Mapbox/MapLibre）解釈に基づき、**不要レイヤー削除 + filter 解釈による feature 単位削除**
3. **簡略化 (simplify)**: 指定条件でジオメトリ頂点を減らし、サイズ/描画負荷を抑制

本プロジェクトは **planet 規模の MBTiles** を前提とし、**読み取り並列 + 単一 writer 集約**のストリーミング処理を基本路線とします。SQLite の WAL モードは「readers が writer をブロックしにくい」性質があり、並列読み取りと単一書き込みの設計と相性が良い、という立場を取ります。

---

## 1. ゴール / 非ゴール

### 1.1 ゴール

* 入出力フォーマット

  * 入力: **MBTiles v1.3 相当（vt-optimizer 互換運用）**, **PMTiles v3（ローカルファイル）**
  * 出力: **MBTiles / PMTiles v3**
  * 出力フォーマットはユーザーが指定可能。デフォルトは入力と同一（ただし出力ファイル名拡張子が明示ならそれに従う“ffmpeg 的”挙動）。
* 品質基準（デフォルト）

  * `--max-tile-bytes` デフォルト **1,250KB**（= 1,250 * 1024 bytes）
  * 超過は **エラーではなく警告**（vt-optimizer 準拠）。
* Style 解釈（最適化）

  * Mapbox Style Spec / MapLibre Style Spec を解釈
  * **filter 式を評価し、filter を通らない feature を削除**
  * filter 仕様上の制約を遵守：

    * 「一致する feature のみ表示」という意味論（= それ以外は prune 対象）
    * zoom 式は整数 zoom で評価
    * filter 内の `feature-state` は非対応（= 仕様上 filter で使えない）
  * 未対応の式に遭遇した場合は **“何もしない（残す）”** を原則にする（保守的 pruning）。
* 性能（目標 SLO）

  * 92GB の planet.mbtiles を 32 vCPU / 96GB RAM / NVMe で **6時間程度以内**（目標）
  * ストリーミング処理 + 読取り並列 + 単一 writer 集約
* 配布形態

  * **単一バイナリ CLI** を主軸（高ポータビリティ）
  * 同一実装コアを **SDK（Rust crate）** として提供

### 1.2 非ゴール

* HTTP Range 前提のリモート入力（S3 等）は **考慮しない**（ローカルファイルのみ）。
* 対話 UI（Inquirer 的なもの）は **提供しない**。古き良き CLI（引数 + stdout/stderr）。
* Style の paint/layout の全表現を完全再現（レンダラ互換の厳密さ）は当面非ゴール。prune のために必要な範囲から段階的に拡張する。

---

## 1.3 マイルストーン（v0.0.1）

今日やりきる最小スコープとして、以下を v0.0.1 とする。

* CLI 骨格が動作し、引数解析がテストで保証される
  * `inspect` / `optimize` / `copy` / `simplify` / `verify` が起動できる（処理本体は未実装で可）
* 入出力フォーマット推定のルールが実装済み
  * `--input-format` / `--output-format` で上書き可能
  * `--output-format` と `--output` 拡張子の矛盾は **エラー（exit 1）**
* `copy` / `optimize` はフォーマット決定まで実施する（I/O は未実装で可）
* `cargo test` が通ること

v0.0.1 では以下を **含めない**。

* MBTiles/PMTiles の読み書き・変換処理
* inspect の統計計算
* style.json の解釈
* simplify の実装
* sidecar / checkpoint / JSON レポート

---

## 1.4 マイルストーン（v0.0.2）

* `copy` の最小実装（**MBTiles→MBTiles のみ**）
  * tiles/metadata を単純コピー
* `inspect` の最小実装（**MBTiles のみ**）
  * tile count / total bytes / max bytes を出力
  * JSON 出力やヒストグラムは未対応
* `optimize` はフォーマット決定のみ（処理は未実装）

v0.0.2 では以下を **含めない**。

* MBTiles→PMTiles や PMTiles 入力
* filter/style 解釈
* simplify 実装
* sidecar / checkpoint / JSON レポート

---

## 2. 用語

* **Tile key**: `z/x/y`（内部表現は XYZ）
* **MBTiles tile_row**: TMS 由来の Y 反転を前提（後述）
* **MVT**: Mapbox Vector Tile（PBF）。extent は 4096 が一般的。
* **Style layer**: Mapbox/MapLibre style JSON の `layers[]`

---

## 3. 入出力フォーマット

### 3.1 MBTiles

* SQLite コンテナで、`tiles(zoom_level, tile_column, tile_row, tile_data)` を基本形（vt-optimizer 準拠）。
* `tiles` が view の場合や、normalized schema（map/images）でも、`tiles` view が提供される前提で透過的に扱う（実務上の互換性）。
* タイル座標の Y は **TMS（下原点）**の反転で格納される前提。XYZ との変換は以下：

  * `y_tms = (2^z - 1) - y_xyz`
* 内部表現は **XYZ**（z/x/y）に統一し、入出力境界で MBTiles 規約へ変換する。

### 3.2 PMTiles（v3）

* 入力/出力とも **PMTiles v3** を基本対象とする（まず v3、必要なら v2 読み取りを将来検討）。
* PMTiles は「単一ファイルのタイルアーカイブ」で、一般にリモート Range を想定した設計だが、本仕様では **ローカルファイルとして読み書き** する。
* PMTiles は **read-only 形式**であり、原則「in-place 更新」ではなく「再生成」になる。

### 3.3 タイルデータ（MVT）

* `.mvt` / `.pbf` は Protocol Buffers ベース。
* extent 4096 は事実上の標準として扱い、互換性を優先する。
* gzip 圧縮については “auto” を基本（vt-optimizer 互換）。解凍に失敗した場合は raw PBF として扱う設定も持てる。

---

## 4. 機能仕様（CLI）

### 4.1 コマンド体系

```
tile-prune <command> [options] <input> [<output>]
```

`<command>`:

* `inspect` : 統計・分布・サンプリング
* `prune`   : style ベース最適化（レイヤー削除 + feature 削除）
* `simplify`: ジオメトリ簡略化
* `copy`    : 変換のみ（MBTiles⇄PMTiles、再圧縮/正規化含む、任意）

### 4.2 入出力フォーマット推定（ffmpeg 的挙動）

* 入力フォーマット：拡張子で推定（`.mbtiles` / `.pmtiles`）。不明なら `--input-format` 必須。
* 出力フォーマット：

  * `--output-format` があればそれを採用
  * なければ `<output>` の拡張子で推定
  * `<output>` が省略された場合、デフォルトは **入力と同一フォーマット**で、`<input>` にサフィックスを付与（例：`planet.mbtiles` → `planet.pruned.mbtiles`）
* `<output>` がファイル名として与えられた場合、拡張子優先（ffmpeg 風）。

### 4.3 共通オプション

* `--max-tile-bytes <bytes>`: デフォルト 1,250KB。超過は警告のみ。
* `--threads <n>`: ワーカ数（デフォルトは論理 CPU 数に基づく）
* `--io-batch <n>`: writer のコミット粒度（タイル n 件ごと）
* `--checkpoint <path>`: sidecar 状態ファイル（JSON/SQLite）
* `--resume`: checkpoint があれば再開
* `--log <level>`: `error|warn|info|debug|trace`
* `--output <text|json|ndjson>`: レポート出力形式（stdout）

### 4.4 inspect

目的：ボトルネックとなる zoom/領域/レイヤーを特定し、prune/simplify の入力設計に資する。

出力（最低限）：

* 全体:

  * tile count
  * total bytes / mean / p50 / p95 / max
  * `max-tile-bytes` 超過数
* zoom 別:

  * 同様の統計
* オプション:

  * `--histogram-buckets 10`（デフォルト 10）
  * `--topn <k>`: 最大タイル（サイズ）上位 k 件（z/x/y、bytes、layer count 等）

### 4.5 prune

入力：tileset + style JSON（任意）
出力：tileset（同一または変換）

主要仕様：

* レイヤー削除

  * style で参照されない `source-layer` は削除対象
  * zoom により可視でない layer はその zoom の tile から除外
* feature 削除（filter 解釈）

  * 同一 `source-layer` を複数 style layer が参照する場合、**可視な style layer の filter の論理和（OR）**で feature を残す
  * style layer に filter が無い場合は「全 feature を表示」とみなす（= 残す）
  * 未対応の式が含まれる場合は、該当 style layer は「判定不能」として **保守的に残す**（後述）

style 解釈はユーザーが選べる：

* `--style-mode none`：style 無視（何も prune しない / ただし再圧縮などは可能）
* `--style-mode layer`：未使用 layer の削除のみ
* `--style-mode layer+filter`：layer 削除 + filter による feature 削除（既定）

### 4.6 simplify

* まずは vt-optimizer と挙動を乖離させない方針（実装速度優先）
* 単位：基本は `source-layer` 単位（MVT layer 名）
* オプション例：

  * `--layer <name>`（複数可）
  * `--tolerance <float>`（既定は小さめ、または必須）
  * `--preserve-topology <bool>`（初期は false でも可）
* 実装は SDK 側の抽象（SimplifyEngine trait）で差し替え可能にする

---

## 5. Style 解釈仕様（Mapbox / MapLibre）

### 5.1 参照仕様の要点（prune に必要な範囲）

* `filter` は「一致する feature のみ表示」
* zoom 式は整数 zoom レベルで評価される
* filter 内 `feature-state` はサポートされない
* `minzoom` / `maxzoom` により layer 可視性が決まる
* `layout.visibility` は `visible|none`（MapLibre の記述を正とする）

### 5.2 対象入力

* Style JSON（Mapbox/MapLibre 互換）
* `sources[]` のうち vector source を対象
* style layer が参照する `source-layer` を MVT layer 名に対応付け

### 5.3 可視性判定（レイヤー）

タイル zoom = z に対し、style layer を「可視」とみなす条件（MVP）：

* `layout.visibility != "none"`（未指定は visible 扱い）
* `z >= minzoom` かつ `z < maxzoom`（未指定は 0/24 扱い）

拡張（vt-optimizer 互換の方向）：

* paint の opacity 系が常に 0 になる等 “実質不可視” も prune 対象にできるが、初期はオプション扱い：

  * `--visibility paint` を指定した場合に有効化

### 5.4 filter による feature 残存判定

* ある MVT layer（=source-layer）について、zoom z で可視な style layer 群 `S(z)` を集める
* 各 style layer `s` の filter を `F_s` とする（省略時は TRUE）
* feature `f` を残す条件：

  * `OR_{s in S(z)} eval(F_s, f, z) == TRUE`
* `eval` が **UNKNOWN**（未対応式）を返した場合の方針：

  * 既定：UNKNOWN は TRUE と同等に扱う（= 保守的に残す）
  * オプション：`--unknown-filter drop|keep`（既定 keep）

---

## 6. 並列化アーキテクチャ（本線：読取り並列＋単一 writer）

### 6.1 基本構造

* Reader（1スレッド）：入力 DB / アーカイブからタイルを列挙し、ジョブキューへ投入
* Workers（Nスレッド）：decode → prune/simplify → encode の CPU 処理
* Writer（1スレッド）：出力へ順次書き込み（MBTiles は SQLite transaction batching、PMTiles はビルドパイプライン）

ストリーミングのための設計要点：

* **bounded channel**（バックプレッシャ）でメモリ上限を制御
* 出力側が詰まったら入力列挙を抑制
* 変換後タイルの一時保持は最小化（タイル blob を持ち回るだけ）

SQLite については WAL が「reader/writer の同時進行」に寄与し得るため、MBTiles 出力時は WAL を検討・推奨します（実装はオプションまたはデフォルト ON）。([SQLite][2])

### 6.2 代替案（将来/実験）：分割→一時成果物→マージ

* zoom/領域で分割して一時 MBTiles/PMTiles を生成し、最後にマージ
* 仕様上は “選べる設計” の余地を残すが、初期リリースでは `--plan split-merge` として **実験機能** 扱い
* ただし checkpoint/restart や失敗局所化の利点があるため、ARD/ADR 側で評価を継続

---

## 7. チェックポイント / 再開（sidecar 状態管理）

### 7.1 形式

* `--checkpoint <path>` で指定
* 形式は **SQLite を推奨**（planet 規模で JSON の追記・肥大化を避けるため）

  * ただし MVP では JSON 実装でも可（互換のため versioned schema を持つ）

### 7.2 収録内容（最低限）

* 入力識別

  * input path / size / mtime（必要なら hash）
  * input format（mbtiles/pmtiles）
* 出力識別

  * output path / format
  * 生成途中の temporary path（安全な原子的置換のため）
* 実行オプションの fingerprint（style の hash を含む）
* 進捗 watermark

  * MBTiles：`(z, x, y_xyz)` の最後に完了したキー、または入力列挙順の rowid
  * PMTiles：tile data セクションの offset、ディレクトリ構築に必要な中間インデックスの進捗
* 統計の中間集計（任意）

  * warning count / skipped count / decode error count

### 7.3 再開条件

* `--resume` 時、fingerprint が一致しない場合は中断（`--force-resume` で上書き可）
* 出力が既に存在する場合は、temporary を優先し、完了時に atomic rename

---

## 8. SDK（Rust crate）仕様

### 8.1 提供形態

* `tile_prune` crate（ライブラリ）
* `tile-prune` binary（CLI）
* CLI は SDK を薄く呼ぶだけ（ロジックは SDK に集中）

### 8.2 SDK の主要 API（案）

* `open_reader(input: Path, format: InputFormat) -> TileReader`
* `open_writer(output: Path, format: OutputFormat, options: WriterOptions) -> TileWriter`
* `run_pipeline(reader, writer, pipeline: PipelineOptions) -> RunReport`

主要 trait（差し替え可能性のため）：

* `TileSource`（列挙と read）
* `TileSink`（write）
* `StyleInterpreter`（style→(z,source-layer)->predicate 群）
* `FilterEvaluator`（expression eval）
* `SimplifyEngine`（simplify 実装）

---

## 9. 実装技術選定（言語・主要ライブラリ）

### 9.1 言語

* **Rust**（単一バイナリ、並列・I/O・安全性、クロスプラットフォーム配布に適するため）

### 9.2 主要ライブラリ（推奨）

* CLI: `clap`
* 並列: `rayon`（データ並列の基盤）
* チャネル/バックプレッシャ: `crossbeam-channel`
* SQLite（MBTiles）: `rusqlite` + `bundled`（SQLite 同梱でポータビリティ重視）
* PMTiles v3: `pmtiles` crate（v3 実装）
* Protobuf（MVT decode/encode）: `prost`
* gzip: `flate2`
* ログ/計測: `tracing` + `tracing-subscriber`
* JSON: `serde` + `serde_json`

---

## 10. 互換性（vt-optimizer 準拠点）

* MBTiles のタイル座標・メタデータ取扱いは vt-optimizer に準拠
* `--max-tile-bytes` 超過は警告（処理継続）
* simplify は挙動が変わりすぎない方向（実装速度優先、後で改善）

---

## 11. エラー処理 / フォールトトレランス

* タイル decode 失敗：

  * 既定：警告カウントし、**元タイルをそのまま出力**（変換のみの場合）
  * `--on-decode-error drop|keep|abort`（既定 keep）
* style 解釈失敗：

  * `--style-mode layer+filter` で失敗した場合でも、保守的に「残す」を選べる
* checkpoint：

  * writer が commit した時点で checkpoint を進める（少なくとも “出力に永続化された単位” を境界にする）

---

## 12. 出力レポート（共通）

`RunReport`（CLI では json/ndjson で吐ける）：

* 入力/出力概要（format、tile count、size）
* prune：

  * dropped layers count（by name）
  * dropped features count（by layer）
  * kept ratio
* simplify：

  * vertex reduction ratio（by layer）
* warnings：

  * max-tile-bytes exceeded count
  * decode error count
  * unknown filter count

---

## 13. セキュリティ / 安全性

* 入力はローカルファイルのみ（リモート fetch なし）
* SQLite/PMTiles のパースで panic を避け、エラーは structured に返す
* 任意ファイル上書き事故を避けるため、出力は temporary 生成→atomic rename を原則

---

## 14. 既知の未確定事項（実装しながら詰める）

* filter evaluator の対応範囲の優先順位（最頻出演算子から段階的に）
* PMTiles 出力の “巨大 tileset” における index 構築方式（メモリ常駐 vs 外部インデックス）
* simplify のアルゴリズム差し替え（Topology preserving の扱い）
* split-merge（実験プラン）のマージ戦略・メタデータ統合

---

## 付録A: 仕様参照（読み替えポリシー）

* MBTiles の TMS 反転は、MBTiles で一般に前提とされる（Y を反転する）扱いに合わせる。
* Style filter の意味論（一致 feature のみ表示、整数 zoom、feature-state 非対応）は Mapbox/MapLibre の記述に合わせる。
* PMTiles は v3 を正とし、read-only である点に留意して出力設計を行う。
