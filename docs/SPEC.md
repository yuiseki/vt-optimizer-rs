# SPEC.md — vt-optimizer-rs

* Project: **vt-optimizer-rs**
* Type: Single-binary CLI + Rust SDK (library)
* Status: Draft (living spec)
* Compatibility baseline: vt-optimizer 準拠（入出力境界・メタデータ方針・警告方針・簡略化の挙動の方向性）

## 0. 概要

vt-optimizer-rs は、**Mapbox Vector Tiles (MVT)** を格納した **MBTiles / PMTiles** を対象に、以下を行うツールチェイン（CLI/SDK）です。

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

## 1.5 マイルストーン（v0.0.3）

* `copy` の拡張
  * MBTiles → PMTiles（最小構成での書き出し）
  * PMTiles → MBTiles（最小構成での読み取り）
* `inspect` の拡張
  * zoom 別の tile count / total bytes / max bytes を出力
* `optimize` はフォーマット決定のみ（処理は未実装）

v0.0.3 では以下を **含めない**。

* style/filter の解釈
* simplify 実装
* sidecar / checkpoint / JSON レポート

---

## 1.6 マイルストーン（v0.0.4）

* `inspect` の実用化
  * `--sample <ratio|count>` による高速化
  * `--topn <k>` による最大タイル上位表示
  * `--histogram-buckets <n>` によるサイズ分布
  * `--output json` による JSON レポート
  * 空タイル率（サイズ閾値ベース）の報告
  * 進捗表示の有効化（`--no-progress` で無効化）

v0.0.4 では以下を **含めない**。

* style/filter の解釈
* simplify 実装
* sidecar / checkpoint / JSON レポートの詳細拡張

---

## 1.7 マイルストーン（v0.0.5）

* `inspect --zoom <z>` で zoom 限定集計（tiles/total/max/avg）
* `inspect --zoom <z> --histogram-buckets <n>` の zoom 別ヒストグラム
* `inspect --zoom <z> --bucket <i>` の bucket 内 tile 数のみ出力

---

## 1.8 マイルストーン（v0.0.6）

* `inspect --zoom <z> --bucket <i> --list-tiles` で z/x/y 一覧
* `--limit <n>` で一覧件数制限
* `--sort size|zxy` で順序指定（既定: size desc）

---

## 1.9 マイルストーン（v0.0.7）

* MVT decode 導入（最小: layer 名 / feature 数）
* `inspect --tile z/x/y --summary` で layer ごとの feature 数

---

## 1.10 マイルストーン（v0.0.8）

* tile summary を詳細化
  * layer ごとの property key 数
  * total feature count
* `--layer <name>` で layer 指定の絞り込み

---

## 1.11 マイルストーン（v0.0.9）

* zoom 別 histogram に以下を追加
  * Running avg size
  * % of tiles
  * % of level size
  * Accum % tiles / size
* `--max-tile-bytes` を基準に warning 判定（文字で表示）

---

## 1.12 マイルストーン（v0.0.10）

* `inspect --zoom <z> --recommend` で高リスク bucket を自動抽出
* topN tile + summary の連結表示

---

## 1.13 マイルストーン（v0.0.11）

* `inspect --zoom <z> --sample <ratio|count>` を zoom 内 sampling に適用
* `--fast` プリセット（sample + topn + histogram を固定）

---

## 1.14 マイルストーン（v0.0.12）

* `--output json` で zoom 別 histogram + tile list + summary を出力
* 大量 tile 向けに NDJSON を検討

---

## 1.15 マイルストーン（v0.0.13）

* `--output ndjson` を追加（inspect の NDJSON 出力）

---

## 1.16 マイルストーン（v0.0.14）

* NDJSON を分割出力（zoom ごとの histogram / summary を行単位に）

---

## 1.17 マイルストーン（v0.0.15）

* NDJSON の tile 出力を1行1tileに分割（bucket/top）

---

## 1.18 マイルストーン（v0.0.16）

* `--ndjson-lite` を追加（summary 行の省略）

---

## 1.19 マイルストーン（v0.0.17）

* NDJSON の順序を安定化（zoom histogram / recommended buckets のソート）

---

## 1.20 マイルストーン（v0.0.18）

* `--ndjson-compact` を追加（NDJSON の最小化）

---

## 1.21 マイルストーン（v0.0.19）

* compact 時は summary を強制省略

---

## 1.22 マイルストーン（v0.0.20）

* `--ndjson-compact` で `--output ndjson` を暗黙指定

---

## 1.23 マイルストーン（v0.0.21）

* inspect text にファイル全体の layer list を追加（name / features / property key 数 / extent / version）

---

## 1.24 マイルストーン（v0.0.22）

* `--tile --summary` の text 出力に property key 名の一覧を追加

---

## 1.25 マイルストーン（v0.0.23）

* text 出力の単位整形（bytes → KB/MB）と桁揃え

---

## 1.26 マイルストーン（v0.0.24）

* text 出力のセクション整理（Summary / Zoom / Histogram / Tiles / Layers）

---

## 1.27 マイルストーン（v0.0.25）

* text 出力で警告を強調表示（near/over を記号で強調）

---

## 1.28 マイルストーン（v0.0.26）

* text 出力で zoom ごとの histogram を並べる

---

## 1.29 マイルストーン（v0.0.27）

* Histogram に acc%size を追加

---

## 1.30 マイルストーン（v0.0.28）

* text 出力にファイルメタデータ（name/bounds/minzoom/maxzoom/format など）を表示

---

## 1.31マイルストーン（v0.0.29）

* サンプリング使用時の大幅な高速化
  * counting tiles の COUNT(*) クエリをスキップ（サンプリング時は不要）
  * ヒストグラム構築をメインループで収集したデータから実行（追加スキャン不要）
  * ズーム別ヒストグラムをスキップ（サンプリング時は全体スキャンが必要なため）
  * レイヤー情報収集をメインループ内で実行（追加スキャン不要）
* プログレスバーとスピナーの改善
  * サンプリング時はプログレスバーの代わりにスピナーを表示
  * スピナーアニメーション改善（Unicode文字、20Hz描画、80ms更新）
  * プログレスバー更新頻度を100タイルごとに変更（1000タイルごとから短縮）
  * スピナーとプログレスバーをシアン色で表示

---

## 1.32 マイルストーン（v0.0.30）

* text 出力で warning を色で強調（near=黄色, over=赤）

---

## 1.33 マイルストーン（v0.0.31）

* `inspect` が `.pmtiles` 入力を受け付ける（入力判定/CLI で拒否しない）
* PMTiles のメタデータ取得（name/minzoom/maxzoom/format など）

---

## 1.34 マイルストーン（v0.0.32）

* PMTiles のタイル列挙により tiles count / zoom 別集計を出力

---

## 1.35 マイルストーン（v0.0.33）

* PMTiles のサイズ統計（max/avg/total）を `inspect` に反映
* `--max-tile-bytes` による警告判定（PMTiles）

---

## 1.36 マイルストーン（v0.0.34）

* PMTiles の zoom 別ヒストグラム（Histogram by Zoom）を `inspect` に追加

---

## 1.37 マイルストーン（v0.0.35）

* PMTiles の layer 解析（text 出力の Layers セクション相当）

---

## 1.38 マイルストーン（v0.0.36）

* prune の最小導入（layer pruning のみ）
  * style JSON から `source-layer` 一覧を抽出
  * MVT から未使用レイヤーを削除（feature は削除しない）
  * `--style-mode layer` 相当の挙動
  * 対象出力は MBTiles のみ（PMTiles は後続）

---

## 1.39 マイルストーン（v0.0.37）

* prune の layer 可視性判定を強化（vt-optimizer 準拠）
  * `layout.visibility` + `minzoom/maxzoom` を反映
  * `PaintPropertiesToCheck` による描画判定（0 判定 / stops 対応）
  * `source-layer` が複数 style layer に存在する場合は OR 判定

---

## 1.40 マイルストーン（v0.0.38）

* prune の filter 最小対応
  * `==`, `!=`, `in`, `!in`, `has`, `!has`, `all`, `any`, `none`
  * `$type` の `Point|LineString|Polygon` 判定をサポート
  * 未対応式は UNKNOWN として keep（安全側）

---

## 1.41 マイルストーン（v0.0.39）

* filter と zoom の結合
  * `["zoom"]` 参照がある場合のみ評価（整数 zoom）
  * minzoom/maxzoom と整合（可視 layer のみ filter を評価）

---

## 1.42 マイルストーン（v0.0.40）

* filter の結合ルールを明確化
  * 同一 source-layer 複数 style layer は OR で keep
  * `layout.visibility: "none"` は常に drop
  * paint が非表示なら filter を飛ばして drop

---

## 1.43 マイルストーン（v0.0.41）

* 実タイル検証と guard
  * osm-fiord.json で実サンプル検証
  * UNKNOWN 式の集計ログを出力

---

## 1.44 マイルストーン（v0.0.42）

* 追加の filter 記法
  * legacy `["!", ...]` の扱い
  * 未対応式の keep 方針は固定（UNKNOWN=keep）

---

## 1.45 マイルストーン（v0.0.43）

* prune のログ出力強化（vt-optimizer 相当の情報量）
  * 処理ステップの明示（style 読込 / tiles 処理 / 出力）
  * zoom 別の削除 feature 数
  * zoom 別の削除 layer 名

---

## 1.46 マイルストーン（v0.0.44）

* inspect Layers の出力拡張
  * テーブル列: name / # of vertices / # of features / # of keys / # of values
  * 頂点数・属性値数を集計

---

## 1.47 マイルストーン（v0.0.45）

* inspect の text 出力を ANSI で装飾
  * セクション見出しは太字
  * テーブルヘッダーは色付き

---

## 1.48 マイルストーン（v0.0.46）

* inspect の見出し色を緑に変更

---

## 1.49 マイルストーン（v0.0.47）

* optimize の PMTiles 入出力対応
  * PMTiles を直接 prune して出力可能

---

## 1.50 マイルストーン（v0.0.48）

* optimize の PMTiles 出力整合
  * metadata を引き継ぎ
  * tile_compression を入力に合わせる

---

## 1.51 マイルストーン（v0.0.49）

* filter 式の基礎拡張
  * `get` 参照をサポート
  * `in` / `any` との組み合わせを評価

---

## 1.52 マイルストーン（v0.0.50）

* filter 式の中核拡張
  * `match` / `case` / `coalesce` を評価

---

## 1.53 マイルストーン（v0.0.51）

* 同一 source-layer の結合ルール確定
  * 可視 layer のみ filter を評価して OR 結合
  * minzoom/maxzoom と整合した判定

---

## 1.54 マイルストーン（v0.0.52）

* 実タイル検証（monaco）
  * MBTiles/PMTiles の optimize を実行して出力の統計を比較

---

## 1.55 マイルストーン（v0.0.53）

* legacy `!` 記法の検証
  * `["!", ["==", ...]]` の評価を固定

---

## 1.56 マイルストーン（v0.0.54）

* 未対応式の keep 方針を明確化
  * unknown filter は keep
  * unknown filter のレイヤー別件数を出力

---

## 1.57 マイルストーン（v0.0.55）

* vt-optimizer 互換モード（filter 無視）を追加
  * `--style-mode vt-compat` で可視 layer のみを判定

---

## 1.58 マイルストーン（v0.0.56）

* MBTiles の map/images スキーマ対応
  * tiles が無い場合は map/images から読み取る
  * optimize/copy/inspect で map/images を扱える

---

## 1.59 マイルストーン（v0.0.58）

* vt-compat と vt-optimizer の出力比較
  * monaco + osm-fiord で削除数と削除レイヤーが一致

## 1.60 マイルストーン（v0.1.1）

* `inspect --tile --summary` の tile summary に **タイル全体の統計** を追加
  * Layers in this tile（layer 数）
  * Vertices in this tile（頂点数合計）
  * Keys in this tile（property key のユニーク数）
  * Values in this tile（property value のユニーク数）
* text / json / ndjson の出力に反映

## 1.61 マイルストーン（v0.1.2）

* `inspect --tile --summary` の **各レイヤー統計** を拡張
  * `# of vertices`（頂点数）
  * `# of values`（property value のユニーク数）
* `--recommend` の top tile summaries にも同一情報を含める

## 1.62 マイルストーン（v0.1.3）

* `inspect --tile --summary` の出力粒度を制御する `--tile-info-format <full|compact>` を追加
  * `compact` は keys の一覧を省略（text/json/ndjson 共通）
  * 既定は `full`

## 1.63 マイルストーン（v0.1.4）

* `inspect --stats <list>` で出力セクションを選別
  * text/json/ndjson で一貫して適用
  * `--output` の種類に関わらず指定した項目のみ出力

## 1.64 マイルストーン（v0.1.5）

* `inspect --layers <name1,name2,...>` を追加
  * file layers / tile summary を指定レイヤのみ表示
  * `--layer` は非推奨エイリアスとして `--layers` に統合

## 1.65 マイルストーン（v0.4.2）

* `inspect` の text 出力で Zoom セクションをテーブル表示に変更（%tiles/%size/acc% 追加）
* `inspect` の text 出力から Histogram by Zoom セクションを除外し、Histogram に注意書きを追加

## 1.66 マイルストーン（v0.4.3）

* `inspect` の text 出力に Top 10 big tiles セクションを追加

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
vt-optimizer <command> [options] <input> [<output>]
```

互換 CLI（legacy）として、subcommand を省略した実行も許可する。

```
vt-optimizer -m <mbtiles> [-s <style.json>] [-o <output>] [-z <z> -x <x> -y <y> [-l <layer>] [-t <tolerance>]]
```

`<command>`:

* `inspect` : 統計・分布・サンプリング
* `prune`   : style ベース最適化（レイヤー削除 + feature 削除）
* `simplify`: ジオメトリ簡略化
* `copy`    : 変換のみ（MBTiles⇄PMTiles、再圧縮/正規化含む、任意）

互換 CLI の挙動:

* `-m` のみ: `inspect` と同等（text）
* `-m -s` / `-m -s -o`: `prune` 相当（style-mode=vt-compat）
* `-m -z -x -y` (+ `-l` / `-t`): `simplify` 相当として受理

注記:

* `-z/-x/-y/-l/-t` の simplify 相当は CLI で受理し、実処理への接続は段階的に実装する（仕様としては保持）。

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
* `--readers <n>`: 読み取りスレッド数（デフォルトは `--threads` と同等）
* `--io-batch <n>`: 読み取り/処理キューの上限（タイル件数）
* `--read-cache-mb <mb>`: 読み取り側 SQLite cache サイズ（MB）
* `--write-cache-mb <mb>`: 書き込み側 SQLite cache サイズ（MB）
* `--drop-empty-tiles`: prune 後に空タイルを出力しない（サイズ削減優先）
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
* ただし checkpoint/restart や失敗局所化の利点があるため、ADR 側で評価を継続

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
* `vt-optimizer` binary（CLI）
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
