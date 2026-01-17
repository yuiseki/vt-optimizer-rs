# MILESTONE.md — vt-optimizer-rs

* Status: Draft (living milestones)

このファイルには、超細かいマイルストーンを集約する。

## マイルストーン 0.4.6

* inspect で `--max-tile-bytes` 超過タイル数の厳密カウントを追加（MBTiles/PMTiles）


## マイルストーン 0.4.5

* `--unknown-filter drop|keep` を追加
* `--style-mode none` を削除


## マイルストーン 0.4.4

* `inspect --tile --summary` の Tile Summary に **タイルサイズ** を追加
* `inspect` の PMTiles 出力セクションを MBTiles と揃える（Summary/Histogram/Top Tiles 等）
* `inspect` の PMTiles でも進捗表示を有効化（推定値が取れる場合はバー、難しい場合はスピナー）


## マイルストーン 0.4.3

* `inspect` の text 出力に Top 10 big tiles セクションを追加
* `inspect` で `-z/-x/-y` 指定時に Tile Summary を表示


## マイルストーン 0.4.2

* `inspect` の text 出力で Zoom セクションをテーブル表示に変更（%tiles/%size/acc% 追加）
* `inspect` の text 出力から Histogram by Zoom セクションを除外し、Histogram に注意書きを追加


## マイルストーン 0.1.5

* `inspect --layers <name1,name2,...>` を追加
  * file layers / tile summary を指定レイヤのみ表示
  * `--layer` は非推奨エイリアスとして `--layers` に統合


## マイルストーン 0.1.4

* `inspect --stats <list>` で出力セクションを選別
  * text/json/ndjson で一貫して適用
  * `--output` の種類に関わらず指定した項目のみ出力


## マイルストーン 0.1.3

* `inspect --tile --summary` の出力粒度を制御する `--tile-info-format <full|compact>` を追加
  * `compact` は keys の一覧を省略（text/json/ndjson 共通）
  * 既定は `full`


## マイルストーン 0.1.2

* `inspect --tile --summary` の **各レイヤー統計** を拡張
  * `# of vertices`（頂点数）
  * `# of values`（property value のユニーク数）
* `--recommend` の top tile summaries にも同一情報を含める


## マイルストーン 0.1.1

* `inspect --tile --summary` の tile summary に **タイル全体の統計** を追加
  * Layers in this tile（layer 数）
  * Vertices in this tile（頂点数合計）
  * Keys in this tile（property key のユニーク数）
  * Values in this tile（property value のユニーク数）
* text / json / ndjson の出力に反映


## マイルストーン 0.0.58

* vt-compat と vt-optimizer の出力比較
  * monaco + osm-fiord で削除数と削除レイヤーが一致


## マイルストーン 0.0.56

* MBTiles の map/images スキーマ対応
  * tiles が無い場合は map/images から読み取る
  * optimize/copy/inspect で map/images を扱える

---


## マイルストーン 0.0.55

* vt-optimizer 互換モード（filter 無視）を追加
  * `--style-mode vt-compat` は `layer` と同義で可視 layer のみを判定

---


## マイルストーン 0.0.54

* 未対応式の keep 方針を明確化
  * unknown filter は keep
  * unknown filter のレイヤー別件数を出力

---


## マイルストーン 0.0.53

* legacy `!` 記法の検証
  * `["!", ["==", ...]]` の評価を固定

---


## マイルストーン 0.0.52

* 実タイル検証（monaco）
  * MBTiles/PMTiles の optimize を実行して出力の統計を比較

---


## マイルストーン 0.0.51

* 同一 source-layer の結合ルール確定
  * 可視 layer のみ filter を評価して OR 結合
  * minzoom/maxzoom と整合した判定

---


## マイルストーン 0.0.50

* filter 式の中核拡張
  * `match` / `case` / `coalesce` を評価

---


## マイルストーン 0.0.49

* filter 式の基礎拡張
  * `get` 参照をサポート
  * `in` / `any` との組み合わせを評価

---


## マイルストーン 0.0.48

* optimize の PMTiles 出力整合
  * metadata を引き継ぎ
  * tile_compression を入力に合わせる

---


## マイルストーン 0.0.47

* optimize の PMTiles 入出力対応（同一フォーマットのみ）
  * PMTiles を直接 optimize して PMTiles に出力可能

---


## マイルストーン 0.0.46

* inspect の見出し色を緑に変更

---


## マイルストーン 0.0.45

* inspect の text 出力を ANSI で装飾
  * セクション見出しは太字
  * テーブルヘッダーは色付き

---


## マイルストーン 0.0.44

* inspect Layers の出力拡張
  * テーブル列: name / # of vertices / # of features / # of keys / # of values
  * 頂点数・属性値数を集計

---


## マイルストーン 0.0.43

* prune のログ出力強化（vt-optimizer 相当の情報量）
  * 処理ステップの明示（style 読込 / tiles 処理 / 出力）
  * zoom 別の削除 feature 数
  * zoom 別の削除 layer 名

---


## マイルストーン 0.0.42

* 追加の filter 記法
  * legacy `["!", ...]` の扱い
  * 未対応式の keep 方針は固定（UNKNOWN=keep）

---


## マイルストーン 0.0.41

* 実タイル検証と guard
  * osm-fiord.json で実サンプル検証
  * UNKNOWN 式の集計ログを出力

---


## マイルストーン 0.0.40

* filter の結合ルールを明確化
  * 同一 source-layer 複数 style layer は OR で keep
  * `layout.visibility: "none"` は常に drop
  * paint が非表示なら filter を飛ばして drop（常時）

---


## マイルストーン 0.0.39

* filter と zoom の結合
  * `["zoom"]` 参照がある場合のみ評価（整数 zoom）
  * minzoom/maxzoom と整合（可視 layer のみ filter を評価）

---


## マイルストーン 0.0.38

* prune の filter 最小対応
  * `==`, `!=`, `in`, `!in`, `has`, `!has`, `all`, `any`, `none`
  * `$type` の `Point|LineString|Polygon` 判定をサポート
  * 未対応式は UNKNOWN として keep（安全側）

---


## マイルストーン 0.0.37

* prune の layer 可視性判定を強化（vt-optimizer 準拠）
  * `layout.visibility` + `minzoom/maxzoom` を反映
  * `PaintPropertiesToCheck` による描画判定（0 判定 / stops 対応）
  * `source-layer` が複数 style layer に存在する場合は OR 判定

---


## マイルストーン 0.0.36

* prune の最小導入（layer pruning のみ）
  * style JSON から `source-layer` 一覧を抽出
  * MVT から未使用レイヤーを削除（feature は削除しない）
  * `--style-mode layer` 相当の挙動
  * 対象出力は MBTiles のみ（PMTiles は後続）

---


## マイルストーン 0.0.35

* PMTiles の layer 解析（text 出力の Layers セクション相当）

---


## マイルストーン 0.0.34

* PMTiles の zoom 別ヒストグラム（Histogram by Zoom）を `inspect` に追加

---


## マイルストーン 0.0.33

* PMTiles のサイズ統計（max/avg/total）を `inspect` に反映
* `--max-tile-bytes` による警告判定（PMTiles）

---


## マイルストーン 0.0.32

* PMTiles のタイル列挙により tiles count / zoom 別集計を出力

---


## マイルストーン 0.0.31

* `inspect` が `.pmtiles` 入力を受け付ける（入力判定/CLI で拒否しない）
* PMTiles のメタデータ取得（name/minzoom/maxzoom/format など）

---


## マイルストーン 0.0.30

* text 出力で warning を色で強調（near=黄色, over=赤）

---


## マイルストーン 0.0.29

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


## マイルストーン 0.0.28

* text 出力にファイルメタデータ（name/bounds/minzoom/maxzoom/format など）を表示

---


## マイルストーン 0.0.27

* Histogram に acc%size を追加

---


## マイルストーン 0.0.26

* text 出力で zoom ごとの histogram を並べる

---


## マイルストーン 0.0.25

* text 出力で警告を強調表示（near/over を記号で強調）

---


## マイルストーン 0.0.24

* text 出力のセクション整理（Summary / Zoom / Histogram / Tiles / Layers）

---


## マイルストーン 0.0.23

* text 出力の単位整形（bytes → KB/MB）と桁揃え

---


## マイルストーン 0.0.22

* `--tile --summary` の text 出力に property key 名の一覧を追加

---


## マイルストーン 0.0.21

* inspect text にファイル全体の layer list を追加（name / features / property key 数）

---


## マイルストーン 0.0.20

* `--ndjson-compact` で `--output ndjson` を暗黙指定

---


## マイルストーン 0.0.19

* compact 時は summary を強制省略

---


## マイルストーン 0.0.18

* `--ndjson-compact` を追加（NDJSON の最小化）

---


## マイルストーン 0.0.17

* NDJSON の順序を安定化（zoom histogram / recommended buckets のソート）

---


## マイルストーン 0.0.16

* `--ndjson-lite` を追加（summary 行の省略）

---


## マイルストーン 0.0.15

* NDJSON の tile 出力を1行1tileに分割（bucket/top）

---


## マイルストーン 0.0.14

* NDJSON を分割出力（zoom ごとの histogram / summary を行単位に）

---


## マイルストーン 0.0.13

* `--output ndjson` を追加（inspect の NDJSON 出力）

---


## マイルストーン 0.0.12

* `--output json` で zoom 別 histogram + tile list + summary を出力
* 大量 tile 向けに NDJSON を検討

---


## マイルストーン 0.0.11

* `inspect --zoom <z> --sample <ratio|count>` を zoom 内 sampling に適用
* `--fast` プリセット（sample + topn + histogram を固定）

---


## マイルストーン 0.0.10

* `inspect --zoom <z> --recommend` で高リスク bucket を自動抽出
* topN tile + summary の連結表示

---


## マイルストーン 0.0.9

* zoom 別 histogram に以下を追加
  * Running avg size
  * % of tiles
  * % of level size
  * Accum % tiles / size
* `--max-tile-bytes` を基準に warning 判定（文字で表示）

---


## マイルストーン 0.0.8

* tile summary を詳細化
  * layer ごとの property key 数
  * total feature count
* `--layer <name>` で layer 指定の絞り込み

---


## マイルストーン 0.0.7

* MVT decode 導入（最小: layer 名 / feature 数）
* `inspect --tile z/x/y --summary` で layer ごとの feature 数

---


## マイルストーン 0.0.6

* `inspect --zoom <z> --bucket <i> --list-tiles` で z/x/y 一覧
* `--limit <n>` で一覧件数制限
* `--sort size|zxy` で順序指定（既定: size desc）

---


## マイルストーン 0.0.5

* `inspect --zoom <z>` で zoom 限定集計（tiles/total/max/avg）
* `inspect --zoom <z> --histogram-buckets <n>` の zoom 別ヒストグラム
* `inspect --zoom <z> --bucket <i>` の bucket 内 tile 数のみ出力

---


## マイルストーン 0.0.4

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


## マイルストーン 0.0.3

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


## マイルストーン 0.0.2

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


## マイルストーン 0.0.1

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
