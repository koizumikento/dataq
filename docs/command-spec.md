# dataq Command Spec

## コマンド一覧

共通形式:

```bash
dataq [--emit-pipeline] <command> [options]
```

サブコマンド:

- `canon`: 入力を決定的に正規化し、JSON/JSONLへ変換
- `assert`: ルールまたはJSON Schemaで検証
- `sdiff`: 2データセットの構造差分を出力
- `profile`: フィールド統計を決定的JSONで出力
- `merge`: base + overlays をポリシーマージ
- `doctor`: `jq` / `yq` / `mlr` の実行前診断
- `recipe run`: 宣言的レシピを定義順に実行

## `profile` 出力契約

- 既存キーは固定: `record_count`, `field_count`, `fields`, `type_distribution`
- `fields.<canonical-path>.numeric_stats` は後方互換な追加キー（数値サンプルが存在するときのみ出力）
- `numeric_stats` スキーマ:
  - `count`
  - `min`
  - `max`
  - `mean`
  - `p50`
  - `p95`
- 数値サンプル抽出対象は JSON number のみ
- パーセンタイル規則は nearest-rank で固定:
  - `rank = ceil(p * n)`（`p` は 0.50 / 0.95）
  - `index = rank - 1`（0始まり）
- `numeric_stats` の浮動小数は小数点以下6桁に丸めて出力

## このCLIの位置づけ

- `dataq` は `jq` / `yq` / `mlr` の代替ではなく、運用で繰り返す複合処理を短いコマンドに固定するための契約CLI
- 直接パイプ（`yq ... | jq ... | mlr ...`）で起きやすいI/O揺れ・終了コード揺れを、`dataq` 側で吸収して統一
- 探索や一時分析は各ツール単体、本番運用パイプラインは `dataq` で固定化する使い分けを想定

## `assert` 補助出力

- `dataq assert --rules-help` で `--rules` 用ルール仕様を機械可読JSONで出力
- `dataq assert --schema-help` で `--schema`（JSON Schema検証）の使い方と結果契約を機械可読JSONで出力
- このモードは検証処理を実行せず、終了コード `0` で終了
- `dataq assert --normalize github-actions-jobs|gitlab-ci-jobs` で生のCI定義を `yq -> jq -> mlr` の固定3段でジョブ単位レコードへ正規化してから `--rules` 検証可能（`yq`/`jq`/`mlr` 必須）

## 外部ツール多段連携（契約方針）

- 多段連携コマンドは、内部で `jq` / `yq` / `mlr` の1つ以上を段階実行して1つの結果JSONを返す
- 各段は役割を分離する:
  - `yq`: YAML抽出/整形
  - `jq`: JSON正規化/判定フラグ付け
  - `mlr`: 集計/結合/統計
- 使用段数や順序は機能ごとに定義し、CLI契約の一部として `--emit-pipeline` で追跡可能にする

## CLI I/O 契約

### 出力モード

- 既定: JSON（機械可読）
- `canon` のみ `--to jsonl` で JSONL 出力を選択可能
- `canon --to jsonl` かつ JSONL入力はレコード単位の逐次処理（出力順は入力順）

### `canon` 入力フォーマット解決

- `--from` 指定時は指定フォーマットを使用
- `--from` 未指定かつ `--input <path>` 指定時は拡張子で解決（`.json|.yaml|.yml|.csv|.jsonl|.ndjson`）
- `--from` 未指定かつ stdin入力時は固定順で自動判別:
  - `JSONL -> JSON -> YAML -> CSV`
- 自動判別失敗は `input_usage_error` で終了コード `3`

### 終了コード

- `0`: 成功
- `2`: 検証失敗（期待仕様に不一致）
- `3`: 入力不正（フォーマット不正、必須引数不足など）または `doctor` の必須ツール不足/起動不可
- `1`: その他実行時エラー

## `doctor` コマンド契約（MVP）

- コマンド: `dataq doctor`
- 出力: JSON（stdout）
- 診断対象ツール順: `jq`, `yq`, `mlr`（固定順）
- 各ツールの出力項目:
  - `name`: ツール名
  - `found`: PATH上に存在するか
  - `version`: 取得できたバージョン文字列（取得不可時は `null`）
  - `executable`: `--version` で起動できたか
  - `message`: 判定理由（失敗時は対処案内を含む）
- 終了コード:
  - `0`: 全ツール起動可能
  - `3`: 1つ以上が欠如または起動不可
  - `1`: 予期しない内部エラー
- `--emit-pipeline` 指定時の `steps`: `doctor_probe_jq`, `doctor_probe_yq`, `doctor_probe_mlr`

### `sdiff` のCIゲート拡張

- `--fail-on-diff`（既定: `false`）:
  比較処理が成功し、かつ `values.total > 0` のとき終了コード `2` で終了
- `--value-diff-cap <usize>`（既定: `100`）:
  レポートの `values.items` 出力件数上限を制御
- レポートJSON契約（`counts`, `keys`, `ignored_paths`, `values`）は不変
- `values.total` は実差分件数を維持し、上限超過時のみ `values.truncated=true`
- `--emit-pipeline` のstderr JSON出力契約は `sdiff` 拡張後も不変

### `--emit-pipeline`（診断出力）

- グローバル引数として利用可能: `dataq --emit-pipeline <subcommand> ...`
- サブコマンド側でも利用可能: `dataq <subcommand> ... --emit-pipeline`
- 有効時は stderr に pipeline JSON を1行追加出力
- 本処理の stdout は従来どおり（既存出力互換）

pipeline JSON schema:

- `command`: 実行サブコマンド名
- `input`: 入力ソース情報（stdin/path, format）
- `steps`: 実行ステップ配列
- `external_tools`: `jq|yq|mlr` の使用有無（ツール名順で固定）
- `stage_diagnostics` (optional): 段ごとの診断情報（`order`, `step`, `tool`, `input_records`, `output_records`, `status`）
- `deterministic_guards`: 適用した決定性ガード
- `assert --rules-help`/`--schema-help` では `steps` が `emit_assert_rules_help` / `emit_assert_schema_help` になる
- `recipe run` では `steps` に `load_recipe_file`, `validate_recipe_schema`, `execute_step_<index>_<kind>` が入る

```bash
cat in.json | dataq --emit-pipeline canon --from json > out.json 2> pipeline.json
```

### 外部ツール連携の方針

- `dataq` は外部ツールを運用上の依存として扱い、CLI契約（JSON/終了コード）をRust層で統一する
- ユーザー入力はシェル文字列展開せず、外部ツール連携時も明示的な引数配列で扱う

## `recipe run` MVP スキーマ

- 実行形式: `dataq recipe run --file <path>`
- レシピファイル形式: 拡張子解決で JSON / YAML をサポート
- `version`: `dataq.recipe.v1` 固定
- `steps`: 実行順配列（定義順で処理）
- `steps[*].kind`: `canon | assert | profile | sdiff`
- `steps[*].args`: 各 step の引数オブジェクト
- step 間データ受け渡し: in-memory
- サマリ出力: stdout JSON に `matched`, `exit_code`, `steps`
- 異常時契約:
  - スキーマ不正 / 未知step / 引数不正は exit `3`
  - `assert` / `sdiff` の不一致は exit `2`

## 関連ドキュメント

- 設計方針・構造: [architecture.md](./architecture.md)
- assert ルール仕様: [rules-schema.md](./rules-schema.md)
