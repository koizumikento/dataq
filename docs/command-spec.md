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

```bash
cat in.json | dataq --emit-pipeline canon --from json > out.json 2> pipeline.json
```

### 外部ツール連携の方針

- `dataq` は外部ツールを運用上の依存として扱い、CLI契約（JSON/終了コード）をRust層で統一する
- ユーザー入力はシェル文字列展開せず、外部ツール連携時も明示的な引数配列で扱う

## 関連ドキュメント

- 設計方針・構造: [architecture.md](./architecture.md)
- assert ルール仕様: [rules-schema.md](./rules-schema.md)
