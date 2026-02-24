# dataq

`dataq` は、JSON / YAML / CSV を対象にした「決定的な前処理・検証・差分」CLIです。  
AI処理そのものは行わず、エージェントやCIから呼びやすい機械可読I/Oを提供します。

## これは何か（3行要約）

- `dataq` は `jq` / `yq` / `mlr` の「よく使う組み合わせ」を単一CLIにまとめるための契約レイヤーです
- 実行のオーケストレーションは Rust 側で行い、必要に応じて `jq` / `yq` / `mlr` の多段連携（例: `yq -> jq -> mlr`）を内部実行しつつ、出力JSONと終了コード契約を固定します
- 探索は各ツール単体、運用パイプラインは `dataq` で再利用する使い分けを想定しています

## 目的

- データ変換を再現可能にする（同じ入力なら同じ出力）
- 失敗を終了コードとJSONで明確化する
- `jq` / `yq` / `mlr` を組み合わせた処理を、短い固定コマンドとして再利用可能にする

## 立ち位置（`jq` / `yq` / `mlr` との関係）

| 観点 | dataq | jq / yq / mlr |
| --- | --- | --- |
| 主目的 | よく使う複合パイプラインを契約化して再利用 | 抽出・変換・集計の表現力 |
| 実行モデル | Rustオーケストレータ + 必要時 `jq/yq/mlr` 連携 | 各ツールのDSL/フィルタ実行 |
| 出力契約 | 機械可読JSONを既定、スキーマ化しやすい | フィルタ次第で形式が変動 |
| 終了コード契約 | `0/2/3/1` を意味付きで固定 | ツールごとに意味が異なる |
| 決定性ガード | キー順・時刻正規化・差分順序などを固定 | フィルタ/オプション次第 |
| 診断 | `--emit-pipeline` で内部ステップをJSON出力 | 同等の共通仕様はない |

## 使い分け

- `dataq` を使う場面:
  CI品質ゲート、前処理の再実行保証、チームで共通化したいパイプライン、差分の定常監視
- `jq` / `yq` / `mlr` を使う場面:
  ワンライナー探索、複雑な抽出クエリ、対話的な整形や一時分析
- 併用の考え方:
  探索は `jq` / `yq` / `mlr`、本番の再利用パイプラインは `dataq`（契約を `dataq` 側に寄せる）

## 生パイプラインとの違い

- 生パイプライン:
  `yq ... | jq ... | mlr ...` のように都度書けるが、引数差分・エラー解釈・終了コードが揺れやすい
- `dataq`:
  同等の処理意図をサブコマンド化し、I/O形式・失敗JSON・終了コードを固定できる
- 監査性:
  `--emit-pipeline` で、内部処理ステップ・外部ツール使用有無・`stage_diagnostics`（段ごとの順序/件数/バイト数/`duration_ms`(決定性保持のため常に`0`)/状態）に加えて、`fingerprint`（`args_hash` / `input_hash`(optional) / 使用ツール版数 / `dataq_version`）を機械可読で残せる

## 外部ツール多段連携の方針

- `dataq` の一部コマンドは、内部的に複数ツールを段階実行することで価値を成立させます
- これは「3ツールの代替」ではなく「3ツールの合わせ技を再利用可能な契約として固定する」ための設計です
- 多段連携コマンドでは、`--emit-pipeline` で各段の利用ツール・ステップ順・件数/バイト数変化・`duration_ms`(決定性保持のため常に`0`)・失敗段を追跡可能にします

## コマンド一覧

共通形式:

```bash
dataq [--emit-pipeline] <command> [options]
```

サブコマンド一覧（`./target/debug/dataq --help` ベース）:

| Command | 用途 | 必須オプション |
| --- | --- | --- |
| `canon` | 入力を決定的に正規化し、JSON/JSONLへ変換 | `--from <json|yaml|csv|jsonl>`（stdin時は省略可） |
| `assert` | ルール or JSON Schema で検証 | `--rules <path>` または `--schema <path>` |
| `gate schema` | JSON Schema で品質ゲートを実行（`assert --schema` の専用ラッパー） | `--schema <path>` |
| `gate policy` | ルールベース品質ゲートを実行（違反詳細を決定的順序で出力） | `--rules <path>` |
| `sdiff` | 2データセットの構造差分を出力 | `--left <path>` `--right <path>` |
| `diff source` | 2ソース（preset/path）を解決して構造差分を出力 | `--left <preset-or-path>` `--right <preset-or-path>` |
| `profile` | フィールド統計を決定的JSONで出力 | `--from <json|yaml|csv|jsonl>` |
| `join` | 2入力をキー結合してJSON配列を出力 | `--left <path>` `--right <path>` `--on <field>` `--how <inner|left>` |
| `aggregate` | グループ単位の集計をJSON配列で出力 | `--input <path>` `--group-by <field>` `--metric <count|sum|avg>` `--target <field>` |
| `merge` | base + overlays をポリシーマージ | `--base <path>` `--overlay <path>...` `--policy <last-wins|deep-merge|array-replace>` `--policy-path <path=policy>...` |
| `doctor` | 依存診断（`--capabilities`/`--profile` 対応） | なし |
| `recipe run` | 宣言的レシピを定義順で実行 | `--file <path>` |
| `recipe lock` | レシピ再現実行用のロック情報を生成 | `--file <path>` |
| `contract` | サブコマンド出力契約を機械可読JSONで取得 | `--command <name>` または `--all` |
| `emit plan` | サブコマンドの静的実行計画（stage/dependency/tool）を出力 | `--command <name>` |
| `mcp` | 1リクエスト単位の MCP(JSON-RPC 2.0) サーバーモード | stdin で JSON-RPC リクエストを1件入力 |

グローバルオプション:

- `--emit-pipeline`: stderr に pipeline JSON を1行追加出力（`fingerprint` を含む）
- `-h, --help`: ヘルプ
- `-V, --version`: バージョン

## 基本的な使い方

```bash
# YAMLを正規化してJSONLへ
cat in.yaml | dataq canon --from yaml --to jsonl > out.jsonl

# stdin入力は --from 省略時に JSONL -> JSON -> YAML -> CSV の順で自動判別
# ただし非空行が1行のみで全体がJSONとして成立する場合は JSON を優先（曖昧さ回避）
cat events.jsonl | dataq canon --to jsonl > out.jsonl

# ルール検証
dataq assert --input out.jsonl --rules rules.yaml

# JSON Schema 検証
dataq assert --input out.jsonl --schema schema.json

# schema 専用ゲート（assert --schema からの移行先）
dataq gate schema --input out.jsonl --schema schema.json

# policy 専用ゲート（rules 検証 + violation 出力）
dataq gate policy --input out.jsonl --rules rules.json --source scan-text

# 差分確認
dataq sdiff --left before.jsonl --right after.jsonl

# 品質プロファイル
dataq profile --from json --input out.jsonl

# 内部結合（idキー）
dataq join --left users.json --right scores.json --on id --how inner

# グループ集計（team単位でprice平均）
dataq aggregate --input orders.json --group-by team --metric avg --target price

# ポリシーマージ
dataq merge --base base.yaml --overlay patch1.json --overlay patch2.yaml --policy deep-merge

# 依存ツール診断
dataq doctor

# 依存ツールの機能診断
dataq doctor --capabilities

# ワークフロー別プリフライト（例: scan）
dataq doctor --profile scan

# assert 出力契約を取得
dataq contract --command assert

# assert の静的ステージ計画を取得
dataq emit plan --command assert --args '["--normalize","github-actions-jobs"]'

# MCP単発リクエスト（tools/list）
printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}' | dataq mcp

# ID で対応付けし、更新時刻は差分対象外
dataq sdiff --left before.jsonl --right after.jsonl --key '$["id"]' --ignore-path '$["updated_at"]'

# CIゲート: 差分があれば終了コード2、値差分詳細は先頭1件まで
dataq sdiff --left before.jsonl --right after.jsonl --fail-on-diff --value-diff-cap 1

# CI定義を preset 経由で正規化して差分比較
dataq diff source \
  --left 'preset:github-actions-jobs:.github/workflows/ci.yml' \
  --right expected-jobs.json \
  --fail-on-diff

# JSON入力をそのままdataqで検証
dataq assert --input raw.json --rules rules.yaml
```

## OSS基本情報

### インストール

```bash
cargo install --path .
```

### 開発（ローカル検証）

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

### Release

- `v*` タグ（例: `v0.1.0`, `v0.1.0-rc.1`）を push すると、GitHub Actions の Release workflow が起動します
- workflow は `cargo fmt --all -- --check`、`cargo clippy --workspace --all-targets --all-features -- -D warnings`、`cargo test --workspace --all-features` を通過した場合のみ公開処理へ進みます
- 配布ターゲットは次の4種類です:
  - `x86_64-unknown-linux-gnu`
  - `x86_64-pc-windows-msvc`
  - `x86_64-apple-darwin`
  - `aarch64-apple-darwin`
- 各ターゲットで `dataq-<tag>-<target>.<ext>` と `dataq-<tag>-<target>.sha256` を GitHub Release に添付します
- タグ名に `-` を含む場合（例: `v0.1.0-rc.1`）は GitHub Pre-release として公開します
- この workflow は `crates.io` 公開を行いません（将来は別 workflow で分離予定）

### コントリビュート

Issue / Pull Request を歓迎します。開発ルールは `AGENTS.md` を参照してください。  
外部コントリビュータ向けの `CONTRIBUTING.md` は追加予定です。

### セキュリティ

脆弱性の報告手順は `SECURITY.md` で整備予定です。  
機密性のある内容は公開Issueに直接記載しないでください。

### ライセンス

このプロジェクトは MIT License で提供します。詳細は `LICENSE` を参照してください。

## サブコマンド詳細（MVP）

### 1. `canon`

入力（JSON/YAML/CSV/JSONL）を決定的に正規化し、JSON もしくは JSONL へ変換。

- `--from` 省略時（stdin入力のみ）は固定順で自動判別: `JSONL -> JSON -> YAML -> CSV`
- 非空行が1行のみで入力全体がJSONとして成立する場合は、曖昧さ回避のため `JSON` として扱う
- 自動判別できない入力は `input_usage_error`（終了コード `3`）
- `--to jsonl` かつ JSONL入力ではレコード単位で逐次処理（入力順を保持）

- キー順ソート
- 型寄せ（数値/真偽値/日時）
- `--sort-keys=false` で入力キー順を保持可能

### 2. `assert`

期待ルールまたは JSON Schema に対して検証。

- 必須キー
- 禁止キー
- フィールド制約（`fields.<path>` に `type` / `enum` / `pattern` / `nullable` / `range` を集約）
- 最小/最大件数
- `--rules <path>`: dataq ルールで検証（ルールスキーマは厳密。未知キーは入力不正）
- ルールは `extends` で再利用可能（親相対パス解決、循環/欠損/不正形式は入力不正）
- `extends` マージ: `required_keys`/`forbid_keys` は和集合、`fields` はパス後勝ち、`count` は最後に定義された値を採用
- `--schema <path>`: JSON Schema で検証
- `--normalize <github-actions-jobs|gitlab-ci-jobs>`: 生のCI定義を `yq -> jq -> mlr` の3段でジョブ単位レコードへ正規化してから検証（`yq`/`jq`/`mlr` 必須）
- `--rules` と `--schema` は同時指定不可（入力不正として終了コード `3`）
- `--rules-help`: `--rules` 用ルール仕様を機械可読JSONで出力して終了（終了コード `0`）
- `--schema-help`: `--schema`（JSON Schema検証）用の使い方と結果契約を機械可読JSONで出力して終了（終了コード `0`）

失敗時は機械可読エラーJSONを返し、終了コード `2`。  
`mismatches[]` は `path`, `rule_kind`, `reason`, `actual`, `expected` を含みます。

`assert` ルール例:

```yaml
extends: [./base.rules.yaml]
required_keys: [id, status]
forbid_keys: [debug, meta.blocked]
fields:
  id:
    type: integer
  score:
    type: number
    nullable: true
    range:
      min: 0
      max: 100
  status:
    enum: [active, archived]
  name:
    pattern: '^[a-z]+_[0-9]+$'
count:
  min: 1
  max: 1000
```

ルール仕様をCLIから取得:

```bash
dataq assert --rules-help
```

JSON Schemaモード仕様をCLIから取得:

```bash
dataq assert --schema-help
```

サービス定義向けのサンプルルール:

- 配置先: `examples/assert-rules/`
- 対象: `cloud-run`, `github-actions`, `gitlab-ci`
- 方式:
  - `raw.rules.yaml`: 生のYAML構造を検証
  - `jobs.rules.yaml`: `--normalize` でジョブ単位に正規化して検証（`yq -> jq -> mlr` の3段方式）

例（Cloud Run の raw 検証）:

```bash
dataq assert --input service.yaml --rules examples/assert-rules/cloud-run/raw.rules.yaml
```

例（GitHub Actions の jobs 検証）:

```bash
dataq assert \
  --input .github/workflows/ci.yml \
  --normalize github-actions-jobs \
  --rules examples/assert-rules/github-actions/jobs.rules.yaml
```

### 2.1 `gate schema`

`assert --schema` と同じ JSON Schema 検証レポートを、schema gate 用コマンドとして明示化。

- コマンド: `dataq gate schema --schema <path> [--input <path|->] [--from <preset>]`
- 出力JSON: `assert --schema` と同一（`matched`, `mismatch_count`, `mismatches`）
- 終了コード:
  - `0`: すべて一致
  - `2`: schema mismatch
  - `3`: schema/input/`--from` の入力不正
  - `1`: 予期しない内部エラー
- `--from`（任意）:
  - `github-actions-jobs`
  - `gitlab-ci-jobs`
  - 未対応 preset は明示的エラーで終了コード `3`
- 移行ガイド:
  - 旧: `dataq assert --schema schema.json --input in.json`
  - 新: `dataq gate schema --schema schema.json --input in.json`

### 2.2 `gate policy`

ルールベース検証の結果を policy gate 用の固定出力として返す。

- コマンド: `dataq gate policy --rules <path> [--input <path|->] [--source <preset>]`
- 出力JSON: `matched`, `violations`, `details`
- 終了コード:
  - `0`: すべて一致
  - `2`: policy violation を検出
  - `3`: rules/input/source の入力不正
  - `1`: 予期しない内部エラー
- `--source`（任意）:
  - `scan-text`
  - `ingest-doc`
  - `ingest-api`
  - `ingest-notes`
  - `ingest-book`

### 3. `sdiff`

変換前後または2データセット間の構造差分を返す。

- 件数差分
- カラム/キー差分
- 値差分（パス単位）
- パス表記は曖昧さ回避のため canonical 形式（例: `$["a.b"]`, `$[0]["quote\"key"]`）
- `--key <canonical-path>` でレコード対応付けキーを指定（例: `$["id"]`）
- `--ignore-path <canonical-path>` で比較除外パスを複数指定可能
- `--value-diff-cap <usize>` で `values.items` の最大件数を制御（既定: `100`）
- `--fail-on-diff` 指定時は `values.total > 0` で終了コード `2`（未指定時は比較成功で `0`）
- `--key` 利用時に重複キーがある場合は入力不正として終了コード `3`
- `--ignore-path` 指定時、レポートに `ignored_paths` が出力される
- `values.total` は実差分件数を維持し、上限超過時のみ `values.truncated=true`

### 4. `diff source`

異なる入力ソース（file または preset）を解決してから、`sdiff` と同じ差分レポートを返す。

- `--left <preset-or-path>` / `--right <preset-or-path>`
  - file: `path/to/input.json`
  - preset: `preset:<github-actions-jobs|gitlab-ci-jobs>:<path>`
- 出力は `sdiff` と同じ `counts` / `keys` / `ignored_paths` / `values` に加えて、`sources`（左右の解決メタデータ）を含む
- `--fail-on-diff` 指定時は `values.total > 0` で終了コード `2`
- `--emit-pipeline` の `steps`: `diff_source_resolve_left`, `diff_source_resolve_right`, `diff_source_compare`

### 5. `profile`

データ品質の概要を決定的な JSON で返す。

- `record_count`: レコード件数
- `field_count`: フィールドパス件数
- `fields`: canonical path ごとの集計
  - `null_ratio`（0.0-1.0）
  - `unique_count`
  - `type_distribution`（`null|boolean|number|string|array|object`）
  - `numeric_stats`（数値サンプルが1件以上ある場合のみ）
    - `count`, `min`, `max`, `mean`, `p50`, `p95`

`numeric_stats` の決定性ルール:

- 数値サンプルは JSON number 型のみを対象（null/文字列/真偽値などは対象外）
- `p50` / `p95` は nearest-rank 方式（`rank = ceil(p * n)`、`index = rank - 1`、0始まり配列で評価）
- `numeric_stats` の浮動小数は小数点以下6桁へ丸め（`round half away from zero` 相当）

### 6. `join`

2つの入力を結合キーで結合し、JSON配列で返す。

- `--left <path>`: 左入力（JSON/YAML/CSV/JSONL）
- `--right <path>`: 右入力（JSON/YAML/CSV/JSONL）
- `--on <field>`: 結合キー
- `--how <inner|left>`: 結合方式
- 入力レコードは object であること、および `--on` キーが全レコードに存在することが必須
- 出力は JSON 配列固定（決定的順序）
- 実行は `mlr` を明示的引数配列で呼び出し、`--emit-pipeline` 時に stage 診断（`input_records`, `output_records`, `input_bytes`, `output_bytes`, `duration_ms`(固定 `0`), `status`）を出力

### 7. `aggregate`

単一入力をグループ化して集計し、JSON配列で返す。

- `--input <path>`: 入力（JSON/YAML/CSV/JSONL）
- `--group-by <field>`: グループキー
- `--metric <count|sum|avg>`: 集計メトリクス
- `--target <field>`: 集計対象キー
- `sum` / `avg` は `--target` が数値であることを要求
- 入力レコードは object であること、および `group-by`/`target` キーが全レコードに存在することが必須
- 出力は JSON 配列固定（メトリクス列は `count` / `sum` / `avg`）
- 実行は `mlr` を明示的引数配列で呼び出し、`--emit-pipeline` 時に stage 診断（`input_records`, `output_records`, `input_bytes`, `output_bytes`, `duration_ms`(固定 `0`), `status`）を出力

### 8. `merge`

複数の JSON/YAML 入力をポリシー指定で決定的にマージ。

- `--base <path>` と `--overlay <path>`（複数指定可）を順に適用
- `--policy last-wins`: 同一キーは overlay 側で上書き（shallow）
- `--policy deep-merge`: object は再帰マージ、配列は要素インデックス単位で再帰マージ
- `--policy array-replace`: object は再帰マージ、配列は overlay 側で全置換
- `--policy-path <canonical-path=policy>`（複数指定可）で subtree ごとのポリシーを上書き
  - 例: `--policy-path '$["spec"]["containers"]=array-replace'`
  - 解決順: 最長一致する `--policy-path` を優先し、同一深さの一致は後ろに指定した定義を優先。一致なしは `--policy` を適用
- 出力は JSON 固定（キー順は決定的にソート）

### 9. `doctor`

実行環境の依存を診断。`--capabilities` と `--profile` に対応。

- 出力は JSON 固定（stdout）
- 各ツールの診断項目: `name`, `found`, `version`, `executable`, `message`
- `--capabilities` 指定時:
  - `capabilities`（固定順）を追加: `jq.null_input_eval`, `yq.null_input_eval`, `mlr.help_command`
  - 項目: `name`, `tool`, `available`, `message`
- `--profile <core|ci-jobs|doc|api|notes|book|scan>` 指定時:
  - `capabilities`（固定順の `*.available` probe）を追加
  - `profile`（`version`, `name`, `description`, `satisfied`, `requirements`）を追加
  - `version` は `dataq.doctor.profile.requirements.v1` で固定
- 終了コード:
  - `0`: `--profile` 未指定時は `jq|yq|mlr` が全て起動可能、`--profile` 指定時は選択 profile 要件を充足
  - `3`: `--profile` 未指定時は `jq|yq|mlr` のいずれかが欠如または起動不可、`--profile` 指定時は選択 profile 要件未達
  - `1`: 予期しない内部エラー
- `--emit-pipeline` 指定時の stderr ステップ:
  - `--profile` 未指定: `doctor_probe_tools`, `doctor_probe_capabilities`
  - `--profile` 指定: `doctor_profile_probe`, `doctor_profile_evaluate`

### 10. `recipe run`

レシピファイル（YAML/JSON）を読み込み、`steps` を定義順で実行します。

- 実行コマンド: `dataq recipe run --file <path>`
- レシピスキーマ（MVP）:
  - `version`: `dataq.recipe.v1`
  - `steps[*].kind`: `canon | assert | profile | sdiff`
  - `steps[*].args`: 各 step の引数オブジェクト
- step 間データは in-memory で受け渡し
- stdout は実行サマリ JSON（`matched`, `exit_code`, `steps`）を返す
- `--emit-pipeline` 有効時は recipe 全体と step 実行トレースを stderr JSON へ出力

例:

```yaml
version: dataq.recipe.v1
steps:
  - kind: canon
    args:
      input: ./input.json
      from: json
  - kind: assert
    args:
      rules:
        required_keys: [id]
        fields:
          id:
            type: integer
```

### 11. `recipe lock`

レシピファイル（YAML/JSON）から、再現実行のためのロック情報を生成します。

- 実行コマンド: `dataq recipe lock --file <path> [--out <lock-path>]`
- 出力:
  - `--out` なし: stdout に lock JSON
  - `--out` あり: lock JSON を指定ファイルへ書き出し（stdout は空）
- lock JSON:
  - `version`: `dataq.recipe.lock.v1`
  - `command_graph_hash`
  - `args_hash`
  - `tool_versions`（`jq`/`yq`/`mlr`）
  - `dataq_version`
- 異常時契約:
  - レシピ不正 / ツール解決失敗は exit `3`
- `--emit-pipeline` 有効時は `recipe_lock_parse`, `recipe_lock_probe_tools`, `recipe_lock_fingerprint` を stderr JSON へ出力

### 12. `contract`

サブコマンドの出力契約を機械可読JSONで取得します（read-only）。

- `dataq contract --command <canon|assert|gate-schema|gate|sdiff|diff-source|profile|merge|doctor|recipe>`
  - 単一コマンドの契約を1オブジェクトで返す
- `dataq contract --all`
  - 全コマンド契約を固定順配列で返す
- 順序: `canon`, `assert`, `gate-schema`, `gate`, `sdiff`, `diff-source`, `profile`, `merge`, `doctor`, `recipe`
- 各契約オブジェクトのキー:
  - `command`, `schema`, `output_fields`, `exit_codes`, `notes`

### 13. `emit plan`

サブコマンドの静的実行計画を、実行せずに機械可読JSONで取得します（read-only）。

- 実行コマンド:
  - `dataq emit plan --command <subcommand> [--args <json-array>]`
- 出力キー:
  - `command`: 対象サブコマンド
  - `args`: 解決に使った引数配列
  - `stages`: `order`, `step`, `tool`, `depends_on` を含む段情報
  - `tools`: `jq|yq|mlr` の期待利用有無（`expected`）
- `--args` は JSON 文字列で渡す（例: `'["--normalize","github-actions-jobs"]'`）
- 終了コード:
  - `0`: 計画生成成功
  - `3`: 未対応サブコマンドまたは `--args` 形式不正
  - `1`: 予期しない内部エラー
- `emit plan` と `--emit-pipeline` の違い:
  - `emit plan`: 実行前の静的計画（外部ツール実行なし）
  - `--emit-pipeline`: 実行時に観測した診断（stderr）

### 14. `mcp`

MCP (Model Context Protocol) の単発JSON-RPC 2.0 リクエストを処理します。

- 実行コマンド: `dataq mcp`
- 入出力:
  - stdin: JSON-RPC 2.0 リクエスト1件
  - stdout: JSON-RPC 2.0 レスポンス1件
- 対応メソッド:
  - `initialize`
  - `tools/list`
  - `tools/call`
- `tools/list` のツール順序は固定:
  - `dataq.canon`
  - `dataq.assert`
  - `dataq.gate.schema`
  - `dataq.gate.policy`
  - `dataq.sdiff`
  - `dataq.diff.source`
  - `dataq.profile`
  - `dataq.join`
  - `dataq.aggregate`
  - `dataq.merge`
  - `dataq.doctor`
  - `dataq.contract`
  - `dataq.emit.plan`
  - `dataq.recipe.run`
  - `dataq.recipe.lock`
- `tools/call` レスポンス:
  - `structuredContent.exit_code`
  - `structuredContent.payload`
  - `structuredContent.pipeline`（`emit_pipeline=true` のときのみ）
  - `isError = (exit_code != 0)`
  - `content[0].text` には `structuredContent` と等価なJSON文字列を格納
- JSON-RPCエラーコード:
  - `-32700` parse error
  - `-32600` invalid request
  - `-32601` method not found
  - `-32602` invalid params
  - `-32603` internal error
- `mcp` モードのプロセス終了コード:
  - レスポンスを書き出せた場合は、ツール実行結果に関係なく `0`
  - レスポンス出力不能な致命的I/O時のみ `3`

## 設計ドキュメント

設計に関する詳細は `docs/` 配下を参照してください。

- [設計方針・実装アーキテクチャ](docs/architecture.md)
- [CLI仕様・I/O契約](docs/command-spec.md)
- [assert ルールスキーマ](docs/rules-schema.md)

## 想定ユースケース

- CIでのデータ品質ゲート
- ETL前後の差分検証
- エージェント実行前の入力正規化
- 手元での再現可能なデータ調査
