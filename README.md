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
  `--emit-pipeline` で、内部処理ステップ・外部ツール使用有無・`stage_diagnostics`（段ごとの順序/件数/状態）を機械可読で残せる

## 外部ツール多段連携の方針

- `dataq` の一部コマンドは、内部的に複数ツールを段階実行することで価値を成立させます
- これは「3ツールの代替」ではなく「3ツールの合わせ技を再利用可能な契約として固定する」ための設計です
- 多段連携コマンドでは、`--emit-pipeline` で各段の利用ツール・ステップ順・件数変化・失敗段を追跡可能にします

## コマンド一覧

共通形式:

```bash
dataq [--emit-pipeline] <command> [options]
```

サブコマンド一覧（`./target/debug/dataq --help` ベース）:

| Command | 用途 | 必須オプション |
| --- | --- | --- |
| `canon` | 入力を決定的に正規化し、JSON/JSONLへ変換 | `--from <json|yaml|csv|jsonl>` |
| `assert` | ルール or JSON Schema で検証 | `--rules <path>` または `--schema <path>` |
| `sdiff` | 2データセットの構造差分を出力 | `--left <path>` `--right <path>` |
| `profile` | フィールド統計を決定的JSONで出力 | `--from <json|yaml|csv|jsonl>` |
| `merge` | base + overlays をポリシーマージ | `--base <path>` `--overlay <path>...` `--policy <last-wins|deep-merge|array-replace>` |
| `doctor` | 実行前診断（`jq`/`yq`/`mlr`） | なし |

グローバルオプション:

- `--emit-pipeline`: stderr に pipeline JSON を1行追加出力
- `-h, --help`: ヘルプ
- `-V, --version`: バージョン

## 基本的な使い方

```bash
# YAMLを正規化してJSONLへ
cat in.yaml | dataq canon --from yaml --to jsonl > out.jsonl

# ルール検証
dataq assert --input out.jsonl --rules rules.yaml

# JSON Schema 検証
dataq assert --input out.jsonl --schema schema.json

# 差分確認
dataq sdiff --left before.jsonl --right after.jsonl

# 品質プロファイル
dataq profile --from json --input out.jsonl

# ポリシーマージ
dataq merge --base base.yaml --overlay patch1.json --overlay patch2.yaml --policy deep-merge

# 依存ツール診断
dataq doctor

# ID で対応付けし、更新時刻は差分対象外
dataq sdiff --left before.jsonl --right after.jsonl --key '$["id"]' --ignore-path '$["updated_at"]'

# CIゲート: 差分があれば終了コード2、値差分詳細は先頭1件まで
dataq sdiff --left before.jsonl --right after.jsonl --fail-on-diff --value-diff-cap 1

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

### 4. `profile`

データ品質の概要を決定的な JSON で返す。

- `record_count`: レコード件数
- `field_count`: フィールドパス件数
- `fields`: canonical path ごとの集計（`null_ratio`（0.0-1.0）, `unique_count`, `type_distribution`（`null|boolean|number|string|array|object`））

### 5. `merge`

複数の JSON/YAML 入力をポリシー指定で決定的にマージ。

- `--base <path>` と `--overlay <path>`（複数指定可）を順に適用
- `--policy last-wins`: 同一キーは overlay 側で上書き（shallow）
- `--policy deep-merge`: object は再帰マージ、配列は要素インデックス単位で再帰マージ
- `--policy array-replace`: object は再帰マージ、配列は overlay 側で全置換
- 出力は JSON 固定（キー順は決定的にソート）

### 6. `doctor`

実行環境で `jq` / `yq` / `mlr` が利用可能かを、固定順 (`jq`, `yq`, `mlr`) で診断。

- 出力は JSON 固定（stdout）
- 各ツールの診断項目: `name`, `found`, `version`, `executable`, `message`
- 終了コード:
  - `0`: 3ツールすべて起動可能
  - `3`: 1つ以上が欠如または起動不可
  - `1`: 予期しない内部エラー
- `--emit-pipeline` 指定時は stderr に診断ステップ (`doctor_probe_jq`, `doctor_probe_yq`, `doctor_probe_mlr`) を追加出力

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
