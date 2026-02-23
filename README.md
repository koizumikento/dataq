# dataq

`dataq` は、JSON / YAML / CSV を対象にした「決定的な前処理・検証・差分」CLIです。  
AI処理そのものは行わず、エージェントやCIから呼びやすい機械可読I/Oを提供します。

## 目的

- データ変換を再現可能にする（同じ入力なら同じ出力）
- 失敗を終了コードとJSONで明確化する
- `jq` / `yq` / `mlr` の強みを統一的に使えるようにする

## 設計方針

- 実装本体は Rust（ネイティブCLI）
- 依存コマンドは `jq`, `yq`, `mlr`（Miller）
- デフォルト出力は JSON（`--human` で人向け表示）
- `stdin -> stdout` を第一に設計
- キー順、時刻フォーマット、丸め規則を固定して決定性を担保

## サブコマンド（MVP）

### 1. `canon`

入力（JSON/YAML/CSV/JSONL）を決定的に正規化し、JSON もしくは JSONL へ変換。

- キー順ソート
- 型寄せ（数値/真偽値/日時）
- `--sort-keys=false` で入力キー順を保持可能

### 2. `assert`

期待ルールまたは JSON Schema に対して検証。

- 必須キー
- 禁止キー
- 型
- enum（許容値列挙）
- pattern（文字列正規表現）
- nullable（null許容フラグ）
- 値域
- 最小/最大件数
- `--rules <path>`: dataq ルールで検証（ルールスキーマは厳密。未知キーは入力不正）
- `--schema <path>`: JSON Schema で検証
- `--rules` と `--schema` は同時指定不可（入力不正として終了コード `3`）

失敗時は機械可読エラーJSONを返し、終了コード `2`。  
`mismatches[]` は `path`, `rule_kind`, `reason`, `actual`, `expected` を含みます。

`assert` ルール例:

```yaml
required_keys: [id, status]
forbid_keys: [debug, meta.blocked]
types:
  id: integer
  score: number
nullable:
  score: true
enum:
  status: [active, archived]
pattern:
  name: '^[a-z]+_[0-9]+$'
count:
  min: 1
  max: 1000
ranges:
  score:
    min: 0
    max: 100
```

### 3. `sdiff`

変換前後または2データセット間の構造差分を返す。

- 件数差分
- カラム/キー差分
- 値差分（パス単位）
- パス表記は曖昧さ回避のため canonical 形式（例: `$["a.b"]`, `$[0]["quote\"key"]`）
- `--key <canonical-path>` でレコード対応付けキーを指定（例: `$["id"]`）
- `--ignore-path <canonical-path>` で比較除外パスを複数指定可能
- `--key` 利用時に重複キーがある場合は入力不正として終了コード `3`
- `--ignore-path` 指定時、レポートに `ignored_paths` が出力される

### 4. `profile`

データ品質の概要を決定的な JSON で返す。

- `record_count`: レコード件数
- `field_count`: フィールドパス件数
- `fields`: canonical path ごとの集計
  - `null_ratio`（0.0-1.0）
  - `unique_count`
  - `type_distribution`（`null|boolean|number|string|array|object`）

### 5. `merge`

複数の JSON/YAML 入力をポリシー指定で決定的にマージ。

- `--base <path>` と `--overlay <path>`（複数指定可）を順に適用
- `--policy last-wins`: 同一キーは overlay 側で上書き（shallow）
- `--policy deep-merge`: object は再帰マージ、配列は要素インデックス単位で再帰マージ
- `--policy array-replace`: object は再帰マージ、配列は overlay 側で全置換
- 出力は JSON 固定（キー順は決定的にソート）

## CLI I/O 契約

### 出力モード

- 既定: JSON（機械可読）
- `canon` のみ `--to jsonl` で JSONL 出力を選択可能

### 終了コード

- `0`: 成功
- `2`: 検証失敗（期待仕様に不一致）
- `3`: 入力不正（フォーマット不正、必須引数不足など）
- `1`: その他実行時エラー

### 例

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

# ID で対応付けし、更新時刻は差分対象外
dataq sdiff --left before.jsonl --right after.jsonl --key '$["id"]' --ignore-path '$["updated_at"]'
```

## Rust 実装メモ

### 推奨クレート

- CLI: `clap`
- JSON: `serde`, `serde_json`
- YAML: `serde_yaml`
- CSV: `csv`
- エラー: `thiserror`, `anyhow`
- 外部コマンド実行: `std::process::Command`

### アーキテクチャ案

拡張しやすさを優先するため、以下の「責務分離」を基本にします。

- `cmd`: CLI境界（引数・入出力・終了コード）
- `engine`: ビジネスロジック（正規化・検証・差分）
- `io`: フォーマット入出力（JSON/YAML/CSV/JSONL）
- `adapters`: 外部ツール呼び出し（`jq`/`yq`/`mlr`）
- `domain`: 型・ルール・エラーなどの共通モデル

```text
dataq/
  Cargo.toml
  rust-toolchain.toml
  README.md
  AGENTS.md
  docs/
    architecture.md
    command-spec.md
    rules-schema.md
  src/
    main.rs
    lib.rs
    cmd/
      mod.rs
      canon.rs
      assert.rs
      sdiff.rs
      profile.rs
      merge.rs
    engine/
      mod.rs
      canon/
        mod.rs
        normalize.rs
        coerce.rs
      assert/
        mod.rs
        validator.rs
      sdiff/
        mod.rs
        compare.rs
    io/
      mod.rs
      reader.rs
      writer.rs
      format/
        mod.rs
        json.rs
        yaml.rs
        csv.rs
        jsonl.rs
    adapters/
      mod.rs
      jq.rs
      yq.rs
      mlr.rs
    domain/
      mod.rs
      schema.rs
      rules.rs
      error.rs
      report.rs
    util/
      mod.rs
      sort.rs
      time.rs
  tests/
    cli/
      canon_cli.rs
      assert_cli.rs
      sdiff_cli.rs
    integration/
      canon_assert_flow.rs
    fixtures/
      input/
      expected/
```

### 機能追加ルール（運用）

- 新しいサブコマンドは `src/cmd/<name>.rs` を追加し、`src/engine/<name>/` にロジックを置く
- 外部依存を増やす前に、まず `engine` だけで完結できるか検討する
- `cmd` 層に業務ロジックを置かない（パース/表示/終了コードのみに限定）
- 出力フォーマット追加は `src/io/format/` に閉じ込める
- エラー型は `src/domain/error.rs` に集約し、文字列ベタ書きで散らさない
- 追加機能ごとに `tests/cli` と `tests/integration` を最低1件ずつ追加する

### 依存の切り分け方

- `dataq` のコア価値（決定性・検証・差分）は Rust ネイティブ実装
- `jq/yq/mlr` はアダプタ層で利用し、置換可能な構造にする
- `adapters` と `engine` を分離して、将来的な内製置換や無効化を容易にする

### 実装ポリシー

- コアロジックは Rust 側に寄せる
- `jq/yq/mlr` はフォーマット変換・大規模処理の補助として使用
- `--emit-pipeline` で内部実行パイプラインを表示可能にする

## ロードマップ

1. MVP (`canon`, `assert`, `sdiff`)
2. `profile`（欠損率、ユニーク数、型分布）
3. `merge`（YAML/JSONのポリシーマージ、実装済み）
4. JSON Schema連携
5. スナップショットテスト拡充

## 想定ユースケース

- CIでのデータ品質ゲート
- ETL前後の差分検証
- エージェント実行前の入力正規化
- 手元での再現可能なデータ調査
