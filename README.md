# dataq

`dataq` は、JSON / YAML / CSV を対象にした「決定的な前処理・検証・差分」CLIです。  
AI処理そのものは行わず、エージェントやCIから呼びやすい機械可読I/Oを提供します。

## 目的

- データ変換を再現可能にする（同じ入力なら同じ出力）
- 失敗を終了コードとJSONで明確化する
- `jq` / `yq` / `mlr` の強みを統一的に使えるようにする

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

# ID で対応付けし、更新時刻は差分対象外
dataq sdiff --left before.jsonl --right after.jsonl --key '$["id"]' --ignore-path '$["updated_at"]'
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
- `--schema <path>`: JSON Schema で検証
- `--rules` と `--schema` は同時指定不可（入力不正として終了コード `3`）
- `--rules-help`: `--rules` 用ルール仕様を機械可読JSONで出力して終了（終了コード `0`）
- `--schema-help`: `--schema`（JSON Schema検証）用の使い方と結果契約を機械可読JSONで出力して終了（終了コード `0`）

失敗時は機械可読エラーJSONを返し、終了コード `2`。  
`mismatches[]` は `path`, `rule_kind`, `reason`, `actual`, `expected` を含みます。

`assert` ルール例:

```yaml
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
- `fields`: canonical path ごとの集計（`null_ratio`（0.0-1.0）, `unique_count`, `type_distribution`（`null|boolean|number|string|array|object`））

### 5. `merge`

複数の JSON/YAML 入力をポリシー指定で決定的にマージ。

- `--base <path>` と `--overlay <path>`（複数指定可）を順に適用
- `--policy last-wins`: 同一キーは overlay 側で上書き（shallow）
- `--policy deep-merge`: object は再帰マージ、配列は要素インデックス単位で再帰マージ
- `--policy array-replace`: object は再帰マージ、配列は overlay 側で全置換
- 出力は JSON 固定（キー順は決定的にソート）

## 設計ドキュメント

設計に関する詳細は `docs/` 配下を参照してください。

- [設計方針・実装アーキテクチャ](docs/architecture.md)
- [CLI仕様・I/O契約](docs/command-spec.md)
- [assert ルールスキーマ](docs/rules-schema.md)

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
