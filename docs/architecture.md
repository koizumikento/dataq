# dataq Architecture

## 目的

`dataq` は、JSON / YAML / CSV を対象にした「決定的な前処理・検証・差分」CLIです。  
AI処理そのものは行わず、エージェントやCIから呼びやすい機械可読I/Oを提供します。

## 基本方針

- 実装本体は Rust（ネイティブCLI）
- 依存コマンドは `jq`, `yq`, `mlr`（Miller）
- デフォルト出力は JSON（機械可読）
- `stdin -> stdout` を第一に設計
- キー順、時刻フォーマット、丸め規則を固定して決定性を担保

## 責務分離

- `cmd`: CLI境界（引数・入出力・終了コード）
- `engine`: ビジネスロジック（正規化・検証・差分）
- `io`: フォーマット入出力（JSON/YAML/CSV/JSONL）
- `adapters`: 外部ツール呼び出し（`jq`/`yq`/`mlr`）
- `domain`: 型・ルール・エラーなどの共通モデル

## 推奨クレート

- CLI: `clap`
- JSON: `serde`, `serde_json`
- YAML: `serde_yaml`
- CSV: `csv`
- エラー: `thiserror`, `anyhow`
- 外部コマンド実行: `std::process::Command`

## 機能追加ルール（運用）

- 新しいサブコマンドは `src/cmd/<name>.rs` を追加し、`src/engine/<name>/` にロジックを置く
- 外部依存を増やす前に、まず `engine` だけで完結できるか検討する
- `cmd` 層に業務ロジックを置かない（パース/表示/終了コードのみに限定）
- 出力フォーマット追加は `src/io/format/` に閉じ込める
- エラー型は `src/domain/error.rs` に集約し、文字列ベタ書きで散らさない
- 追加機能ごとに `tests/cli` と `tests/integration` を最低1件ずつ追加する

## 依存の切り分け

- `dataq` のコア価値（決定性・検証・差分）は Rust ネイティブ実装
- `jq/yq/mlr` はアダプタ層で利用し、置換可能な構造にする
- `adapters` と `engine` を分離して、将来的な内製置換や無効化を容易にする

## 実装ポリシー

- コアロジックは Rust 側に寄せる
- `jq/yq/mlr` はフォーマット変換・大規模処理の補助として使用
- `--emit-pipeline` で内部実行パイプラインを表示可能にする

## 関連ドキュメント

- CLI仕様: [command-spec.md](./command-spec.md)
- ルール仕様: [rules-schema.md](./rules-schema.md)
