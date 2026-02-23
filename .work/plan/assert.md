# dataq 実装指示書: `assert`

## 目的

- データをルールで検証し、CIで判定可能な終了コードを返す。
- 不一致内容を機械可読JSONで返す。

## CLI仕様（MVP）

- `dataq assert --rules <path> [--input <path>]`
- `--input` 未指定時は stdin

## 実装方針

1. `src/cmd/assert.rs`
   - 引数パース
   - 検証結果に応じて終了コードを分岐（`0` or `2`）
2. `src/domain/rules.rs`
   - ルール構造（必須キー、型、件数、値域）を定義
3. `src/engine/assert/validator.rs`
   - ルール評価
   - 不一致一覧（path/reason/actual/expected）を生成
4. `src/io/`
   - 入力読み込み（JSON/YAML/CSV/JSONL）
   - 結果JSON出力

## 受け入れ条件

- ルール一致時は終了コード `0`。
- ルール不一致時は終了コード `2`。
- 入力/引数不正は終了コード `3`。
- エラーレポートがJSONで安定出力される。
- `tests/cli` に `assert` の最低1テストを追加。
- `tests/integration` に `canon -> assert` の最低1フローテストを追加。

## 完了チェック

- `cargo check`
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`
