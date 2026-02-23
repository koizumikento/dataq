# dataq 実装指示書: `canon`

## 目的

- 入力（JSON/YAML/CSV/JSONL）を決定的に正規化する。
- JSON/JSONLへ変換し、後段（`assert`/`sdiff`）で扱いやすくする。

## CLI仕様（MVP）

- `dataq canon --from <json|yaml|csv|jsonl> --to <json|jsonl>`
- `--sort-keys`（既定: true）
- `--input <path>`（未指定時: stdin）

## 実装方針

1. `src/cmd/canon.rs`
   - 引数パース
   - エラーを終了コードへ変換（`3`/`1`）
2. `src/io/`
   - `--from` に応じて読取
   - `--to` に応じて書込
3. `src/engine/canon/`
   - キー順固定
   - 可能範囲で型寄せ（`"true"`→`true`, `"1"`→`1` など）
4. `src/domain/report.rs`
   - 処理件数、変換件数を返すための最小レポート型を追加

## 受け入れ条件

- 同じ入力に対して同じ出力になる。
- `stdin -> stdout` で動作する。
- 異常入力は終了コード `3`、その他実行時エラーは `1`。
- `tests/cli` に `canon` の最低1テストを追加。
- `tests/integration` に `canon` を使った最低1フローテストを追加。

## 完了チェック

- `cargo check`
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`
