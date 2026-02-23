# dataq 実装指示書: `sdiff`

## 目的

- 2つのデータセット間の構造差分を返す。
- 差分を件数/キー/値の観点で機械可読JSONにまとめる。

## CLI仕様（MVP）

- `dataq sdiff --left <path> --right <path>`
- 入力形式は JSON/YAML/CSV/JSONL を許可

## 実装方針

1. `src/cmd/sdiff.rs`
   - 引数パース
   - 左右入力の読込呼び出し
2. `src/engine/sdiff/compare.rs`
   - レコード件数差分
   - キー/カラム差分
   - 主要パスの値差分（上限件数あり）
3. `src/domain/report.rs`
   - 差分レポート型（counts/keys/values）を定義
4. `src/io/`
   - 差分JSONを安定順で出力

## 受け入れ条件

- 同一データでは差分なしを返す。
- 異なる件数を正しく報告する。
- 左右でキーが異なる場合に差分を報告する。
- `tests/cli` に `sdiff` の最低1テストを追加。
- `tests/integration` に `canon -> sdiff` の最低1フローテストを追加。

## 完了チェック

- `cargo check`
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`
