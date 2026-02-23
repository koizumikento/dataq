# sdiff CIゲート拡張 指示書（`--fail-on-diff` / `--value-diff-cap`）

## 目的
- `sdiff` を「確認」だけでなく CI の品質ゲートとして直接使えるようにする。
- 既存の差分 JSON 契約を維持したまま、終了コード制御と出力量制御を追加する。

## 対象
- `src/main.rs`
- `src/cmd/sdiff.rs`
- `src/engine/sdiff/mod.rs`
- `src/engine/sdiff/compare.rs`（必要な場合）
- `tests/cli/sdiff_cli.rs`
- `README.md`
- `docs/command-spec.md`

## 追加 CLI 仕様
- `dataq sdiff --left <path> --right <path> [--key <path>] [--ignore-path <path> ...]`
- 追加オプション:
  - `--fail-on-diff`（bool, default: `false`）
  - `--value-diff-cap <usize>`（default: `DEFAULT_VALUE_DIFF_CAP`）

## 挙動
- 差分レポート JSON は従来どおり stdout に出力する。
- 比較処理自体が成功した場合:
  - `--fail-on-diff=false`: exit `0`
  - `--fail-on-diff=true` かつ `values.total > 0`: exit `2`
  - `--fail-on-diff=true` かつ差分なし: exit `0`
- 既存どおり:
  - 入力不正/キー重複/パス不正は exit `3`
  - 予期しない内部失敗は exit `1`

## 互換性制約
- レポート JSON の既存キー（`counts`, `keys`, `ignored_paths`, `values`）を維持する。
- パス表記・ソート順・決定的出力を維持する。
- `--emit-pipeline` の stderr 出力契約を壊さない。

## 実装タスク
1. `SdiffArgs` に `fail_on_diff: bool` と `value_diff_cap: usize` を追加。
2. `run_sdiff` で `value_diff_cap` を `sdiff::parse_options` に渡す。
3. レポート生成後、`fail_on_diff` と差分件数で exit code を分岐。
4. エラー時の stderr JSON 契約（`input_usage_error` / `internal_error`）を維持。
5. README / command-spec に新オプションと終了コードの説明を追記。

## テスト
- `--value-diff-cap 1` で `values.items` が1件に制限され、`values.truncated=true` になる。
- `--fail-on-diff` 未指定時は差分があっても exit `0`（従来互換）。
- `--fail-on-diff` 指定時は差分ありで exit `2`。
- `--fail-on-diff` 指定時でも差分なしは exit `0`。
- `--key` / `--ignore-path` 併用ケースで回帰がない。
- `--emit-pipeline` 有効時の stderr JSON が出続ける。

## 品質ゲート
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`

## 完了条件
- 追加2オプションが仕様どおり機能し、終了コード契約 `0/2/3/1` を満たす。
- 既存 `sdiff` の JSON 出力契約を壊していない。
