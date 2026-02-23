# assert ルール再利用機能 指示書（`extends` / `include`）

## 目的
- `assert` ルールの重複を減らし、共通ポリシーを再利用可能にする。
- ルール保守の変更コストを下げ、CI 設定群への横展開を容易にする。

## 対象
- `src/cmd/assert.rs`
- `src/domain/rules.rs`
- `src/engine/assert/`
- `tests/cli/assert_cli.rs`
- `tests/integration/`
- `docs/rules-schema.md`
- `README.md`

## MVP 仕様
- ルールファイルに再利用指定を追加:
  - `extends: <path | [paths...]>`
- 読み込み順:
  - `extends` 先を先に解決し、最後に現在ファイルを適用（現在ファイル優先）。
- 不正ケース:
  - 循環参照
  - 存在しないファイル
  - 不正フォーマット
  いずれも exit `3`（input/usage error）。

## マージ規則（MVP）
- `required_keys`: 和集合（重複排除・決定的順序）
- `forbid_keys`: 和集合（重複排除・決定的順序）
- `fields`: path キーで上書きマージ（後勝ち）
- `count`: 後勝ち（最終定義を採用）

## 互換性制約
- `extends` を使わない既存ルールは挙動変更なし。
- 既存 mismatch 出力フォーマットを維持。
- 既存終了コード契約 `0/2/3/1` を維持。

## 実装タスク
1. ルール構造体に `extends` を追加（未知キー拒否方針は維持）。
2. 参照解決ローダーを追加（相対パス基準は親ルールファイル）。
3. 循環検出を実装（visited stack で検出）。
4. マージ規則を engine 側へ実装。
5. `--rules-help` / `rules-schema.md` に新キーを追記。

## テスト
- 単一 `extends` で期待どおり継承される。
- 多段 `extends` で順序どおり後勝ちになる。
- 循環参照で exit `3`。
- 参照先欠如で exit `3`。
- 同一入力で出力が決定的（順序揺れなし）。

## 品質ゲート
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`

## 完了条件
- ルール共通化が可能になり、既存契約を壊さず運用利用できる。
