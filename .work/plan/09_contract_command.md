# contract コマンド追加 指示書

## 目的
- 各サブコマンドの出力契約を機械可読で公開し、ツール連携側の実装を安定化する。
- README だけでなく CLI から直接契約情報を取得可能にする。

## 対象
- `src/main.rs`
- `src/cmd/mod.rs`
- `src/cmd/contract.rs`（新規）
- `tests/cli/entry_cli.rs`
- `tests/cli/contract_cli.rs`（新規）
- `README.md`
- `docs/command-spec.md`

## MVP CLI 仕様
- `dataq contract --command <canon|assert|sdiff|profile|merge|doctor|recipe>`
- `dataq contract --all`
- 出力: JSON（schema id、主要フィールド、終了コード契約）

## 出力項目（最低限）
- `command`
- `schema`
- `output_fields`（トップレベル）
- `exit_codes`（`0/2/3/1` の意味）
- `notes`（互換性注意）

## 互換性制約
- `contract` は参照専用で副作用を持たない。
- 既存コマンド挙動を変更しない。

## 実装タスク
1. `contract` サブコマンド追加。
2. コマンド別契約メタデータを静的定義。
3. `--all` で全コマンド契約を配列で出力。
4. README / command-spec に使用例を追記。

## テスト
- `--command assert` が期待キーを返す。
- `--all` が決定的順序で全件を返す。
- 未知コマンド指定で exit `3`。

## 品質ゲート
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`

## 完了条件
- 契約情報を CLI から一貫して取得できる。
