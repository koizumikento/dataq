# join / aggregate 機能追加 指示書

## 目的
- `mlr` 連携価値を前面化し、運用で多い結合・集計パターンを `dataq` で固定化する。
- ad-hoc なシェルパイプを、契約化された CLI に置き換える。

## 対象
- `src/main.rs`
- `src/cmd/mod.rs`
- `src/cmd/join.rs`（新規）
- `src/engine/`（必要に応じて共通整形ロジック）
- `src/adapters/mlr.rs`
- `tests/cli/join_cli.rs`（新規）
- `README.md`
- `docs/command-spec.md`

## MVP CLI 仕様（案）
- `dataq join --left <path> --right <path> --on <field> --how <inner|left>`
- `dataq aggregate --input <path> --group-by <field> --metric <count|sum|avg> --target <field>`
- 出力は JSON（配列）固定。

## 実行方針
- 外部実行は必ず引数配列で行い、シェル展開は使わない。
- `--emit-pipeline` で stage 順・件数変化・外部ツール使用を出力する。
- 欠損キーなど入力不正は exit `3`。

## 互換性制約
- 既存サブコマンドへの影響を与えない。
- 終了コード契約 `0/2/3/1` を維持。

## 実装タスク
1. `join` / `aggregate` の clap 定義を追加。
2. `mlr` adapter で実行ユニットを実装。
3. Rust 側で I/O 契約を統一し JSON に正規化。
4. pipeline 診断を追加。
5. README / command-spec に仕様・制限・例を追記。

## テスト
- `join` の inner/left が期待結果を返す。
- `aggregate` の count/sum/avg が決定的に一致する。
- 入力不正時に exit `3`。
- `--emit-pipeline` が期待 schema を満たす。

## 品質ゲート
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`

## 完了条件
- 運用で頻出の join/aggregate を `dataq` 契約で再利用できる。
