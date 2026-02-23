# recipe run 機能追加 指示書

## 目的
- 繰り返し実行する処理を宣言的レシピで固定し、運用パイプラインを短いコマンドで再実行できるようにする。
- `dataq` の契約（機械可読 JSON / 終了コード / 決定性）を recipe 実行でも維持する。

## 対象
- `src/main.rs`
- `src/cmd/mod.rs`
- `src/cmd/recipe.rs`（新規）
- `src/domain/report.rs`（recipe 実行レポート型を追加する場合）
- `tests/cli/recipe_cli.rs`（新規）
- `README.md`
- `docs/command-spec.md`
- 必要に応じて `docs/architecture.md`

## MVP CLI 仕様
- `dataq recipe run --file <path>`
- 入力レシピは YAML/JSON をサポート（拡張子解決）。
- 既定出力は JSON（stdout）。

## MVP レシピスキーマ（案）
- `version`: `dataq.recipe.v1`
- `steps`: 実行順配列
- `steps[*].kind`: `canon | assert | profile | sdiff`
- `steps[*].args`: 各コマンド相当の引数オブジェクト

## 実行ルール
- step は定義順に実行し、順序は固定（決定的）。
- step 間データ受け渡しは in-memory で行う。
- 未知 step、必須引数不足、型不正は exit `3`（input/usage error）。
- step 実行で検証不一致が起きた場合は exit `2`。
- 予期しない内部失敗は exit `1`。
- すべて成功時は exit `0`。

## 出力契約（stdout）
- 実行サマリ JSON を返す。
- 最低限含める項目:
  - `matched`（全step成功/不一致なし）
  - `exit_code`
  - `steps`（stepごとの結果サマリ）

## `--emit-pipeline`
- recipe 全体と各 step の実行ステップを stderr JSON に出力する。
- stdout の本体 JSON を汚染しない。

## 実装タスク
1. `Commands` に `Recipe` を追加し、`recipe run` を clap で定義。
2. レシピファイル読み込み・フォーマット解決・スキーマ検証を実装。
3. step dispatcher（`canon/assert/profile/sdiff`）を実装。
4. step 結果を recipe 実行サマリ JSON に集約。
5. 失敗時の exit code を `0/2/3/1` 契約にマップ。
6. README / command-spec に例と制約を追記。

## テスト
- 正常レシピで exit `0`、決定的に同一 JSON を返す。
- スキーマ不正レシピで exit `3`。
- `assert` 不一致を含むレシピで exit `2`。
- `sdiff` 差分あり（ゲート条件あり）レシピで exit `2`。
- `--emit-pipeline` 有効時に stderr JSON が出力される。

## 品質ゲート
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`

## 完了条件
- `recipe run` がMVP仕様どおり動作し、既存 CLI 契約と整合している。
- テストとドキュメント更新まで完了している。
