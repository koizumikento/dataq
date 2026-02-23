# emit-pipeline 拡張 指示書（可観測性）

## 目的
- 実行される処理パイプラインを明示し、監査性と再現性を高める。

## 対象
- `src/main.rs`
- `src/cmd/canon.rs`
- `src/cmd/assert.rs`
- `src/cmd/sdiff.rs`
- `src/cmd/profile.rs`（実装後）
- `src/cmd/merge.rs`（実装後）
- `src/adapters/`（必要に応じて）
- `tests/cli/entry_cli.rs`
- `README.md`

## CLI 仕様
- グローバルまたはサブコマンド引数: `--emit-pipeline`
- 出力:
  - 本処理結果は従来どおり
  - pipeline 情報は stderr に JSON で出力

## pipeline JSON（案）
- `command`: サブコマンド名
- `input`: ソース情報（stdin/path, format）
- `steps`: 実行ステップ配列
- `external_tools`: `jq|yq|mlr` 使用情報
- `deterministic_guards`: 適用した決定性ガード

## 実装タスク
1. 共通 pipeline 構造体を `domain/report` 系に追加。
2. 各 cmd で step を収集し、`--emit-pipeline` 時のみ stderr 出力。
3. ユーザー入力をシェル展開しない方針を明文化。
4. README に CI 連携例を追記。

## テスト
- `--emit-pipeline` なしで既存出力非変更。
- `--emit-pipeline` ありで stderr JSON が出る。
- 代表コマンドで `steps` が空でないこと。

## 完了条件
- 全サブコマンドで同一スキーマの pipeline を出せる。
- 既存終了コード契約を維持。

