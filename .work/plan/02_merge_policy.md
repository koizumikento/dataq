# merge 機能 指示書（ポリシーマージ）

## 目的
- JSON/YAML の複数入力を、明示ポリシーに従って決定的にマージする。

## 対象
- `src/cmd/merge.rs`
- `src/engine/merge/`（新規）
- `src/main.rs`
- `tests/cli/merge_cli.rs`（新規）
- `tests/integration/`（必要に応じて追加）
- `README.md`

## CLI 仕様（初期）
- コマンド: `dataq merge`
- 入力:
  - `--base <path>`
  - `--overlay <path>`（複数可）
- ポリシー:
  - `--policy <last-wins|deep-merge|array-replace>`
- 出力: JSON 固定
- 終了コード:
  - `0`: 成功
  - `3`: 入力不正
  - `1`: 内部エラー

## マージ仕様
- `last-wins`: 同一キーは後勝ち。
- `deep-merge`: object は再帰マージ、スカラーは後勝ち。
- `array-replace`: 配列は overlay 側で全置換。
- ルールは全て deterministic に適用し、キー順は固定。

## 実装タスク
1. 入力形式解決（拡張子/指定）を `io` 既存機能で統一。
2. `engine/merge` にポリシー別マージ実装。
3. 競合時の挙動を明文化したエラー/結果を整備。
4. `cmd/merge.rs` でサブコマンド入出力を接続。
5. README へポリシー別サンプルを追記。

## テスト
- CLI:
  - ポリシーごとの差分を固定入力で検証。
  - 未対応ポリシー指定で exit code `3`。
- Integration:
  - `base + overlay1 + overlay2` の結果が順序依存であること（仕様通り）。
  - 同入力の再実行で一致。

## 完了条件
- 3ポリシーが最低限実装済み。
- ドキュメントと実装挙動に齟齬がない。

