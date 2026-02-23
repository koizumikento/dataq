# sdiff 拡張 指示書（比較モード）

## 目的
- 差分ノイズを減らし、実運用で意味のある差分のみを返せるようにする。

## 対象
- `src/cmd/sdiff.rs`
- `src/engine/sdiff/`
- `src/main.rs`
- `tests/cli/sdiff_cli.rs`
- `tests/integration/canon_sdiff_flow.rs`
- `README.md`

## 追加 CLI 仕様
- `--key <canonical-path>`: レコード対応付けキー
- `--ignore-path <canonical-path>`: 比較除外（複数可）
- 既存 `value_diff_cap` 設定との共存を維持

## 仕様詳細
- `--key` 指定時:
  - 左右レコードを key 値で対応付け
  - key 重複は入力不正（exit code `3`）
- `--ignore-path`:
  - 指定 path 配下を差分計算対象外にする
  - レポートに `ignored_paths` を明示

## 実装タスク
1. 引数追加とバリデーションを実装。
2. engine 側に key ベース比較経路を追加。
3. ignore-path フィルタを差分抽出前に適用。
4. 出力スキーマ変更を最小差分で反映。
5. README に比較モードの使い分けを追記。

## テスト
- key あり/なしで結果が変わるケース。
- key 重複で exit code `3`。
- ignore-path 指定で差分件数が減ること。
- canonical path エスケープキーで正しく動作すること。

## 完了条件
- 既存テストを壊さず、追加ケースを網羅。
- path 仕様が README と一致。

