# assert 拡張 指示書（ルール表現力）

## 目的
- 品質ルールを実務向けに拡張し、検証の記述力を高める。

## 対象
- `src/domain/rules.rs`
- `src/engine/assert/validator.rs`
- `src/cmd/assert.rs`
- `tests/cli/assert_cli.rs`
- `tests/integration/canon_assert_flow.rs`
- `README.md`

## 追加ルール（第一弾）
- `enum`: 許容値列挙
- `pattern`: 文字列正規表現
- `forbid_keys`: 禁止キー
- `nullable`: null 許容フラグ

## 仕様
- ルールスキーマは引き続き strict（未知キーは exit code `3`）。
- 不一致は exit code `2`、入力/スキーマ不正は exit code `3`。
- mismatch レポートに rule kind を明記。

## 実装タスク
1. `AssertRules` を後方互換を維持して拡張。
2. validator の検証順序を固定し、レポート順も決定的にする。
3. mismatch payload に機械可読フィールドを追加。
4. README に新ルール例を追加。

## テスト
- ルール単体（enum/pattern/forbid_keys/nullable）。
- 複合ルール時の優先順位とメッセージ安定性。
- 未知キー混入で exit code `3`。

## 完了条件
- 既存ルールが破壊されていない。
- 失敗レポートだけで CI が判定可能。

