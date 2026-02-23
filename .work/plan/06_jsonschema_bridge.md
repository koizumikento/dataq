# JSON Schema 連携 指示書

## 目的
- `assert` ルールと JSON Schema の橋渡しを行い、外部標準との互換性を持たせる。

## 対象
- `src/cmd/assert.rs`
- `src/domain/rules.rs`
- `src/engine/assert/`
- `src/io/`（schema 読み込み）
- `tests/cli/assert_cli.rs`
- `README.md`

## 方針（段階導入）
- Phase 1: `--schema <path>` で JSON Schema 検証を追加
- Phase 2: `dataq rules export-schema`（任意）
- Phase 3: `dataq rules import-schema`（任意）

## Phase 1 CLI 仕様
- `dataq assert --input ... --schema schema.json`
- `--rules` と `--schema` は同時指定不可（入力不正: exit code `3`）
- schema 不一致は exit code `2`

## 実装タスク
1. Schema ファイル読み込み・形式判定。
2. 検証エンジン追加（ライブラリ選定を先に実施）。
3. mismatch を既存 report 形式へ正規化。
4. 既存 `assert` レポートとの整合を保証。
5. README に移行ガイドを追記。

## テスト
- required/type/pattern/enum の代表ケース。
- `--rules` 併用エラー。
- schema 構文不正で exit code `3`。

## 完了条件
- schema 利用時も既存 exit-code 契約を遵守。
- report の機械可読性が維持される。

