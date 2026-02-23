# profile 機能 指示書（MVP）

## 目的
- データ品質の概要を機械可読 JSON で出力する。
- `stdin -> stdout` を維持し、決定的な結果を返す。

## 対象
- `src/cmd/profile.rs`
- `src/engine/profile/`（新規）
- `src/main.rs`（サブコマンド接続）
- `tests/cli/profile_cli.rs`（新規）
- `tests/integration/`（必要に応じて追加）
- `README.md`

## CLI 仕様（MVP）
- コマンド: `dataq profile`
- 入力: `--input <path>`（省略時 stdin）
- 形式: `--from <json|yaml|csv|jsonl>`（当面は必須）
- 出力: JSON 固定
- 終了コード:
  - `0`: 成功
  - `3`: 入力不正
  - `1`: 内部エラー

## 出力 JSON（MVP）
- `record_count: number`
- `field_count: number`
- `fields: object`
  - キー: canonical path（例: `$["id"]`）
  - 値:
    - `null_ratio: number`（0.0-1.0）
    - `unique_count: number`
    - `type_distribution: object`（`null|boolean|number|string|array|object`）

## 実装タスク
1. `engine/profile` に集計ロジックを実装。
2. path 表現は `sdiff` と同じ canonical 形式へ統一。
3. 出力オブジェクトのキー順を決定的にする。
4. `cmd/profile.rs` で I/O と終了コードマッピングを実装。
5. `main.rs` に `Profile` サブコマンドを追加。
6. README に使用例を追記。

## テスト
- CLI:
  - JSON 入力で期待 JSON を返す。
  - CSV 入力で型分布が安定している。
  - 不正入力で exit code `3`。
- Integration:
  - 同一入力を2回流して完全一致する（決定性）。

## 完了条件
- quality gates がすべて通る。
- README のサブコマンド一覧に `profile` が載っている。
- 主要パスの集計結果がスナップショット不要で比較可能。

