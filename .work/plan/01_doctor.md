# doctor 機能追加 指示書

## 目的
- `jq` / `yq` / `mlr` 前提の実行環境を事前診断し、失敗原因を実行前に明確化する。
- CI での再現性を高めるため、診断結果を機械可読 JSON で出力する。

## 対象
- `src/main.rs`
- `src/cmd/mod.rs`
- `src/cmd/doctor.rs`（新規）
- `src/domain/report.rs`（必要に応じて診断レポート型を追加）
- `src/adapters/`（ツール検出ロジック共通化する場合）
- `tests/cli/doctor_cli.rs`（新規）
- `README.md`
- `docs/command-spec.md`

## MVP CLI 仕様
- `dataq doctor`
- 既定出力: JSON（stdout）
- 判定対象ツール: `jq`, `yq`, `mlr`
- 各ツールについて以下を返す:
  - `name`
  - `found`（PATH 上に存在するか）
  - `version`（取得できる場合）
  - `executable`（起動できるか）
  - `message`（失敗時の理由）

## 終了コード契約（MVP）
- `0`: 全ツールが利用可能
- `3`: 入力/利用環境エラー（必須ツール不足・起動不可を含む）
- `1`: 予期しない内部エラー

## 実装タスク
1. `doctor` サブコマンドを追加。
2. `jq --version` / `yq --version` / `mlr --version` の起動確認を実装（引数配列のみを使用）。
3. 診断結果を決定的な JSON で出力（ツール順は `jq`, `yq`, `mlr` で固定）。
4. エラー時のメッセージを actionable に整備（インストール案内の最小文言を含む）。
5. `--emit-pipeline` 利用時に `doctor` の診断ステップを出力。
6. README / command-spec に使用例と終了コードを追記。

## テスト
- 3ツールすべて利用可能な場合に exit code `0`。
- 少なくとも1ツール欠如時に exit code `3`。
- JSON スキーマ（キー存在、ツール順、型）が安定していること。
- `--emit-pipeline` の stderr 出力が既存契約を壊さないこと。

## 完了条件
- `doctor` が CI で事前診断に使える機械可読出力を返す。
- 既存の終了コード契約と出力契約を維持する。
