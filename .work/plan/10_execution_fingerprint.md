# 実行フィンガープリント 指示書

## 目的
- 実行再現性の監査情報を標準化し、CI 障害の再現と差分追跡を容易にする。
- `--emit-pipeline` 診断に、再現に必要な最小メタ情報を追加する。

## 対象
- `src/main.rs`
- `src/domain/report.rs`
- `src/cmd/*`（pipeline report 生成箇所）
- `tests/cli/entry_cli.rs`
- `docs/command-spec.md`
- `README.md`

## MVP 仕様
- `pipeline` JSON に `fingerprint` セクションを追加:
  - `command`
  - `args_hash`（正規化した引数配列ハッシュ）
  - `input_hash`（取得可能な場合）
  - `tool_versions`（`jq`, `yq`, `mlr` の使用分のみ）
  - `dataq_version`
- 既定では `--emit-pipeline` 有効時のみ出力。

## 決定性制約
- ハッシュ対象とシリアライズ順序を固定する。
- 環境差分で揺れる項目（時刻・ホスト名等）は含めない。

## 実装タスク
1. fingerprint モデルを `domain/report` に追加。
2. 各コマンドの pipeline report builder で fingerprint を生成。
3. 外部ツール版数は実使用ツールのみ収集し、失敗時は明示メッセージにする。
4. docs に fingerprint 項目と用途を追記。

## テスト
- `--emit-pipeline` で `fingerprint` が出力される。
- 同一引数・同一入力で `args_hash` / `input_hash` が一致する。
- 既存 `pipeline` キー構造への互換性を確認する。

## 品質ゲート
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`

## 完了条件
- 再現性監査に必要な最小情報が機械可読で取得できる。
