# stdin自動判別 / ストリーミング処理 指示書

## 目的
- 大規模入力でのメモリ負荷を抑えつつ、`stdin -> stdout` の実運用性を強化する。
- 入力フォーマット指定の負担を減らし、使い勝手を改善する。

## 対象
- `src/io/mod.rs`
- `src/io/reader.rs`
- `src/io/format/`
- `src/main.rs`
- `tests/integration/io_format_resolution.rs`
- `tests/integration/io_roundtrip.rs`
- `README.md`
- `docs/command-spec.md`

## MVP 仕様（段階導入）
1. `stdin` 自動判別（`--from` 未指定時）
  - 優先判定: JSONL -> JSON -> YAML -> CSV
  - 判別失敗は exit `3`
2. ストリーミング（対象コマンドを限定）
  - まず `canon --to jsonl` から導入
  - 1レコードずつ処理し、全件バッファを避ける

## 決定性制約
- 自動判別ロジックは固定順で実行し、曖昧ケースでも再現可能な結果を返す。
- ストリーミング時もレコード順と変換規則を固定する。

## 実装タスク
1. フォーマット推定ユーティリティを `io` 層に追加。
2. `main` の `--from` 未指定時分岐を更新。
3. `canon` 用ストリーミング reader/writer 経路を追加。
4. エラーを input_usage に統一し actionable message を整備。
5. ドキュメントに推定順と制限を明記。

## テスト
- `stdin` 無指定入力で期待フォーマットに解決される。
- 曖昧入力で規定どおりの判定結果になる。
- ストリーミング経路で通常経路と同一出力になる。
- 大きめ入力で処理が完走し、回帰がない。

## 品質ゲート
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`

## 完了条件
- `stdin` 運用の利便性とスケール性能が改善され、契約互換を維持できている。
