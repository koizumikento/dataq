# profile 数値統計拡張 指示書（`min/max/mean/p50/p95`）

## 目的
- `profile` の実用性を高め、フィールド品質の傾向を一目で判定できるようにする。
- 既存の型分布と null 比率を維持しつつ、数値列の統計を追加する。

## 対象
- `src/domain/report.rs`
- `src/engine/profile/mod.rs`
- `src/cmd/profile.rs`
- `tests/cli/profile_cli.rs`
- `tests/integration/profile_determinism.rs`
- `README.md`
- `docs/command-spec.md`

## 追加仕様（MVP）
- 各フィールドに `numeric_stats` を追加（数値サンプルがある場合のみ）。
- `numeric_stats`:
  - `count`
  - `min`
  - `max`
  - `mean`
  - `p50`
  - `p95`
- 数値が存在しないフィールドは `numeric_stats` を省略。

## 決定性ルール
- パーセンタイル計算は明示ルールで固定（補間方式を仕様化）。
- 浮動小数は丸め規則を固定（小数桁数または文字列表現規約を固定）。
- 入力同一時の出力完全一致を維持する。

## 互換性制約
- 既存キー（`record_count`, `field_count`, `fields`, `type_distribution`）を維持。
- 追加は後方互換な拡張に限定（既存利用者の JSON パースを壊さない）。

## 実装タスク
1. `ProfileFieldReport` に `numeric_stats` を追加。
2. `profile_values` で数値サンプル抽出と統計計算を実装。
3. 仕様どおりのパーセンタイル計算をドキュメント化。
4. CLI 出力例と README の説明を更新。

## テスト
- 数値列で `min/max/mean/p50/p95` が期待値どおり。
- 数値なし列で `numeric_stats` が省略される。
- `null` 混在時の count と比率が一貫する。
- 同一入力で出力バイト列が一致（決定性）。

## 品質ゲート
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`

## 完了条件
- `profile` で数値統計を契約化し、既存契約との後方互換を維持できている。
