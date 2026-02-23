# merge パス別ポリシー 指示書

## 目的
- 単一ポリシーでは扱いづらい実運用マージ要件に対応する。
- 配列や特定 subtree に対するマージ戦略をパス単位で指定可能にする。

## 対象
- `src/main.rs`
- `src/cmd/merge.rs`
- `src/engine/merge/mod.rs`
- `tests/cli/merge_cli.rs`
- `tests/integration/merge_flow.rs`
- `README.md`
- `docs/command-spec.md`

## MVP 仕様（案）
- 既存 `--policy` は全体デフォルトとして維持。
- 追加:
  - `--policy-path <path=policy>`（複数指定可）
  - 例: `--policy-path '$["spec"]["containers"]=array-replace'`
- 優先順位:
  - 最長一致 path の個別ポリシー
  - 一致なしは `--policy` を適用

## 互換性制約
- `--policy-path` 未指定時の挙動は完全に従来どおり。
- 出力 JSON の決定的キー順を維持。

## 実装タスク
1. CLI で `--policy-path` を受理し、厳格にパース。
2. merge engine に path 単位のポリシー解決を追加。
3. path パース失敗や未知 policy は exit `3`。
4. README / command-spec に優先順位と例を追記。

## テスト
- デフォルト policy の既存挙動が回帰しない。
- path 指定で subtree だけ期待ポリシーが適用される。
- 複数 path 指定時に最長一致が優先される。
- 不正定義で exit `3`。

## 品質ゲート
- `cargo fmt --all -- --check`
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`
- `cargo test --workspace --all-features`

## 完了条件
- 実運用の複合マージ要件を path 単位で表現できる。
