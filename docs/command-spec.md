# dataq Command Spec

## コマンド一覧

共通形式:

```bash
dataq [--emit-pipeline] <command> [options]
```

サブコマンド:

- `canon`: 入力を決定的に正規化し、JSON/JSONLへ変換
- `assert`: ルールまたはJSON Schemaで検証
- `sdiff`: 2データセットの構造差分を出力
- `profile`: フィールド統計を決定的JSONで出力
- `merge`: base + overlays をポリシーマージ

## `assert` 補助出力

- `dataq assert --rules-help` で `--rules` 用ルール仕様を機械可読JSONで出力
- `dataq assert --schema-help` で `--schema`（JSON Schema検証）の使い方と結果契約を機械可読JSONで出力
- このモードは検証処理を実行せず、終了コード `0` で終了

## CLI I/O 契約

### 出力モード

- 既定: JSON（機械可読）
- `canon` のみ `--to jsonl` で JSONL 出力を選択可能

### 終了コード

- `0`: 成功
- `2`: 検証失敗（期待仕様に不一致）
- `3`: 入力不正（フォーマット不正、必須引数不足など）
- `1`: その他実行時エラー

### `--emit-pipeline`（診断出力）

- グローバル引数として利用可能: `dataq --emit-pipeline <subcommand> ...`
- サブコマンド側でも利用可能: `dataq <subcommand> ... --emit-pipeline`
- 有効時は stderr に pipeline JSON を1行追加出力
- 本処理の stdout は従来どおり（既存出力互換）

pipeline JSON schema:

- `command`: 実行サブコマンド名
- `input`: 入力ソース情報（stdin/path, format）
- `steps`: 実行ステップ配列
- `external_tools`: `jq|yq|mlr` の使用有無
- `deterministic_guards`: 適用した決定性ガード

```bash
cat in.json | dataq --emit-pipeline canon --from json > out.json 2> pipeline.json
```

### 外部ツール連携の方針

- ユーザー入力はシェル文字列展開せず、外部ツール連携時も明示的な引数配列で扱う

## 関連ドキュメント

- 設計方針・構造: [architecture.md](./architecture.md)
- assert ルール仕様: [rules-schema.md](./rules-schema.md)
