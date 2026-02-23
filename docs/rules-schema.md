# dataq Rules Schema (assert)

## 概要

`dataq assert --rules <path>` で利用するルールファイル仕様です。  
ルールスキーマは厳密で、未知キーは入力不正（終了コード `3`）として扱います。

CLIから同内容の機械可読ヘルプを取得する場合:

```bash
dataq assert --rules-help
```

JSON Schema検証モードの機械可読ヘルプを取得する場合:

```bash
dataq assert --schema-help
```

## まず全キーを把握

| キー | 型 | 目的 |
| --- | --- | --- |
| `extends` | `string \| string[]` | 親ルールファイルを再利用（親から順にマージ） |
| `required_keys` | `string[]` | 各レコードで必須にするキー（パス） |
| `forbid_keys` | `string[]` | 各レコードで禁止するキー（パス） |
| `fields` | `object<string, object>` | パスごとの検証ルール集約 |
| `count` | `object` | 入力レコード件数の制約（`min`, `max`） |

`type` に指定可能な値は `string`, `number`, `integer`, `boolean`, `object`, `array`, `null` です。

## 例

```yaml
extends: [./base.rules.yaml]
required_keys: [id, status]
forbid_keys: [debug, meta.blocked]
fields:
  id:
    type: integer
  score:
    type: number
    nullable: true
    range:
      min: 0
      max: 100
  status:
    enum: [active, archived]
  name:
    pattern: '^[a-z]+_[0-9]+$'
count:
  min: 1
  max: 1000
```

## パス指定ルール（`required_keys` など共通）

- パスは `.` 区切りのオブジェクトキーで指定します（例: `meta.blocked`）。
- 空文字や空セグメント（例: `a..b`）は無効です。
- 配列インデックス記法はサポートしません（`assert` では各行オブジェクトに対して同一パスを適用）。
- ミスマッチ出力ではレコード位置が先頭に付き、`$[0].meta.blocked` のような形式になります。

## キー別リファレンス

### `extends`

- 型: `string` または `string[]`
- 意味: 参照先ルールを先に読み込み、最後に現在ファイルを適用します（現在ファイル優先）。
- パス解決: 現在ファイルの親ディレクトリ基準の相対パス。
- マージ規則:
  - `required_keys`: 和集合（重複排除・決定的順序）
  - `forbid_keys`: 和集合（重複排除・決定的順序）
  - `fields`: パス単位で後勝ち上書き
  - `count`: 最後に `count` を定義したファイルを採用

### `required_keys`

- 型: `string[]`
- 意味: 指定パスが存在しない場合にミスマッチ（`rule_kind: "required_keys"`, `reason: "missing_key"`）。
- 補足: 重複パスは内部で重複排除されます。

### `forbid_keys`

- 型: `string[]`
- 意味: 指定パスが存在した場合にミスマッチ（`rule_kind: "forbid_keys"`, `reason: "forbidden_key"`）。
- 補足: 重複パスは内部で重複排除されます。

### `fields`

- 型: `object<string, object>`
- 意味: パス単位で以下の制約をまとめて定義します。

`fields.<path>` で指定できるキー:

| キー | 型 | 説明 |
| --- | --- | --- |
| `type` | `type` | 型制約 |
| `nullable` | `bool` | `null` 許容フラグ |
| `enum` | `any[]` | 許容値一覧 |
| `pattern` | `string` | 正規表現（文字列値に適用） |
| `range` | `object` | 数値範囲（`min`, `max`） |

`fields.<path>` は空オブジェクト不可です（上記のいずれか1つ以上が必要）。

`fields.<path>.type`:

- 失敗時:
  - キー欠落: `reason: "missing_key"`
  - 型不一致: `reason: "type_mismatch"`

`fields.<path>.nullable`:

- `false`: そのパスの `null` を不許可（`reason: "null_not_allowed"`）。
- `true`: 同パスの `type` / `enum` / `pattern` / `range` で `null` を許可。

`fields.<path>.enum`:

- 失敗時:
  - キー欠落: `reason: "missing_key"`
  - 不一致: `reason: "enum_mismatch"`

`fields.<path>.pattern`:

- 失敗時:
  - キー欠落: `reason: "missing_key"`
  - 値が文字列以外: `reason: "pattern_not_string"`
  - 正規表現不一致: `reason: "pattern_mismatch"`
- 補足: ルール読込時に正規表現をコンパイルし、無効パターンは入力不正（終了コード `3`）です。

### `count`

- 型: `object`（`min?: usize`, `max?: usize`）
- 意味: 入力レコード件数（配列長）を検証します。
- 失敗時:
  - `min` 未満: `reason: "below_min_count"`
  - `max` 超過: `reason: "above_max_count"`
- 補足: `min > max` は入力不正（終了コード `3`）です。

`fields.<path>.range`:

- 型: `object`（`min?: number`, `max?: number`）
- 意味: 数値値の最小/最大を検証します。
- 失敗時:
  - キー欠落: `reason: "missing_key"`
  - 値が数値以外: `reason: "not_numeric"`
  - `min` 未満: `reason: "below_min"`
  - `max` 超過: `reason: "above_max"`
- 補足: `min > max` は入力不正（終了コード `3`）です。

## 入力不正（終了コード `3`）になる主なケース

- 未知キーが含まれる（トップレベル、`count`、`fields.<path>`、`fields.<path>.range` すべて厳密）
- 旧トップレベルキー（`types` / `nullable` / `enum` / `pattern` / `ranges`）を指定する
- パスが不正（空文字、空セグメント）
- `extends` が循環参照している
- `extends` の参照先が存在しない
- `extends` の形式が不正（`string` / `string[]` 以外）
- `count.min > count.max`
- `fields.<path>.range.min > fields.<path>.range.max`
- `pattern` の正規表現が無効
- ルールファイルが単一オブジェクトでない

## この形は依存ツール都合か？

`assert --rules` のスキーマは `dataq` の Rust 実装で直接パース・検証しており、`jq` / `yq` / `mlr` の入力仕様に合わせているわけではありません。  
依存ツール連携の有無に関わらず、`assert` では同じルールスキーマを使います（`--emit-pipeline` の `external_tools` も既定で未使用）。

## 関連ドキュメント

- CLI仕様: [command-spec.md](./command-spec.md)
- 設計方針・構造: [architecture.md](./architecture.md)
