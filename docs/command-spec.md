# dataq Command Spec

## コマンド一覧

共通形式:

```bash
dataq [--emit-pipeline] <command> [options]
```

サブコマンド:

- `canon`: 入力を決定的に正規化し、JSON/JSONLへ変換
- `assert`: ルールまたはJSON Schemaで検証
- `gate schema`: JSON Schemaで品質ゲートを実行（`assert --schema` ラッパー）
- `gate policy`: ルールベース品質ゲートを実行（`matched/violations/details`）
- `sdiff`: 2データセットの構造差分を出力
- `diff source`: 2ソース（preset/path）を解決して構造差分を出力
- `profile`: フィールド統計を決定的JSONで出力
- `join`: 2入力をキー結合してJSON配列を出力
- `aggregate`: グループ集計をJSON配列で出力
- `merge`: base + overlays をポリシーマージ（`--policy-path` で subtree 別上書き可）
- `doctor`: 依存ツール診断（`--profile` 指定でワークフロー別要件評価）
- `recipe run`: 宣言的レシピを定義順に実行
- `recipe lock`: 再現実行のための lock JSON を生成
- `contract`: サブコマンド出力契約を機械可読JSONで取得
- `emit plan`: サブコマンドの静的実行計画（stage/dependency/tool）を取得
- `mcp`: MCP(JSON-RPC 2.0) 単発リクエストを処理

## `contract` 出力契約（MVP）

- コマンド:
  - `dataq contract --command <canon|assert|gate-schema|gate|sdiff|diff-source|profile|merge|doctor|recipe-run|recipe-lock>`
  - `dataq contract --all`
- `--command` 出力: 単一オブジェクト
- `--all` 出力: 契約オブジェクト配列（決定的順序）
  - `canon`, `assert`, `gate-schema`, `gate`, `sdiff`, `diff-source`, `profile`, `merge`, `doctor`, `recipe-run`, `recipe-lock`
- 各オブジェクトの最低限キー:
  - `command`
  - `schema`
  - `output_fields`
  - `exit_codes`
  - `notes`
- 終了コード:
  - `0`: 成功
  - `3`: 入力不正（例: `--command` に未知値）
  - `1`: 予期しない内部エラー
- 副作用:
  - `contract` は参照専用（read-only）で、入力データやファイル内容を変更しない

## `emit plan` 出力契約（MVP）

- コマンド:
  - `dataq emit plan --command <subcommand> [--args <json-array>]`
- `--args`:
  - JSON配列文字列のみ受理（例: `'["--normalize","github-actions-jobs"]'`）
  - 配列要素はすべて文字列
- 出力キー:
  - `command`: 対象サブコマンド名
  - `args`: 計画解決に使った引数配列
  - `stages`: 段情報配列（`order`, `step`, `tool`, `depends_on`）
  - `tools`: `jq|yq|mlr` の期待利用有無（`expected`）
- 終了コード:
  - `0`: 成功
  - `3`: 未対応サブコマンドまたは `--args` 形式不正
  - `1`: 予期しない内部エラー
- 実行制約:
  - 計画解決は静的（外部コマンド実行なし）
- `--emit-pipeline` との違い:
  - `emit plan`: 実行前の静的計画
  - `--emit-pipeline`: 実行時に観測された診断

## `mcp` 単発JSON-RPC契約（MVP）

- 実行形式:
  - `dataq mcp`
- 入出力:
  - stdin: JSON-RPC 2.0 request 1件
  - stdout: JSON-RPC 2.0 response 1件
- 対応method:
  - `initialize`
  - `tools/list`
  - `tools/call`
- JSON-RPCエラーコード:
  - `-32700` parse error
  - `-32600` invalid request
  - `-32601` method not found
  - `-32602` invalid params
  - `-32603` internal error
- `tools/list` の tool 順序は固定:
  - `dataq.canon`
  - `dataq.assert`
  - `dataq.gate.schema`
  - `dataq.gate.policy`
  - `dataq.sdiff`
  - `dataq.diff.source`
  - `dataq.profile`
  - `dataq.join`
  - `dataq.aggregate`
  - `dataq.merge`
  - `dataq.doctor`
  - `dataq.contract`
  - `dataq.emit.plan`
  - `dataq.recipe.run`
  - `dataq.recipe.lock`
- `tools/call` 結果契約:
  - `result.structuredContent.exit_code: i32`
  - `result.structuredContent.payload: JSON`
  - `result.structuredContent.pipeline: JSON`（`emit_pipeline=true`時のみ）
  - `result.isError = (exit_code != 0)`
  - `result.content[0].text` は `structuredContent` と等価なJSON文字列
- `emit_pipeline`:
  - すべてのtoolで共通引数として受理（default: `false`）
  - `true` のときのみ `structuredContent.pipeline` を返す
  - 従来CLIの stderr pipeline 出力契約は不変（`mcp` ではstderrへ出さない）
- `dataq.doctor` の追加引数:
  - `profile`（任意）: `core|ci-jobs|doc|api|notes|book|scan`
- 競合入力（path + inline を同一logical inputで同時指定）:
  - JSON-RPCエラーではなく `tools/call` 成功レスポンス内で `exit_code=3` / `isError=true` を返す
- `mcp` モードのプロセス終了コード:
  - JSON-RPCレスポンスを書き出せた場合は tool `exit_code` に関係なく `0`
  - レスポンス出力不能な致命的I/O時のみ `3`

## `join` コマンド契約（MVP）

- コマンド:
  - `dataq join --left <path> --right <path> --on <field> --how <inner|left>`
- 出力: JSON 配列（stdout）
- 入力要件:
  - 左右入力の各レコードは object
  - `--on` で指定したキーは全レコードに存在
- 異常時契約:
  - 入力不正または結合実行失敗は exit `3`
- 実行方式:
  - `mlr` を明示的引数配列で実行（シェル展開なし）
  - `--emit-pipeline` で `stage_diagnostics` に `join_mlr_execute` を出力（`input_records`, `output_records`, `input_bytes`, `output_bytes`, `duration_ms`(固定 `0`), `status`）

## `aggregate` コマンド契約（MVP）

- コマンド:
  - `dataq aggregate --input <path> --group-by <field> --metric <count|sum|avg> --target <field>`
- 出力: JSON 配列（stdout）
- 出力フィールド:
  - `--metric count` のとき集計列は `count`
  - `--metric sum` のとき集計列は `sum`
  - `--metric avg` のとき集計列は `avg`
- 入力要件:
  - 各レコードは object
  - `group-by` と `target` は全レコードで必須
  - `sum` / `avg` は `target` が数値であることが必須
- 異常時契約:
  - 入力不正または集計実行失敗は exit `3`
- 実行方式:
  - `mlr` を明示的引数配列で実行（シェル展開なし）
  - `--emit-pipeline` で `stage_diagnostics` に `aggregate_mlr_execute` を出力（`input_records`, `output_records`, `input_bytes`, `output_bytes`, `duration_ms`(固定 `0`), `status`）

## `profile` 出力契約

- 既存キーは固定: `record_count`, `field_count`, `fields`, `type_distribution`
- `fields.<canonical-path>.numeric_stats` は後方互換な追加キー（数値サンプルが存在するときのみ出力）
- `numeric_stats` スキーマ:
  - `count`
  - `min`
  - `max`
  - `mean`
  - `p50`
  - `p95`
- 数値サンプル抽出対象は JSON number のみ
- パーセンタイル規則は nearest-rank で固定:
  - `rank = ceil(p * n)`（`p` は 0.50 / 0.95）
  - `index = rank - 1`（0始まり）
- `numeric_stats` の浮動小数は小数点以下6桁に丸めて出力

## このCLIの位置づけ

- `dataq` は `jq` / `yq` / `mlr` の代替ではなく、運用で繰り返す複合処理を短いコマンドに固定するための契約CLI
- 直接パイプ（`yq ... | jq ... | mlr ...`）で起きやすいI/O揺れ・終了コード揺れを、`dataq` 側で吸収して統一
- 探索や一時分析は各ツール単体、本番運用パイプラインは `dataq` で固定化する使い分けを想定

## `assert` 補助出力

- `dataq assert --rules-help` で `--rules` 用ルール仕様を機械可読JSONで出力
- `dataq assert --schema-help` で `--schema`（JSON Schema検証）の使い方と結果契約を機械可読JSONで出力
- このモードは検証処理を実行せず、終了コード `0` で終了
- `dataq assert --normalize github-actions-jobs|gitlab-ci-jobs` で生のCI定義を `yq -> jq -> mlr` の固定3段でジョブ単位レコードへ正規化してから `--rules` 検証可能（`yq`/`jq`/`mlr` 必須）

## `gate schema` 契約（MVP）

- コマンド:
  - `dataq gate schema --schema <path> [--input <path|->] [--from <preset>]`
- 目的:
  - JSON Schema 検証を専用 gate コマンドとして固定化
  - 出力JSONは `assert --schema` と同一形状（`matched`, `mismatch_count`, `mismatches`）
- `--from`:
  - 対応 preset: `github-actions-jobs`, `gitlab-ci-jobs`
  - 未対応 preset は明示的エラーで exit `3`
- `--emit-pipeline`:
  - `steps`: `gate_schema_ingest`, `gate_schema_validate`

## `gate policy` 契約（MVP）

- コマンド:
  - `dataq gate policy --rules <path> [--input <path|->] [--source <preset>]`
- 目的:
  - ルール検証結果を policy gate 用の固定形で返す
  - 出力JSONは `matched`, `violations`, `details`
- `--source`:
  - 対応 preset: `scan-text`, `ingest-doc`, `ingest-api`, `ingest-notes`, `ingest-book`
  - 未対応 preset は明示的エラーで exit `3`
- `--emit-pipeline`:
  - `steps`: `gate_policy_source`, `gate_policy_assert_rules`

## `merge` パス別ポリシー（MVP）

- 既存 `--policy` は全体デフォルトポリシーとして動作
- 追加 `--policy-path <canonical-path=policy>` は複数指定可能
  - 例: `--policy-path '$["spec"]["containers"]=array-replace'`
  - `canonical-path` は `$["field"][0]...` 形式を要求
  - `policy` は `last-wins | deep-merge | array-replace`
- ポリシー解決順:
  - 現在マージ中の値パスに対して、最長一致する `--policy-path` を適用
  - 最長一致が同一深さで複数ある場合は、後ろに指定した `--policy-path` を優先
  - 一致がなければ `--policy` を適用
- 入力不正:
  - `--policy-path` の path が非canonical、または policy が未知値の場合は exit `3`
  - `--policy-path` 未指定時の挙動は従来どおり

## 外部ツール多段連携（契約方針）

- 多段連携コマンドは、内部で `jq` / `yq` / `mlr` の1つ以上を段階実行して1つの結果JSONを返す
- 各段は役割を分離する:
  - `yq`: YAML抽出/整形
  - `jq`: JSON正規化/判定フラグ付け
  - `mlr`: 集計/結合/統計
- 使用段数や順序は機能ごとに定義し、CLI契約の一部として `--emit-pipeline` で追跡可能にする

## CLI I/O 契約

### 出力モード

- 既定: JSON（機械可読）
- `canon` のみ `--to jsonl` で JSONL 出力を選択可能
- `canon --to jsonl` かつ JSONL入力はレコード単位の逐次処理（出力順は入力順）

### `canon` 入力フォーマット解決

- `--from` 指定時は指定フォーマットを使用
- `--from` 未指定かつ `--input <path>` 指定時は拡張子で解決（`.json|.yaml|.yml|.csv|.jsonl|.ndjson`）
- `--from` 未指定かつ stdin入力時は固定順で自動判別:
  - `JSONL -> JSON -> YAML -> CSV`
- 非空行が1行のみで入力全体がJSONとして成立する場合は `JSON` を優先（JSON/JSONLの曖昧さ回避）
- 自動判別失敗は `input_usage_error` で終了コード `3`

### 終了コード

- `0`: 成功
- `2`: 検証失敗（期待仕様に不一致）
- `3`: 入力不正（フォーマット不正、必須引数不足など）または `doctor` の要件未達（`--profile` 未指定時は `jq|yq|mlr` 不足/起動不可、指定時は profile 要件未達）
- `1`: その他実行時エラー

## `doctor` コマンド契約（MVP）

- コマンド: `dataq doctor [--capabilities] [--profile <core|ci-jobs|doc|api|notes|book|scan>]`
- 出力: JSON（stdout）
- 診断対象ツール順: `jq`, `yq`, `mlr`（固定順）
- 各ツールの出力項目:
  - `name`: ツール名
  - `found`: PATH上に存在するか
  - `version`: 取得できたバージョン文字列（取得不可時は `null`）
  - `executable`: `--version` で起動できたか
  - `message`: 判定理由（失敗時は対処案内を含む）
- `--capabilities` 指定時:
  - `capabilities`: 固定順の capability probe 結果
  - 固定順: `jq.null_input_eval`, `yq.null_input_eval`, `mlr.help_command`
  - 各項目: `name`, `tool`, `available`, `message`
- `--profile` 指定時:
  - `capabilities`: 固定順の capability probe 結果（`*.available`）
  - `profile`: 要件評価結果
    - `version`: `dataq.doctor.profile.requirements.v1`
    - `name`: 指定プロフィール名
    - `description`: プロフィール用途
    - `satisfied`: 要件充足可否
    - `requirements[*]`: `capability`, `tool`, `reason`, `satisfied`, `message`
- 終了コード:
  - `0`: `--profile` 未指定時は `jq|yq|mlr` が全て起動可能、`--profile` 指定時は選択 profile 要件を充足
  - `3`: `--profile` 未指定時は `jq|yq|mlr` のいずれかが欠如または起動不可、`--profile` 指定時は選択 profile 要件未達
  - `1`: 予期しない内部エラー
- `--emit-pipeline` 指定時の `steps`:
  - `--profile` 未指定: `doctor_probe_tools`, `doctor_probe_capabilities`
  - `--profile` 指定時: `doctor_profile_probe`, `doctor_profile_evaluate`

### `sdiff` のCIゲート拡張

- `--fail-on-diff`（既定: `false`）:
  比較処理が成功し、かつ `values.total > 0` のとき終了コード `2` で終了
- `--value-diff-cap <usize>`（既定: `100`）:
  レポートの `values.items` 出力件数上限を制御
- レポートJSON契約（`counts`, `keys`, `ignored_paths`, `values`）は不変
- `values.total` は実差分件数を維持し、上限超過時のみ `values.truncated=true`
- `--emit-pipeline` のstderr JSON出力契約は `sdiff` 拡張後も不変

### `diff source` コマンド契約（MVP）

- コマンド:
  - `dataq diff source --left <preset-or-path> --right <preset-or-path> [--fail-on-diff]`
- source 指定:
  - file: `<path>`
  - preset: `preset:<github-actions-jobs|gitlab-ci-jobs>:<path>`
- 出力:
  - `sdiff` レポート（`counts`, `keys`, `ignored_paths`, `values`）を維持
  - `sources.left` / `sources.right` に解決メタデータ（`kind`, `preset?`, `path`, `format`）を追加
- 終了コード:
  - `0`: 成功
  - `2`: `--fail-on-diff` かつ `values.total > 0`
  - `3`: source解決またはpreset指定エラー
  - `1`: 予期しない内部エラー
- `--emit-pipeline`:
  - `steps` は `diff_source_resolve_left`, `diff_source_resolve_right`, `diff_source_compare`

### `--emit-pipeline`（診断出力）

- グローバル引数として利用可能: `dataq --emit-pipeline <subcommand> ...`
- サブコマンド側でも利用可能: `dataq <subcommand> ... --emit-pipeline`
- 有効時は stderr に pipeline JSON を1行追加出力
- 本処理の stdout は従来どおり（既存出力互換）

pipeline JSON schema:

- `command`: 実行サブコマンド名
- `input`: 入力ソース情報（stdin/path, format）
- `steps`: 実行ステップ配列
- `external_tools`: 外部ツールの使用有無。通常は `jq|yq|mlr`（固定順）。`doctor --profile` では `jq|yq|mlr|pandoc|xh|nb|mdbook|rg`（probe順）を出力
- `stage_diagnostics` (optional): 段ごとの診断情報（`order`, `step`, `tool`, `input_records`, `output_records`, `status`）
  - 追加メトリクス: `input_bytes`, `output_bytes`, `duration_ms`（決定性保持のため固定 `0`）
  - 後方互換: 既存フィールド（`order`, `step`, `tool`, `input_records`, `output_records`, `status`）は不変
- `fingerprint`: 実行フィンガープリント（`command`, `args_hash`, `input_hash`(optional), `tool_versions`(使用ツールのみ), `dataq_version`）
- `deterministic_guards`: 適用した決定性ガード
- `assert --rules-help`/`--schema-help` では `steps` が `emit_assert_rules_help` / `emit_assert_schema_help` になる
- `recipe run` では `steps` に `load_recipe_file`, `validate_recipe_schema`, `execute_step_<index>_<kind>` が入る
- `emit plan` では `steps` が `emit_plan_parse`, `emit_plan_resolve` になる
- `recipe lock` では `steps` に `recipe_lock_parse`, `recipe_lock_probe_tools`, `recipe_lock_fingerprint` が入る

```bash
cat in.json | dataq --emit-pipeline canon --from json > out.json 2> pipeline.json
```

### 外部ツール連携の方針

- `dataq` は外部ツールを運用上の依存として扱い、CLI契約（JSON/終了コード）をRust層で統一する
- ユーザー入力はシェル文字列展開せず、外部ツール連携時も明示的な引数配列で扱う

## `recipe run` MVP スキーマ

- 実行形式: `dataq recipe run --file <path>`
- レシピファイル形式: 拡張子解決で JSON / YAML をサポート
- `version`: `dataq.recipe.v1` 固定
- `steps`: 実行順配列（定義順で処理）
- `steps[*].kind`: `canon | assert | profile | sdiff`
- `steps[*].args`: 各 step の引数オブジェクト
- step 間データ受け渡し: in-memory
- サマリ出力: stdout JSON に `matched`, `exit_code`, `steps`
- 異常時契約:
  - スキーマ不正 / 未知step / 引数不正は exit `3`
  - `assert` / `sdiff` の不一致は exit `2`

## `recipe lock` MVP スキーマ

- 実行形式: `dataq recipe lock --file <recipe-path> [--out <lock-path>]`
- `--out` 未指定時は stdout に lock JSON を出力
- `--out` 指定時は lock JSON を指定パスへ書き込み、stdout は空
- lock JSON:
  - `version`: `dataq.recipe.lock.v1`
  - `command_graph_hash`
  - `args_hash`
  - `tool_versions`（使用ツールのみ。キーはツール名の辞書順: `jq` / `mlr` / `yq`）
  - `dataq_version`
- pipeline ステップ:
  - `recipe_lock_parse`
  - `recipe_lock_probe_tools`
  - `recipe_lock_fingerprint`
- 異常時契約:
  - レシピファイル不正 / step引数不正は exit `3`
  - ツール解決失敗（未存在/非実行可能/版数取得失敗）は exit `3`

## 関連ドキュメント

- 設計方針・構造: [architecture.md](./architecture.md)
- assert ルール仕様: [rules-schema.md](./rules-schema.md)
