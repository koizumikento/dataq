# dataq

`dataq` は、JSON / YAML / CSV とドキュメント入力を対象にした「決定的な前処理・検証・差分」CLIです。  
AI処理そのものは行わず、エージェントやCIから呼びやすい機械可読I/Oを提供します。

## これは何か（3行要約）

- `dataq` は `jq` / `yq` / `mlr` の「よく使う組み合わせ」を単一CLIにまとめるための契約レイヤーです
- 実行のオーケストレーションは Rust 側で行い、必要に応じて `jq` / `yq` / `mlr` の多段連携（例: `yq -> jq -> mlr`）を内部実行しつつ、出力JSONと終了コード契約を固定します
- 探索は各ツール単体、運用パイプラインは `dataq` で再利用する使い分けを想定しています

## 目的

- データ変換を再現可能にする（同じ入力なら同じ出力）
- 失敗を終了コードとJSONで明確化する
- `jq` / `yq` / `mlr` を組み合わせた処理を、短い固定コマンドとして再利用可能にする

## 立ち位置（`jq` / `yq` / `mlr` との関係）

| 観点 | dataq | jq / yq / mlr |
| --- | --- | --- |
| 主目的 | よく使う複合パイプラインを契約化して再利用 | 抽出・変換・集計の表現力 |
| 実行モデル | Rustオーケストレータ + 必要時 `pandoc/jq/yq/mlr` 連携 | 各ツールのDSL/フィルタ実行 |
| 出力契約 | 機械可読JSONを既定、スキーマ化しやすい | フィルタ次第で形式が変動 |
| 終了コード契約 | `0/2/3/1` を意味付きで固定 | ツールごとに意味が異なる |
| 決定性ガード | キー順・時刻正規化・差分順序などを固定 | フィルタ/オプション次第 |
| 診断 | `--emit-pipeline` で内部ステップをJSON出力 | 同等の共通仕様はない |

## 使い分け

- `dataq` を使う場面:
  CI品質ゲート、前処理の再実行保証、チームで共通化したいパイプライン、差分の定常監視
- `jq` / `yq` / `mlr` を使う場面:
  ワンライナー探索、複雑な抽出クエリ、対話的な整形や一時分析
- 併用の考え方:
  探索は `jq` / `yq` / `mlr`、本番の再利用パイプラインは `dataq`（契約を `dataq` 側に寄せる）

## 生パイプラインとの違い

- 生パイプライン:
  `yq ... | jq ... | mlr ...` のように都度書けるが、引数差分・エラー解釈・終了コードが揺れやすい
- `dataq`:
  同等の処理意図をサブコマンド化し、I/O形式・失敗JSON・終了コードを固定できる
- 監査性:
  `--emit-pipeline` で、内部処理ステップ・外部ツール使用有無・`stage_diagnostics`（段ごとの順序/件数/バイト数/`duration_ms`(決定性保持のため常に`0`)/状態）に加えて、`fingerprint`（`args_hash` / `input_hash`(optional) / 使用ツール版数 / `dataq_version`）を機械可読で残せる

## 外部ツール多段連携の方針

- `dataq` の一部コマンドは、内部的に複数ツールを段階実行することで価値を成立させます
- これは「3ツールの代替」ではなく「3ツールの合わせ技を再利用可能な契約として固定する」ための設計です
- 多段連携コマンドでは、`--emit-pipeline` で各段の利用ツール・ステップ順・件数/バイト数変化・`duration_ms`(決定性保持のため常に`0`)・失敗段を追跡可能にします

## 最小依存と外部ツール一覧

- 最小運用（`dataq doctor` の既定診断対象）は `jq` / `yq` / `mlr`
- 追加機能はコマンド別に必要なツールだけ導入する運用を想定

| 用途 | 必要ツール |
| --- | --- |
| コア（canon/assert/gate/sdiff/profile/join/aggregate など） | `jq`, `yq`, `mlr` |
| `ingest api` | `xh`, `jq` |
| `ingest doc` | `pandoc`, `jq` |
| `ingest tabular` | `csvkit`（`in2csv`, `csvjson`） |
| `ingest jc` | `jc` |
| `ingest notes` | `nb`, `jq` |
| `ingest book` | `jq`（`DATAQ_INGEST_BOOK_VERIFY_MDBOOK` 有効時は `mdbook` も必要） |
| `schema infer` | `qsv` |
| `scan text` | `rg`（`--jq-project` を使う場合は `jq` も必要） |
| `transform rowset` | `jq`, `mlr`（`--emit-pipeline` の stage2 tool label は `mlr`） |
| `transform sql` | `duckdb` |

補足:
- `doctor --profile` でワークフロー別に必要ツールの充足を診断できます（`doc` / `api` / `notes` / `book` / `scan` など）。

## コマンド一覧

共通形式:

```bash
dataq [--emit-pipeline] <command> [options]
```

サブコマンド一覧（`./target/debug/dataq --help` ベース）:

| Command | 用途 | 必須オプション |
| --- | --- | --- |
| `canon` | 入力を決定的に正規化し、JSON/JSONLへ変換 | `--from <format>`（`format`: `json` / `yaml` / `csv` / `jsonl`。stdin時は省略可） |
| `ingest api` | HTTP API 応答を `xh -> jq` で決定的JSONへ正規化 | `--url <http(s)://...>` |
| `ingest yaml-jobs` | YAMLのCIジョブ定義を正規化JSON配列へ変換 | `--input <path-or-stdin>` `--mode <mode>`（`mode`: `github-actions` / `gitlab-ci` / `generic-map`） |
| `ingest tabular` | 表形式入力を `csvkit` で決定的JSON配列へ正規化 | `--input <path-or-stdin>` |
| `ingest jc` | 半構造テキストを `jc` で決定的JSONエンベロープへ変換 | `--parser <name>` |
| `ingest notes` | `nb` ノートをフィルタ・正規化してJSON/JSONLで出力 | なし（`--tag`/`--since`/`--until`/`--to` は任意） |
| `ingest book` | mdBook `SUMMARY.md` とメタ情報を決定的JSONへ変換 | `--root <path>` |
| `assert` | ルール or JSON Schema で検証 | `--rules <path>` または `--schema <path>` |
| `gate schema` | JSON Schema で品質ゲートを実行（`assert --schema` の専用ラッパー） | `--schema <path>` |
| `gate policy` | ルールベース品質ゲートを実行（違反詳細を決定的順序で出力） | `--rules <path>` |
| `schema infer` | 表形式入力から `qsv` で JSON Schema を推定 | なし（`--input <path-or-stdin>` は任意） |
| `sdiff` | 2データセットの構造差分を出力 | `--left <path>` `--right <path>` |
| `diff source` | 2ソース（preset/path）を解決して構造差分を出力 | `--left <preset-or-path>` `--right <preset-or-path>` |
| `profile` | フィールド統計を決定的JSONで出力 | `--from <format>`（`format`: `json` / `yaml` / `csv` / `jsonl`）, `--field <name-or-path>`（複数可）, `--allow-missing-fields`, `--brief`, `--max-fields <n>`, `--sort-fields <path\|unique_count\|null_ratio>` |
| `ingest doc` | ドキュメントを共通JSONスキーマへ抽出 | `--input <path-or-stdin>` `--from <format>`（`format`: `md` / `html` / `docx` / `rst` / `latex`） |
| `join` | 2入力をキー結合してJSON配列を出力 | `--left <path>` `--right <path>` `--on <field>` `--how <how>`（`how`: `inner` / `left`） |
| `aggregate` | グループ単位の集計をJSON配列で出力 | `--input <path>` `--group-by <field>` `--metric <metric>`（`metric`: `count` / `sum` / `avg`） `--target <field>`（任意: `--sort-by <group\|metric>` `--order <asc\|desc>` `--limit <n>`） |
| `scan text` | テキストを正規表現で走査し構造化結果を出力 | `--pattern <regex>` |
| `transform rowset` | `jq -> mlr` の2段でrowsetを変換しJSON配列を出力 | `--input <path-or-stdin>` `--engine <sqlite>` `--jq-filter <filter>` `--mlr <verb...>` |
| `transform sql` | `duckdb` でSQL変換し、DuckDB返却順のJSON配列を出力 | `--input <path-or-stdin>` `--query <sql>` `--engine <duckdb>` |
| `merge` | base + overlays をポリシーマージ | `--base <path>` `--overlay <path>...` `--policy <policy>`（`policy`: `last-wins` / `deep-merge` / `array-replace`） `--policy-path <path=policy>...` |
| `doctor` | 依存診断（`--capabilities`/`--profile` 対応） | なし |
| `recipe run` | 宣言的レシピを定義順で実行 | `--file <path>` |
| `recipe lock` | レシピ再現実行用のロック情報を生成 | `--file <path>` |
| `recipe replay` | lock 制約を検証してレシピを再実行 | `--file <recipe-path>` `--lock <lock-path>` |
| `contract` | サブコマンド出力契約を機械可読JSONで取得 | `--command <name>` または `--all` |
| `emit plan` | サブコマンドの静的実行計画（stage/dependency/tool）を出力 | `--command <name>` |
| `codex install-skill` | 埋め込み済み dataq skill を Codex skills root に配置 | `--dest <dir>`（省略時は `CODEX_HOME/skills` → `HOME/.agents/skills`） |
| `mcp` | 1リクエスト単位の MCP(JSON-RPC 2.0) サーバーモード | stdin で JSON-RPC リクエストを1件入力 |

グローバルオプション:

- `--emit-pipeline`: stderr に pipeline JSON を1行追加出力（`fingerprint` を含む）
  - `fingerprint.tool_versions` は実際に呼び出す外部ツール実体を対象に採取（`DATAQ_JQ_BIN` / `DATAQ_YQ_BIN` / `DATAQ_MLR_BIN` / `DATAQ_PANDOC_BIN` / `DATAQ_DUCKDB_BIN` / `DATAQ_QSV_BIN` を尊重）
- `-h, --help`: ヘルプ
- `-V, --version`: バージョン

stdout のパイプ契約:

- 後段プロセスが stdout を閉じたことによる `BrokenPipe` は、consumer-closed の正常終了として扱い、コマンド本来の終了コードが `2` でもプロセス終了コードは `0` にする
- `BrokenPipe` では panic や `internal_error` を stderr に出さない。`--emit-pipeline` など明示的に要求された stderr 診断は stdout と分離したまま維持する
- `BrokenPipe` 以外の stdout write/flush 失敗は `internal_error` として終了コード `1` にする。stdout を使わない入力・使用エラーの終了コード `3` は従来どおり

## LLM / agent quickstart

LLM やエージェントに `dataq` を使わせるときは、長い README 全体を読むより、最初に環境・契約・データ形状を短く確認すると失敗を切り分けやすくなります。探索中の一時的な加工は `jq` / `yq` / `mlr` で構いませんが、再利用するパイプラインは `dataq` の固定コマンドと出力契約に寄せます。

```bash
# 1. 依存確認。core は jq/yq/mlr を中心に確認する
dataq doctor --profile core

# 2. エージェントに渡す前に出力契約と終了コードを確認する
dataq contract --command profile

# 3. 実データを読まずに静的な stage / tool 計画を確認する
dataq emit plan --command aggregate \
  --args '["--input","orders.json","--group-by","team","--metric","sum","--target","price","--sort-by","metric","--order","desc","--limit","10"]'

# 4. LLM のコンテキストを圧迫しない形で入力の列・型・ユニーク数を把握する
dataq profile --from json --input orders.json --brief \
  --sort-fields unique_count --max-fields 20

# 5. group count / sum の top-k は aggregate で決定的に返す
dataq aggregate --input orders.json --group-by team --metric sum --target price \
  --sort-by metric --order desc --limit 10

# 6. 投影や複雑な確認集計は SQL で再利用可能にし、順序が必要なら ORDER BY を書く
dataq transform sql --input orders.json --engine duckdb \
  --query 'SELECT team, SUM(price) AS revenue FROM input GROUP BY team ORDER BY revenue DESC LIMIT 10'

# 7. 実行時診断は stdout の結果とは分けて stderr に残す
dataq --emit-pipeline aggregate --input orders.json --group-by team --metric sum --target price \
  --sort-by metric --order desc --limit 10 > top-teams.json 2> pipeline.json
```

- `doctor`: エージェント実行前の依存ツール確認
- `contract`: stdout JSON の主要フィールドと exit code 契約の確認
- `emit plan`: 実データを読まない stage / dependency / tool の事前確認
- `profile --brief`: LLM 向けの省コンテキストなデータ概観
- `aggregate`: group metric と top-k の決定的な集計
- `transform sql`: 投影、結合前確認、複雑な集計を SQL として固定
- `--emit-pipeline`: 実行時の stage 診断、fingerprint、外部ツール使用状況の保存

## 基本的な使い方

```bash
# YAMLを正規化してJSONLへ
cat in.yaml | dataq canon --from yaml --to jsonl > out.jsonl

# stdin入力は --from 省略時に JSONL -> JSON -> YAML -> CSV の順で自動判別
# ただし非空行が1行のみで全体がJSONとして成立する場合は JSON を優先（曖昧さ回避）
cat events.jsonl | dataq canon --to jsonl > out.jsonl

# ルール検証
dataq assert --input out.jsonl --rules rules.yaml

# API応答を取得して正規化
dataq ingest api --url https://example.test/items --header 'accept:application/json'

# YAMLのCIジョブ定義を正規化
dataq ingest yaml-jobs --input .github/workflows/ci.yml --mode github-actions > jobs.json
dataq assert --input jobs.json --rules examples/assert-rules/github-actions/jobs.rules.yaml

# 表形式データをcsvkit経由で正規化
dataq ingest tabular --input orders.csv > rows.json

# JSON Schema 検証
dataq assert --input out.jsonl --schema schema.json

# schema 専用ゲート（assert --schema からの移行先）
dataq gate schema --input out.jsonl --schema schema.json

# policy 専用ゲート（rules 検証 + violation 出力）
dataq gate policy --input out.jsonl --rules rules.json --source scan-text

# 差分確認
dataq sdiff --left before.jsonl --right after.jsonl

# 品質プロファイル
dataq profile --from json --input out.jsonl
dataq profile --from json --brief --sort-fields unique_count --max-fields 20

# ドキュメント抽出（pandoc AST -> jq 投影）
dataq ingest doc --input README.md --from md

# 内部結合（idキー）
dataq join --left users.json --right scores.json --on id --how inner

# グループ集計（team単位でprice平均）
dataq aggregate --input orders.json --group-by team --metric avg --target price

# 集計metric上位10件（同点はgroupキー昇順）
dataq aggregate --input orders.json --group-by team --metric sum --target price \
  --sort-by metric --order desc --limit 10

# テキスト走査（policy mode ではヒット時に終了コード2）
dataq scan text --pattern 'TODO|FIXME' --path . --glob '*.rs' --policy-mode

# rowset変換（stage1: jq, stage2: mlr）
dataq transform rowset --input orders.json --engine sqlite --jq-filter '.' --mlr stats1 -a mean -f price -g team

# SQL変換（DuckDB）
dataq transform sql --input orders.json \
  --engine duckdb \
  --query 'SELECT team, AVG(price) AS avg_price FROM input GROUP BY team ORDER BY team'

# ポリシーマージ
dataq merge --base base.yaml --overlay patch1.json --overlay patch2.yaml --policy deep-merge

# 依存ツール診断
dataq doctor

# 依存ツールの機能診断
dataq doctor --capabilities

# ワークフロー別プリフライト（例: scan）
dataq doctor --profile scan

# assert 出力契約を取得
dataq contract --command assert

# assert の静的ステージ計画を取得
dataq emit plan --command assert --args '["--normalize","github-actions-jobs"]'

# dataq skill を Codex skills root へ配置
dataq codex install-skill

# lock 制約付きでレシピを再実行（ミスマッチでも実行継続）
dataq recipe replay --file recipe.json --lock recipe.lock.json

# MCP単発リクエスト（tools/list）
printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}' | dataq mcp

# ID で対応付けし、更新時刻は差分対象外
dataq sdiff --left before.jsonl --right after.jsonl --key '$["id"]' --ignore-path '$["updated_at"]'

# CIゲート: 差分があれば終了コード2、値差分詳細は先頭1件まで
dataq sdiff --left before.jsonl --right after.jsonl --fail-on-diff --value-diff-cap 1

# CI定義を preset 経由で正規化して差分比較
dataq diff source \
  --left 'preset:github-actions-jobs:.github/workflows/ci.yml' \
  --right expected-jobs.json \
  --fail-on-diff

# JSON入力をそのままdataqで検証
dataq assert --input raw.json --rules rules.yaml
```

## OSS基本情報

### インストール

```bash
cargo install --path .
```

#### Homebrew（tap）

```bash
brew tap koizumikento/stray-tools https://github.com/koizumikento/stray-tools.git
brew install koizumikento/stray-tools/dataq
```

上記 formula は連携ツール依存（`jq`, `yq`, `miller`(=`mlr`), `csvkit`, `jc`, `qsv`, `duckdb`, `check-jsonschema`, `pandoc`, `xh`, `ripgrep`, `nb`, `mdbook`）も併せて導入します。

詳細な設定手順は `docs/homebrew-tap.md` を参照してください。

#### Claude Code plugin

このリポジトリは Claude Code plugin 構成も含みます。

- plugin manifest: `.claude-plugin/plugin.json`
- skills:
  - `skills/dataq/SKILL.md`
  - `skills/dataq-rules-recipes/SKILL.md`

ローカルで plugin として読み込む例:

```bash
claude --plugin-dir .
```

plugin 定義の検証:

```bash
claude plugin validate .
```

### 開発（ローカル検証）

Rustコードや挙動に影響する変更時は、次を実行してください（`*.md` のみの更新では任意）。

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
cargo llvm-cov --workspace --all-features --fail-under-lines 80 --fail-under-regions 75
```

### Release

- `v*` タグ（例: `v0.1.0`, `v0.1.0-rc.1`）を push すると、GitHub Actions の Release workflow が起動します
- リリースノートは `docs/releases/` 配下に `vX.Y.Z.md` 形式で記録します
- `docs/releases/<tag>.md` は必須です。存在しない、または空ファイルの場合は Release workflow が失敗します
- GitHub Release 本文には `docs/releases/<tag>.md` の内容をそのまま使用します
- workflow は `cargo fmt --all -- --check`、`cargo clippy --workspace --all-targets --all-features -- -D warnings`、`cargo test --workspace --all-features`、`cargo llvm-cov --workspace --all-features --fail-under-lines 80 --fail-under-regions 75` を通過した場合のみ公開処理へ進みます
- 配布ターゲットは次の4種類です:
  - `x86_64-unknown-linux-gnu`
  - `x86_64-pc-windows-msvc`
  - `x86_64-apple-darwin`
  - `aarch64-apple-darwin`
- 各ターゲットで `dataq-<tag>-<target>.<ext>` と `dataq-<tag>-<target>.sha256` を GitHub Release に添付します
- タグ名に `-` を含む場合（例: `v0.1.0-rc.1`）は GitHub Pre-release として公開します
- この workflow は `crates.io` 公開を行いません（将来は別 workflow で分離予定）
- Release 公開後、`publish-homebrew-tap.yml` により tap リポジトリの `Formula/dataq.rb` を自動更新できます（設定手順: `docs/homebrew-tap.md`）

### コントリビュート

Issue / Pull Request を歓迎します。開発ルールは `AGENTS.md` を参照してください。  
外部コントリビュータ向けの `CONTRIBUTING.md` は追加予定です。

### セキュリティ

脆弱性の報告手順は `SECURITY.md` で整備予定です。  
機密性のある内容は公開Issueに直接記載しないでください。

### ライセンス

このプロジェクトは MIT License で提供します。詳細は `LICENSE` を参照してください。

## サブコマンド詳細（MVP）

### 1. `canon`

入力（JSON/YAML/CSV/JSONL）を決定的に正規化し、JSON もしくは JSONL へ変換。

- `--from` 省略時（stdin入力のみ）は固定順で自動判別: `JSONL -> JSON -> YAML -> CSV`
- 非空行が1行のみで入力全体がJSONとして成立する場合は、曖昧さ回避のため `JSON` として扱う
- 自動判別できない入力は `input_usage_error`（終了コード `3`）
- CSVヘッダー名は完全一致・大文字小文字を区別して一意である必要があり、重複時は最初と重複側の0始まり列indexを含む `input_usage_error`（終了コード `3`）を返す
- `--to jsonl` かつ JSONL入力ではレコード単位で逐次処理（入力順を保持）

- キー順ソート
- 型寄せ（数値/真偽値/日時）
- `--sort-keys=false` で入力キー順を保持可能

### 2. `assert`

期待ルールまたは JSON Schema に対して検証。

- 必須キー
- 禁止キー
- フィールド制約（`fields.<path>` に `type` / `enum` / `pattern` / `nullable` / `range` を集約）
- 最小/最大件数
- `--rules <path>`: dataq ルールで検証（ルールスキーマは厳密。未知キーは入力不正）
- ルールは `extends` で再利用可能（親相対パス解決、循環/欠損/不正形式は入力不正）
- `extends` マージ: `required_keys`/`forbid_keys` は和集合、`fields` はパス後勝ち、`count` は最後に定義された値を採用
- `--schema <path>`: JSON Schema で検証
- `--engine <jsonschema|ajv|checkjs>`: `--schema` 時の検証エンジンを選択（既定: `jsonschema`。`ajv` は `ajv` CLI と `DATAQ_AJV_BIN`、`checkjs` は `check-jsonschema` CLI と `DATAQ_CHECK_JSONSCHEMA_BIN` で上書き可）
- `--normalize <github-actions-jobs|gitlab-ci-jobs>`: 生のCI定義を `yq -> jq -> mlr` の3段でジョブ単位レコードへ正規化してから検証（`yq`/`jq`/`mlr` 必須）
- `--rules` と `--schema` は同時指定不可（入力不正として終了コード `3`）
- `--rules-help`: `--rules` 用ルール仕様を機械可読JSONで出力して終了（終了コード `0`）
- `--schema-help`: `--schema`（JSON Schema検証）用の使い方と結果契約を機械可読JSONで出力して終了（終了コード `0`）
- `mismatches[].expected`（schemaモード）には `engine`, `instance_path`, `schema_path`, `keyword`（任意）, `message` を正規化して出力

失敗時は機械可読エラーJSONを返し、終了コード `2`。  
`mismatches[]` は `path`, `rule_kind`, `reason`, `actual`, `expected` を含みます。

`assert` ルール例:

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

ルール仕様をCLIから取得:

```bash
dataq assert --rules-help
```

JSON Schemaモード仕様をCLIから取得:

```bash
dataq assert --schema-help
```

サービス定義向けのサンプルルール:

- 配置先: `examples/assert-rules/`
- 対象: `cloud-run`, `github-actions`, `gitlab-ci`
- 方式:
  - `raw.rules.yaml`: 生のYAML構造を検証
  - `jobs.rules.yaml`: `--normalize` でジョブ単位に正規化して検証（`yq -> jq -> mlr` の3段方式）

例（Cloud Run の raw 検証）:

```bash
dataq assert --input service.yaml --rules examples/assert-rules/cloud-run/raw.rules.yaml
```

例（GitHub Actions の jobs 検証）:

```bash
dataq assert \
  --input .github/workflows/ci.yml \
  --normalize github-actions-jobs \
  --rules examples/assert-rules/github-actions/jobs.rules.yaml
```

### 2.1 `gate schema`

`assert --schema` と同じ JSON Schema 検証レポートを、schema gate 用コマンドとして明示化。

- コマンド: `dataq gate schema --schema <path> [--input <path|->] [--from <preset>]`
- 出力JSON: `assert --schema` と同一（`matched`, `mismatch_count`, `mismatches`）
- 終了コード:
  - `0`: すべて一致
  - `2`: schema mismatch
  - `3`: schema/input/`--from` の入力不正
  - `1`: 予期しない内部エラー
- `--from`（任意）:
  - `github-actions-jobs`
  - `gitlab-ci-jobs`
  - 未対応 preset は明示的エラーで終了コード `3`
- 移行ガイド:
  - 旧: `dataq assert --schema schema.json --input in.json`
  - 新: `dataq gate schema --schema schema.json --input in.json`

### 2.2 `gate policy`

ルールベース検証の結果を policy gate 用の固定出力として返す。

- コマンド: `dataq gate policy --rules <path> [--input <path|->] [--source <preset>]`
- 出力JSON: `matched`, `violations`, `details`
- 終了コード:
  - `0`: すべて一致
  - `2`: policy violation を検出
  - `3`: rules/input/source の入力不正
  - `1`: 予期しない内部エラー
- `--source`（任意）:
  - `scan-text`
  - `ingest-doc`
  - `ingest-api`
  - `ingest-notes`
  - `ingest-book`

### 2.3 `ingest yaml-jobs`

YAMLのCIジョブ定義を `yq -> jq -> mlr` の固定3段で正規化し、決定的JSON配列へ変換します。

- コマンド: `dataq ingest yaml-jobs --input <path|-> --mode <github-actions|gitlab-ci|generic-map>`
- `--mode github-actions`: `job_id`, `runs_on`, `steps_count`, `uses_unpinned_action`
- `--mode gitlab-ci`: `job_name`, `stage`, `script_count`, `uses_only_except`
- `--mode generic-map`: `job_name`, `field_count`, `has_stage`, `has_script`
- `--emit-pipeline` の `steps`: `ingest_yaml_jobs_yq_extract`, `ingest_yaml_jobs_jq_normalize`, `ingest_yaml_jobs_mlr_shape`
- malformed YAML、未知 mode、`jq`/`yq`/`mlr` 不足は終了コード `3`

### 2.4 `ingest jc`

半構造テキスト入力を `jc` でパースし、決定的な JSON エンベロープで返します。

- コマンド: `dataq ingest jc --parser <name> [--input <path|->]`
- `--parser` は必須。`--input` の既定値は `-`（stdin）
- 成功時の出力キー: `source`, `parser`, `result_type`, `record_count`, `records`
- `jc` 不在・`--parser` 不正・入力不正は終了コード `3`
- `--emit-pipeline` の `steps`: `ingest_jc_parse`

### 2.5 `ingest notes`

`nb` ノートを `nb -> jq` の固定2段で正規化し、決定的順序で出力します。

- コマンド: `dataq ingest notes [--tag <tag>...] [--since <rfc3339>] [--until <rfc3339>] [--to <json|jsonl>]`
- `--tag` は複数指定可（空文字は禁止）
- `--since` / `--until` は境界を含む時刻フィルタ
- 出力は `--to json` で JSON 配列、`--to jsonl` で JSONL
- 正規化行は `created_at` -> `id` の順で安定ソートし、時刻は可能な限り UTC 正規化
- `nb` / `jq` 不在、時刻フィルタ不正、正規化失敗は終了コード `3`
- `--emit-pipeline` の `steps`: `ingest_notes_nb_export`, `ingest_notes_jq_normalize`

### 2.6 `ingest book`

mdBook ルートを解析し、`SUMMARY.md` と book メタデータを決定的JSONへ変換します。

- コマンド: `dataq ingest book --root <path> [--include-files]`
- 出力キー: `book`, `summary`
- `--include-files` を有効化すると章ファイルの `size_bytes` / `content_hash` などを追加
- `DATAQ_INGEST_BOOK_VERIFY_MDBOOK=1` の場合は `mdbook` による補助検証を有効化
- `SUMMARY.md` 不正、参照章ファイル欠損、`jq`/`mdbook` 不在は終了コード `3`
- `--emit-pipeline` の `steps`: `ingest_book_summary_parse`, `ingest_book_mdbook_meta`, `ingest_book_jq_project`

### 2.7 `schema infer`

表形式入力から `qsv` を使って JSON Schema を推定します。

- コマンド: `dataq schema infer [--input <path|->]`
- `--input` 省略時または `-` 指定時は stdin から読み込み
- 成功時は `qsv schema` の JSON を stdout へそのまま返す
- `qsv` 不在・入力ファイル不正・推定失敗は終了コード `3`
- `--emit-pipeline` の `steps`: `schema_infer_qsv`, `schema_infer_parse_json`

### 3. `sdiff`

変換前後または2データセット間の構造差分を返す。

- 件数差分
- カラム/キー差分
- 値差分（パス単位）
- パス表記は曖昧さ回避のため canonical 形式（例: `$["a.b"]`, `$[0]["quote\"key"]`）
- `--key <canonical-path>` でレコード対応付けキーを指定（例: `$["id"]`）
- `--ignore-path <canonical-path>` で比較除外パスを複数指定可能
- `--value-diff-cap <usize>` で `values.items` の最大件数を制御（既定: `100`）
- `--fail-on-diff` 指定時は `values.total > 0` で終了コード `2`（未指定時は比較成功で `0`）
- `--key` 利用時に重複キーがある場合は入力不正として終了コード `3`
- `--ignore-path` 指定時、レポートに `ignored_paths` が出力される
- `values.total` は実差分件数を維持し、上限超過時のみ `values.truncated=true`

### 4. `diff source`

異なる入力ソース（file または preset）を解決してから、`sdiff` と同じ差分レポートを返す。

- `--left <preset-or-path>` / `--right <preset-or-path>`
  - file: `path/to/input.json`
  - preset: `preset:<github-actions-jobs|gitlab-ci-jobs>:<path>`
- 出力は `sdiff` と同じ `counts` / `keys` / `ignored_paths` / `values` に加えて、`sources`（左右の解決メタデータ）を含む
- `--fail-on-diff` 指定時は `values.total > 0` で終了コード `2`
- `--emit-pipeline` の `steps`: `diff_source_resolve_left`, `diff_source_resolve_right`, `diff_source_compare`

### 5. `profile`

データ品質の概要を決定的な JSON で返す。

- `record_count`: レコード件数
- `field_count`: フィールドパス件数
- `returned_field_count`: `--field` 指定時に返したフィールドパス件数
- `fields`: canonical path ごとの集計
  - `null_ratio`（0.0-1.0）
  - `unique_count`
  - `type_distribution`（`null|boolean|number|string|array|object`）
  - `numeric_stats`（数値サンプルが1件以上ある場合のみ）
    - `count`, `min`, `max`, `mean`, `p50`, `p95`
- `missing_fields`: `--allow-missing-fields` 指定時の欠損 projection パス（canonical path、ソート/重複排除済み）
- `--field <name-or-path>` は出力 `fields` を projection する。直接名は `$["name"]` に解決し、`$` で始まる値は canonical path として検証する
- projection 時も `field_count` は入力全体のフィールドパス件数を維持し、`fields` は canonical path 順で返す
- 欠損 projection は既定で exit `3` の `input_usage_error`。`--allow-missing-fields` 指定時は exit `0` で存在する field のみ返す
- `--brief` は LLM 向けの省コンテキスト出力を返す。トップレベルは `record_count`, `field_count`, `truncated`, `fields` で、projection metadata がある場合は `missing_fields` も維持する
- brief の `fields` は配列で、各要素は `path`, `null_ratio`, `unique_count`, `dominant_type`, `numeric` を持つ。`numeric` は通常出力の `numeric_stats` と同じ内容、数値統計がない場合は `null`
- `dominant_type` は `null|boolean|number|string|array|object`。非 null 型の最多カウントを採用し、同数なら `boolean`, `number`, `string`, `array`, `object` の順で優先する
- `--sort-fields` は brief の field 配列を制御する。`path` は canonical path 昇順、`unique_count` は降順、`null_ratio` は降順で、同値時はいずれも path 昇順
- `--max-fields <n>` は projection と sort の後に brief field を切り詰める。`0` も有効で、返した field 数が利用可能 field 数より少ない場合は `truncated: true`
- `--from csv` では通常CSV行の集計に加えて、qsv adapter の profile/stats CSV（`field`, `type`, `cardinality`, `nullcount`, `record_count` など）も受け取り、同一スキーマへ正規化
- qsv stats CSV のデータセット件数は、明示的な `record_count|records|rows|row_count|total_rows`、`Integer` 行で全セルが非空の `n_negative+n_zero+n_positive+nullcount`、または全列で揃った `type=NULL`, `cardinality=0`, `nullcount=0` の空データ証明から決定し、全フィールドで同じ件数を使う。`Float` の符号別カウンタは `NaN` などを数えないため件数根拠には使わない。空セルは利用不能、リテラル `0` は有効値として扱い、文字列列を 0 件とはみなさない
- qsv 数値行の `numeric_stats.count` は、その行の非空な `n_negative+n_zero+n_positive` をオーバーフロー検査付きで合算する。そのため `Float` 行の `NaN` はデータセット件数と `type_distribution.number` には残り得るが、数値統計の件数には加えない。いずれかの符号別カウンタが利用不能なら、その行の `numeric_stats` は省略する
- qsv の件数セルは非負の10進整数表記のみ受け付ける。小数、指数表記、負数、`usize` 範囲外の値は exit `3` の入力エラー
- qsv の `sparsity` は既定で丸められるため、件数や欠損件数の導出には使わない。正規化する各行には非空の `nullcount` または `null_count` が必要
- 正確な件数を決定できない qsv stats CSV（例: 非空の文字列列のみで null がない出力）、候補同士の矛盾、または `nullcount > record_count` は exit `3` の `input_usage_error` とする
- qsv adapter CSV 正規化パスが使われた場合、`--emit-pipeline` の `stage_diagnostics` に `profile_qsv_normalize` を出力し、`external_tools` に `qsv` を `used=true` で記録

`numeric_stats` の決定性ルール:

- 数値サンプルは JSON number 型のみを対象（null/文字列/真偽値などは対象外）
- `p50` / `p95` は nearest-rank 方式（`rank = ceil(p * n)`、`index = rank - 1`、0始まり配列で評価）
- `numeric_stats` の浮動小数は小数点以下6桁へ丸め（`round half away from zero` 相当）
- native profile では、6桁丸めの途中計算や単純加算が overflow する大きな有限 JSON number も、表現可能な `min` / `max` / `mean` / `p50` / `p95` を有限 JSON number のまま保持
- JSON number として表現不能な入力は `input_usage_error`（exit `3`）として拒否

### 6. `join`

2つの入力を結合キーで結合し、JSON配列で返す。

- `--left <path>`: 左入力（JSON/YAML/CSV/JSONL）
- `--right <path>`: 右入力（JSON/YAML/CSV/JSONL）
- `--on <field>`: 結合キー
- `--how <inner|left>`: 結合方式
- 入力レコードは object であること、および `--on` キーが全レコードに存在することが必須
- 出力は JSON 配列固定（決定的順序）
- 実行は `mlr` を明示的引数配列で呼び出し、`--emit-pipeline` 時に stage 診断（`input_records`, `output_records`, `input_bytes`, `output_bytes`, `duration_ms`(固定 `0`), `status`）を出力

### 7. `aggregate`

単一入力をグループ化して集計し、JSON配列で返す。

- `--input <path>`: 入力（JSON/YAML/CSV/JSONL）
- `--group-by <field>`: グループキー
- `--metric <count|sum|avg>`: 集計メトリクス
- `--target <field>`: 集計対象キー
- `--sort-by <group|metric>`: 出力行の並び順キー（既定: `group`）
- `--order <asc|desc>`: 並び順（既定: `asc`）
- `--limit <n>`: sort後に返す件数（任意、`0` は `[]`）
- `sum` / `avg` は `--target` が数値であることを要求
- 入力レコードは object であること、および `group-by`/`target` キーが全レコードに存在することが必須
- 出力は JSON 配列固定（メトリクス列は `count` / `sum` / `avg`）
- `sum` は `mlr` が正確な整数を返した場合、JSON の `i64` / `u64` 範囲で整数精度を保持する（`avg` と小数の `sum` は浮動小数）
- 構文上整数の `sum` が `i64` / `u64` 範囲外の場合、`f64` へ丸めず input/representation error（exit `3`）にする。小数・指数表記は有限な `f64` として扱う
- metric sort は整数メトリクスを `f64` に変換せず比較するため、`2^53` を超える整数でも正確に順位付けする
- `--sort-by metric --order desc --limit 10` で集計metric上位10件を1コマンドで返す。同点は group キー literal 昇順で決定する
- 実行は `mlr` を明示的引数配列で呼び出し、`--emit-pipeline` 時に stage 診断（`input_records`, `output_records`, `input_bytes`, `output_bytes`, `duration_ms`(固定 `0`), `status`）を出力

### `ingest tabular`

表形式入力（CSVなど）を `csvkit` (`in2csv -> csvjson`) で JSON 配列へ変換します。

- `--input <path|->`: 入力ファイルまたは stdin（`-`）
- stage1: `in2csv` で CSV へ正規化
- stage2: `csvjson --no-inference` で JSON 配列化
- 行オブジェクトはキー順を再帰的に固定し、同一入力で同一出力を保証
- `csvkit` 不在・変換失敗・不正入力は終了コード `3`
- `--emit-pipeline` ステップ:
  - `ingest_tabular_csvkit_in2csv`
  - `ingest_tabular_csvkit_csvjson`

### 8. `ingest doc`

ドキュメント入力（Markdown/HTML/DOCX/reStructuredText/LaTeX）を、固定スキーマ JSON へ抽出します。

- `--input <path|->`: 入力ファイルまたは stdin（`-`）
- `--from <md|html|docx|rst|latex>`: 入力フォーマット
- stage1: `pandoc -f <from> -t json` で AST 化
- stage2: `jq` で `meta`, `headings`, `links`, `tables`, `code_blocks` へ投影
- `pandoc` 不在・parse失敗・不正入力は終了コード `3`
- `--emit-pipeline` 時のステップは `ingest_doc_pandoc_ast`, `ingest_doc_jq_project`
  - `external_tools` は `pandoc` と `jq` を `used=true` で記録

### 9. `scan text`

決定的な順序でテキストを走査し、マッチを構造化JSONで返す。

- `dataq scan text --pattern <regex> [--path <dir>] [--glob <glob>...] [--max-matches <n>]`
- `--policy-mode` を有効にすると、1件以上ヒット時に終了コード `2`
- `--jq-project` で任意の jq 投影ステージ（`scan_text_jq_project`）を有効化
- 出力は `matches`（path/line/column順）と `summary`
- `rg` が未インストール、または regex 不正時は終了コード `3`
- `--emit-pipeline` ステップ:
  - `scan_text_rg_execute`
  - `scan_text_parse`
  - `scan_text_jq_project`

### 10. `transform rowset`

固定2段 (`jq -> mlr`) で rowset を変換し、JSON配列で返す（CLI 互換のため `--engine sqlite` を受け付け）。

- `--input <path|->`: 入力（`-` は stdin）
- `--engine <sqlite>`: stage2 engine セレクタ（既定値 `sqlite`）
- `--jq-filter <filter>`: stage1 の jq filter
- `--mlr <verb...>`: stage2 (`mlr`) アダプタ引数列
- 出力は JSON 配列固定
- `jq`/`mlr` 実行や filter/args 不正は終了コード `3`
- `--emit-pipeline` では `transform_rowset_jq`, `transform_rowset_mlr` を stage 診断として出力（stage2 `tool` は `mlr`）

### 11. `transform sql`

`duckdb` に入力 rowset をロードして SQL を実行し、JSON 配列で返す。

- `--input <path|->`: 入力（`-` は stdin）
- `--query <sql>`: 実行する SQL（決定性が必要な場合は `ORDER BY` を明示）
- `--engine <duckdb>`: SQL 実行エンジン（現状は `duckdb` 固定）
- 出力は JSON 配列固定。DuckDB が返した行順を保持し、各行のオブジェクトキーは再帰的に canonical 順へ揃える
- 数値の float 表現は JSON 出力時に正規化する
- `duckdb` 不在・`--query` 不正・SQL実行失敗・入力不正は終了コード `3`
- 終了コード:
  - `0`: SQL 実行成功
  - `3`: 入力/使用エラー（上記）
  - `1`: 予期しない内部エラー
  - `2`: 本コマンドでは未使用（検証系コマンドで利用）

### 12. `merge`

複数の JSON/YAML 入力をポリシー指定で決定的にマージ。

- `--base <path>` と `--overlay <path>`（複数指定可）を順に適用
- `--policy last-wins`: 同一キーは overlay 側で上書き（shallow）
- `--policy deep-merge`: object は再帰マージ、配列は要素インデックス単位で再帰マージ
- `--policy array-replace`: object は再帰マージ、配列は overlay 側で全置換
- `--policy-path <canonical-path=policy>`（複数指定可）で subtree ごとのポリシーを上書き
  - 例: `--policy-path '$["spec"]["containers"]=array-replace'`
  - 解決順: 最長一致する `--policy-path` を優先し、同一深さの一致は後ろに指定した定義を優先。一致なしは `--policy` を適用
- 出力は JSON 固定（キー順は決定的にソート）

### 13. `doctor`

実行環境の依存を診断。`--capabilities` と `--profile` に対応。

- 出力は JSON 固定（stdout）
- 各ツールの診断項目: `name`, `found`, `version`, `executable`, `message`
- `--capabilities` 指定時:
  - `capabilities`（固定順）を追加: `jq.null_input_eval`, `yq.null_input_eval`, `mlr.help_command`
  - 項目: `name`, `tool`, `available`, `message`
- `--profile <core|ci-jobs|doc|api|notes|book|scan>` 指定時:
  - `capabilities`（固定順の `*.available` probe）を追加
  - `profile`（`version`, `name`, `description`, `satisfied`, `requirements`）を追加
  - `version` は `dataq.doctor.profile.requirements.v1` で固定
- 終了コード:
  - `0`: `--profile` 未指定時は `jq|yq|mlr` が全て起動可能、`--profile` 指定時は選択 profile 要件を充足
  - `3`: `--profile` 未指定時は `jq|yq|mlr` のいずれかが欠如または起動不可、`--profile` 指定時は選択 profile 要件未達
  - `1`: 予期しない内部エラー
- `--emit-pipeline` 指定時の stderr ステップ:
  - `--profile` 未指定: `doctor_probe_tools`, `doctor_probe_capabilities`
  - `--profile` 指定: `doctor_profile_probe`, `doctor_profile_evaluate`

### 14. `recipe run`

レシピファイル（YAML/JSON）を読み込み、`steps` を定義順で実行します。

- 実行コマンド: `dataq recipe run --file <path>`
- レシピスキーマ（MVP）:
  - `version`: `dataq.recipe.v1`
  - `steps[*].kind`: `canon | assert | profile | sdiff`
  - `steps[*].args`: 各 step の引数オブジェクト
- step 間データは in-memory で受け渡し
- stdout は実行サマリ JSON（`matched`, `exit_code`, `steps`）を返す
- `--emit-pipeline` 有効時は recipe 全体と step 実行トレースを stderr JSON へ出力

例:

```yaml
version: dataq.recipe.v1
steps:
  - kind: canon
    args:
      input: ./input.json
      from: json
  - kind: assert
    args:
      rules:
        required_keys: [id]
        fields:
          id:
            type: integer
```

### 15. `recipe lock`

レシピファイル（YAML/JSON）から、再現実行のためのロック情報を生成します。

- 実行コマンド: `dataq recipe lock --file <path> [--out <lock-path>]`
- 出力:
  - `--out` なし: stdout に lock JSON
  - `--out` あり: lock JSON を指定ファイルへ書き出し（stdout は空）
- lock JSON:
  - `version`: `dataq.recipe.lock.v1`
  - `command_graph_hash`
  - `args_hash`
  - `tool_versions`（使用ツールのみ。キーはツール名の辞書順: `jq`/`mlr`/`yq`）
  - `dataq_version`
- 異常時契約:
  - レシピ不正 / step引数不正 / ツール解決失敗は exit `3`
- `--emit-pipeline` 有効時は `recipe_lock_parse`, `recipe_lock_probe_tools`, `recipe_lock_fingerprint` を stderr JSON へ出力

### 16. `recipe replay`

lock ファイルを検証したうえで `recipe run` と同じレシピ実行を行います。

- 実行コマンド: `dataq recipe replay --file <recipe-path> --lock <lock-path> [--strict]`
- lock 制約は固定順で検証:
  - `lock.version`
  - `lock.command_graph_hash`
  - `lock.args_hash`
  - `lock.dataq_version`
  - `lock.tool_versions.<tool>`
- stdout は実行サマリ JSON（`matched`, `exit_code`, `lock_check`, `steps`）を返す
- `--strict` 指定時:
  - lock mismatch は exit `2`（validation mismatch、実行はスキップ）
- 非 strict 時:
  - lock mismatch を `lock_check.mismatches` に報告しつつ実行継続
  - 実行された step の検証不一致は従来どおり exit `2`
- `--emit-pipeline` 有効時は `recipe_replay_parse`, `recipe_replay_verify_lock`, `recipe_replay_execute` を stderr JSON へ出力

### 17. `contract`

サブコマンドの出力契約を機械可読JSONで取得します（read-only）。

- `dataq contract --command <canon|ingest-api|ingest|ingest-jc|ingest-tabular|assert|gate-schema|gate|schema-infer|sdiff|diff-source|profile|ingest-doc|ingest-notes|ingest-book|join|aggregate|scan|transform-rowset|transform-sql|merge|doctor|recipe-run|recipe-lock|recipe-replay|emit-plan>`
  - 単一コマンドの契約を1オブジェクトで返す
  - `recipe` は `recipe run` の契約（`matched`, `exit_code`, `steps`）を返す
- `dataq contract --all`
  - 全コマンド契約を固定順配列で返す
- 順序: `canon`, `ingest-api`, `ingest yaml-jobs`, `ingest-jc`, `ingest-tabular`, `assert`, `gate-schema`, `gate`, `schema-infer`, `sdiff`, `diff-source`, `profile`, `ingest.doc`, `ingest.notes`, `ingest-book`, `join`, `aggregate`, `scan`, `transform-rowset`, `transform-sql`, `merge`, `doctor`, `recipe-run`, `recipe-lock`, `recipe-replay`, `emit-plan`
- 各契約オブジェクトのキー:
  - `command`, `schema`, `output_fields`, `exit_codes`, `notes`
  - `assert` の `notes` には `--schema` 経路の既定エンジン（`jsonschema`）と任意エンジン（`ajv`/`checkjs`）を含む

### 18. `emit plan`

サブコマンドの静的実行計画を、実行せずに機械可読JSONで取得します（read-only）。

- 実行コマンド:
  - `dataq emit plan --command <subcommand> [--args <json-array>]`
- 出力キー:
  - `command`: 対象サブコマンド
  - `args`: 解決に使った引数配列
  - `stages`: `order`, `step`, `tool`, `depends_on` を含む段情報
  - `tools`: `jq|yq|mlr|ajv|duckdb|check-jsonschema` の期待利用有無（`expected`）
- `--args` は JSON 文字列で渡す（例: `'["--normalize","github-actions-jobs"]'`）
- `assert --schema` の既定 `stages` は `validate_assert_schema_with_jsonschema`
  - `--engine=ajv` で `validate_assert_schema_with_ajv`
  - `--engine=checkjs` で `validate_assert_schema_with_check_jsonschema`
- `assert` 向け `--args` では runtime と同様に、`--engine/--schema-engine` は `--schema` と併用時のみ有効
- 終了コード:
  - `0`: 計画生成成功
  - `3`: 未対応サブコマンドまたは `--args` 形式不正
  - `1`: 予期しない内部エラー
- `emit plan` と `--emit-pipeline` の違い:
  - `emit plan`: 実行前の静的計画（外部ツール実行なし）
  - `--emit-pipeline`: 実行時に観測した診断（stderr）

### 19. `mcp`

MCP (Model Context Protocol) の単発JSON-RPC 2.0 リクエストを処理します。

- 実行コマンド: `dataq mcp`
- 入出力:
  - stdin: JSON-RPC 2.0 リクエスト1件
  - stdout: JSON-RPC 2.0 レスポンス1件
- 対応メソッド:
  - `initialize`
  - `tools/list`
  - `tools/call`
- `tools/list` のツール順序は固定:
  - `dataq.canon`
  - `dataq.ingest.api`
  - `dataq.ingest.yaml_jobs`
  - `dataq.assert`
  - `dataq.gate.schema`
  - `dataq.gate.policy`
  - `dataq.sdiff`
  - `dataq.diff.source`
  - `dataq.profile`
    - `field`: string または string[]。CLI の `--field` と同じ projection 指定
    - `allow_missing_fields`: boolean。CLI の `--allow-missing-fields` と同じ欠損許可
    - `brief`: boolean。CLI の `--brief` と同じ省コンテキスト出力
    - `max_fields`: integer >= 0。CLI の `--max-fields` と同じ brief field 上限
    - `sort_fields`: `path` / `unique_count` / `null_ratio`。CLI の `--sort-fields` と同じ brief field 順序
  - `dataq.ingest.doc`
  - `dataq.ingest.notes`
  - `dataq.ingest.book`
  - `dataq.join`
  - `dataq.aggregate`
    - `sort_by`: `group` / `metric`。CLI の `--sort-by` と同じ出力行 sort キー
    - `order`: `asc` / `desc`。CLI の `--order` と同じ sort 方向
    - `limit`: integer >= 0。CLI の `--limit` と同じ sort 後の件数上限
  - `dataq.scan.text`
  - `dataq.transform.rowset`
  - `dataq.transform.sql`
  - `dataq.merge`
  - `dataq.doctor`
  - `dataq.contract`
  - `dataq.emit.plan`
  - `dataq.recipe.run`
  - `dataq.recipe.lock`
  - `dataq.recipe.replay`
- `tools/list` の各 tool 定義:
  - `inputSchema.additionalProperties = false`（デフォルト）
  - canonical 引数名のみ `properties` に掲載し、`required` / `enum` / `oneOf` を明示
  - `examples` に実行例（canonical 引数）
  - `meta.exit_code_contract` に `0|2|3|1` の契約メタデータ
  - `dataq.ingest.api` の `method` は `GET|POST|PUT|PATCH|DELETE` を大文字小文字非依存で受理
- `tools/call` レスポンス:
  - `structuredContent.exit_code`
  - `structuredContent.payload`
  - `structuredContent.pipeline`（`emit_pipeline=true` のときのみ）
  - `structuredContent.meta.warnings`（alias 引数使用時の非推奨警告）
  - `isError = (exit_code != 0)`
  - `content[0].text` には `structuredContent` と等価なJSON文字列を格納
  - 未知引数は `input_usage_error`（exit `3`）として拒否される
- alias 引数:
  - 既存 alias は互換性維持のため受理
  - `tools/call` の `structuredContent.meta.warnings` に
    - `code = "deprecated_arg_alias"`
    - `alias`
    - `canonical`
    - `message`
    を返す
- `input_usage_error` payload:
  - `error`, `message` に加えて `invalid_params` を返す
  - `invalid_params[*]` は `name`, `reason` を持つ機械可読エントリ
- JSON-RPCエラーコード:
  - `-32700` parse error
  - `-32600` invalid request
  - `-32601` method not found
  - `-32602` invalid params
  - `-32603` internal error
- `mcp` モードのプロセス終了コード:
  - レスポンスを書き出せた場合は、ツール実行結果に関係なく `0`
  - stdout の `BrokenPipe` は consumer-closed の正常終了として `0`
  - `BrokenPipe` 以外の stdout write/flush 失敗は `1`、stdin 読み取り失敗は `3`

### 20. `codex install-skill`

Codex で再利用できる dataq skill を、CLIに埋め込まれた固定資産からインストールします。

- 実行コマンド: `dataq codex install-skill [--dest <dir>] [--force]`
- 配置先ルート解決:
  - `--dest <dir>` 指定時: `<dir>`
  - 未指定時: `CODEX_HOME/skills`
  - `CODEX_HOME` 未設定時: `HOME/.agents/skills`
- 最終配置先: `<root>/dataq`
- コピー対象（固定）:
  - `SKILL.md`
  - `agents/openai.yaml`
- 成功時は stdout JSON:
  - `schema`: `dataq.codex.install_skill.output.v1`
  - `skill_name`: `dataq`
  - `destination`: 配置先ディレクトリ
  - `copied_files`: 相対パス配列（固定順）
  - `overwrite`: `--force` 指定有無
- 既存ディレクトリがある場合:
  - `--force` なし: 終了コード `3`
  - `--force` あり: 上書き再配置
- `--emit-pipeline`:
  - `steps`: `resolve_codex_skill_root`, `prepare_codex_skill_destination`, `write_embedded_codex_skill_files`, `emit_codex_install_skill_output`
  - `deterministic_guards`: `rust_native_fs_execution`, `compile_time_embedded_skill_assets`, `fixed_embedded_asset_write_order`

## 設計ドキュメント

設計に関する詳細は `docs/` 配下を参照してください。

- [設計方針・実装アーキテクチャ](docs/architecture.md)
- [CLI仕様・I/O契約](docs/command-spec.md)
- [assert ルールスキーマ](docs/rules-schema.md)

## 想定ユースケース

- CIでのデータ品質ゲート
- ETL前後の差分検証
- エージェント実行前の入力正規化
- 手元での再現可能なデータ調査
