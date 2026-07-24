# Scoop bucket 配布

`dataq` を Windows の Scoop で導入できるようにするための、bucket 連携手順です。

## 1. bucket リポジトリを用意

GitHub の bucket リポジトリを用意します（既定: `${GITHUB_REPOSITORY_OWNER}/stray-tools`）。
workflow はそのリポジトリの `bucket/dataq.json` を更新します。

## 2. このリポジトリの Actions 設定

`Settings > Secrets and variables > Actions` で次を設定します。

- Secret: `SCOOP_BUCKET_TOKEN`
  - bucket リポジトリへ push できる GitHub token
  - 必要権限: `Contents: Read and write`
- Variable（任意）: `SCOOP_BUCKET_REPO`
  - 例: `koizumikento/stray-tools`
  - 未設定時は `HOMEBREW_TAP_REPO`、さらに未設定なら `${GITHUB_REPOSITORY_OWNER}/stray-tools` を使います

移行互換性のため、`SCOOP_BUCKET_TOKEN` が未設定なら既存の `HOMEBREW_TAP_TOKEN` を使います。
専用の Scoop 設定を追加しなくても、現在の Homebrew tap 用設定で公開を開始できます。

## 3. 自動反映 workflow

`.github/workflows/publish-scoop-bucket.yml` は次のタイミングで `bucket/dataq.json` を更新します。

- `Release` workflow 成功時（`workflow_run`）
- GitHub Release 公開時（`release.published`）
- タグを指定した手動実行（`workflow_dispatch`）

workflow は Release 添付の `x86_64-pc-windows-msvc` 向け SHA256 を厳密に検証し、
`scripts/generate-scoop-manifest.sh` で決定的な manifest を生成して bucket リポジトリへ push します。
Homebrew workflow と共通の concurrency group を使うため、同じ配布リポジトリへの push は直列化されます。

## 4. ユーザー向け install

PowerShell で次を実行します。

```powershell
scoop bucket add stray-tools https://github.com/koizumikento/stray-tools
scoop install stray-tools/dataq
```

Scoop manifest が導入するのは Windows x86_64 用の `dataq.exe` だけです。
Homebrew formula と異なり、`jq`、`yq`、`mlr` などの連携ツールは自動では導入しません。
必要な連携ツールは、利用する `dataq` コマンドに応じて別途インストールしてください。
