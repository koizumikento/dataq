# Homebrew tap 配布

`dataq` を `brew install` で導入できるようにするための、tap 連携手順です。

## 1. tap リポジトリを用意

GitHub の tap リポジトリを用意します（例: `stray-tools`）。

- 例: `https://github.com/koizumikento/stray-tools`
- `homebrew-` 接頭辞のないリポジトリを使う場合、`brew tap` は URL 指定が必要です

このリポジトリに `Formula/` ディレクトリを作っておくと運用が分かりやすくなります。

## 2. このリポジトリの Actions 設定

`Settings > Secrets and variables > Actions` で次を設定します。

- Secret: `HOMEBREW_TAP_TOKEN`
  - tap リポジトリへ push できる GitHub token
  - 必要権限: `Contents: Read and write`
- Variable（任意）: `HOMEBREW_TAP_REPO`
  - 例: `koizumikento/stray-tools`
  - 未設定時は `${GITHUB_REPOSITORY_OWNER}/stray-tools` を使います

## 3. 自動反映 workflow

`.github/workflows/publish-homebrew-tap.yml` は次のタイミングで `Formula/dataq.rb` を更新します。

- Release 公開時（`release.published`）
- 手動実行（`workflow_dispatch`）

workflow は Release 添付の macOS 向け SHA256 を読み取り、`scripts/generate-homebrew-formula.sh` で formula を生成して tap リポジトリへ push します。

## 4. ユーザー向け install

```bash
brew tap koizumikento/stray-tools https://github.com/koizumikento/stray-tools.git
brew install koizumikento/stray-tools/dataq
```

この tap は URL 指定で登録した前提のため、`brew install dataq` より fully-qualified 名を推奨します。
