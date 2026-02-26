#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/generate-homebrew-formula.sh \
    --tag <release-tag> \
    --repo <owner/repo> \
    --x86_64-macos-sha <sha256> \
    --arm64-macos-sha <sha256> \
    --output <path>

Example:
  scripts/generate-homebrew-formula.sh \
    --tag v0.1.0 \
    --repo koizumikento/dataq \
    --x86_64-macos-sha <sha256> \
    --arm64-macos-sha <sha256> \
    --output Formula/dataq.rb
EOF
}

tag=""
repo=""
x86_64_macos_sha=""
arm64_macos_sha=""
output=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag)
      tag="${2:-}"
      shift 2
      ;;
    --repo)
      repo="${2:-}"
      shift 2
      ;;
    --x86_64-macos-sha)
      x86_64_macos_sha="${2:-}"
      shift 2
      ;;
    --arm64-macos-sha)
      arm64_macos_sha="${2:-}"
      shift 2
      ;;
    --output)
      output="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 3
      ;;
  esac
done

if [[ -z "${tag}" || -z "${repo}" || -z "${x86_64_macos_sha}" || -z "${arm64_macos_sha}" || -z "${output}" ]]; then
  echo "missing required arguments" >&2
  usage >&2
  exit 3
fi

version="${tag#v}"
if [[ "${version}" == "${tag}" ]]; then
  echo "tag must start with 'v': ${tag}" >&2
  exit 3
fi

if ! [[ "${x86_64_macos_sha}" =~ ^[0-9a-f]{64}$ ]]; then
  echo "invalid x86_64 macOS SHA256: ${x86_64_macos_sha}" >&2
  exit 3
fi

if ! [[ "${arm64_macos_sha}" =~ ^[0-9a-f]{64}$ ]]; then
  echo "invalid arm64 macOS SHA256: ${arm64_macos_sha}" >&2
  exit 3
fi

mkdir -p "$(dirname "${output}")"

cat > "${output}" <<EOF
class Dataq < Formula
  desc "Rust-native CLI for deterministic data preprocessing"
  homepage "https://github.com/${repo}"
  license "MIT"
  version "${version}"

  on_macos do
    if Hardware::CPU.arm?
      url "https://github.com/${repo}/releases/download/${tag}/dataq-${tag}-aarch64-apple-darwin.tar.gz"
      sha256 "${arm64_macos_sha}"
    else
      url "https://github.com/${repo}/releases/download/${tag}/dataq-${tag}-x86_64-apple-darwin.tar.gz"
      sha256 "${x86_64_macos_sha}"
    end
  end

  def install
    bin.install "dataq"
    prefix.install "LICENSE"
    pkgshare.install "README.md"
  end

  test do
    output = shell_output("#{bin}/dataq --help")
    assert_match "dataq", output
  end
end
EOF
