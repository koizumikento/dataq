#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  scripts/generate-scoop-manifest.sh \
    --tag <release-tag> \
    --repo <owner/repo> \
    --windows-sha <sha256> \
    --output <path>

Example:
  scripts/generate-scoop-manifest.sh \
    --tag v0.1.0 \
    --repo koizumikento/dataq \
    --windows-sha <sha256> \
    --output bucket/dataq.json
EOF
}

usage_error() {
  echo "$1" >&2
  usage >&2
  exit 3
}

require_value() {
  if [[ $# -lt 2 || -z "$2" ]]; then
    usage_error "missing value for $1"
  fi
}

tag=""
repo=""
windows_sha=""
output=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --tag)
      require_value "$@"
      tag="$2"
      shift 2
      ;;
    --repo)
      require_value "$@"
      repo="$2"
      shift 2
      ;;
    --windows-sha)
      require_value "$@"
      windows_sha="$2"
      shift 2
      ;;
    --output)
      require_value "$@"
      output="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage_error "unknown argument: $1"
      ;;
  esac
done

if [[ -z "${tag}" || -z "${repo}" || -z "${windows_sha}" || -z "${output}" ]]; then
  usage_error "missing required arguments"
fi

semver_tag_regex='^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-([0-9A-Za-z]+([.-][0-9A-Za-z]+)*))?(\+[0-9A-Za-z]+([.-][0-9A-Za-z]+)*)?$'
if ! [[ "${tag}" =~ ${semver_tag_regex} ]]; then
  usage_error "invalid release tag (expected v<major>.<minor>.<patch> with optional prerelease/build metadata): ${tag}"
fi

prerelease="${BASH_REMATCH[5]:-}"
if [[ -n "${prerelease}" ]]; then
  IFS='.' read -r -a prerelease_identifiers <<<"${prerelease}"
  for identifier in "${prerelease_identifiers[@]}"; do
    if [[ "${identifier}" =~ ^[0-9]+$ && "${identifier}" != "0" && "${identifier}" == 0* ]]; then
      usage_error "invalid release tag (numeric prerelease identifiers must not contain leading zeroes): ${tag}"
    fi
  done
fi

if ! [[ "${repo}" =~ ^[A-Za-z0-9][A-Za-z0-9-]*/[A-Za-z0-9][A-Za-z0-9._-]*$ ]] ||
   [[ "${repo}" == *..* ]] ||
   [[ "${repo}" == */*. ]]; then
  usage_error "invalid repository (expected safe owner/repo): ${repo}"
fi

if ! [[ "${windows_sha}" =~ ^[0-9a-f]{64}$ ]]; then
  usage_error "invalid Windows SHA256 (expected 64 lowercase hexadecimal characters): ${windows_sha}"
fi

version="${tag#v}"
archive="dataq-${tag}-x86_64-pc-windows-msvc.zip"
mkdir -p "$(dirname "${output}")"

cat > "${output}" <<EOF
{
  "version": "${version}",
  "description": "Rust-native CLI for deterministic data preprocessing",
  "homepage": "https://github.com/${repo}",
  "license": "MIT",
  "architecture": {
    "64bit": {
      "url": "https://github.com/${repo}/releases/download/${tag}/${archive}",
      "hash": "${windows_sha}"
    }
  },
  "bin": "dataq.exe",
  "checkver": {
    "github": "https://github.com/${repo}"
  },
  "autoupdate": {
    "architecture": {
      "64bit": {
        "url": "https://github.com/${repo}/releases/download/v\$version/dataq-v\$version-x86_64-pc-windows-msvc.zip"
      }
    }
  }
}
EOF
