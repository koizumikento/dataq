#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
generator="${script_dir}/generate-scoop-manifest.sh"
test_dir="$(mktemp -d)"
trap 'rm -rf "${test_dir}"' EXIT

sha="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
output="${test_dir}/dataq.json"

"${generator}" \
  --tag v1.2.3 \
  --repo koizumikento/dataq \
  --windows-sha "${sha}" \
  --output "${output}"

cat > "${test_dir}/expected.json" <<'EOF'
{
  "version": "1.2.3",
  "description": "Rust-native CLI for deterministic data preprocessing",
  "homepage": "https://github.com/koizumikento/dataq",
  "license": "MIT",
  "architecture": {
    "64bit": {
      "url": "https://github.com/koizumikento/dataq/releases/download/v1.2.3/dataq-v1.2.3-x86_64-pc-windows-msvc.zip",
      "hash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    }
  },
  "bin": "dataq.exe",
  "checkver": {
    "github": "https://github.com/koizumikento/dataq"
  },
  "autoupdate": {
    "architecture": {
      "64bit": {
        "url": "https://github.com/koizumikento/dataq/releases/download/v$version/dataq-v$version-x86_64-pc-windows-msvc.zip"
      }
    }
  }
}
EOF

cmp "${test_dir}/expected.json" "${output}"
grep -F 'v$version/dataq-v$version-' "${output}" >/dev/null

"${generator}" \
  --tag v1.2.3 \
  --repo koizumikento/dataq \
  --windows-sha "${sha}" \
  --output "${test_dir}/second.json"
cmp "${output}" "${test_dir}/second.json"

"${generator}" \
  --tag v2.0.0-rc.1 \
  --repo example-org/dataq \
  --windows-sha "${sha}" \
  --output "${test_dir}/prerelease.json"
grep -F '"version": "2.0.0-rc.1"' "${test_dir}/prerelease.json" >/dev/null

expect_usage_error() {
  local name="$1"
  shift
  local stderr_file="${test_dir}/${name}.stderr"

  set +e
  "${generator}" "$@" >"${test_dir}/${name}.stdout" 2>"${stderr_file}"
  local status=$?
  set -e

  if [[ "${status}" -ne 3 ]]; then
    echo "${name}: expected exit 3, got ${status}" >&2
    exit 1
  fi
  if [[ ! -s "${stderr_file}" ]]; then
    echo "${name}: expected actionable stderr" >&2
    exit 1
  fi
}

expect_usage_error missing_arguments
expect_usage_error missing_tag_value --tag
expect_usage_error missing_repo \
  --tag v1.2.3 --windows-sha "${sha}" --output "${test_dir}/invalid.json"
expect_usage_error missing_hash \
  --tag v1.2.3 --repo koizumikento/dataq --output "${test_dir}/invalid.json"
expect_usage_error missing_output \
  --tag v1.2.3 --repo koizumikento/dataq --windows-sha "${sha}"
expect_usage_error unknown_argument --unknown value
expect_usage_error invalid_tag \
  --tag 1.2.3 --repo koizumikento/dataq --windows-sha "${sha}" --output "${test_dir}/invalid.json"
expect_usage_error invalid_tag_shape \
  --tag v01.2.3 --repo koizumikento/dataq --windows-sha "${sha}" --output "${test_dir}/invalid.json"
expect_usage_error whitespace_tag \
  --tag 'v1.2.3 rc.1' --repo koizumikento/dataq --windows-sha "${sha}" --output "${test_dir}/invalid.json"
expect_usage_error slash_path_tag \
  --tag 'v1.2.3/../../payload' --repo koizumikento/dataq --windows-sha "${sha}" --output "${test_dir}/invalid.json"
expect_usage_error injection_shaped_tag \
  --tag 'v1.2.3$(echo injected)' --repo koizumikento/dataq --windows-sha "${sha}" --output "${test_dir}/invalid.json"
expect_usage_error invalid_repo \
  --tag v1.2.3 --repo 'koizumikento/dataq/extra' --windows-sha "${sha}" --output "${test_dir}/invalid.json"
expect_usage_error unsafe_repo \
  --tag v1.2.3 --repo 'koizumikento/dataq..json' --windows-sha "${sha}" --output "${test_dir}/invalid.json"
expect_usage_error invalid_hash \
  --tag v1.2.3 --repo koizumikento/dataq --windows-sha deadbeef --output "${test_dir}/invalid.json"
expect_usage_error uppercase_hash \
  --tag v1.2.3 --repo koizumikento/dataq \
  --windows-sha "0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF" \
  --output "${test_dir}/invalid.json"

echo "generate-scoop-manifest tests passed"
