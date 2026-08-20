#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# A dns_lookup_family pinned to one address family makes egress resolution fail
# closed on a cluster of the other family: the name resolves to nothing and the
# actor's connection fails before Envoy ever dials. The setting is spread over
# six sites in two manifests, so a seventh added without it, or one of the six
# reverted, is easy to miss. No Go test reads these manifests.
#
# AUTO is rejected alongside the explicit pins. Despite the name it is a legacy
# alias for "V6 preferred" and does not fall back: measured on the pinned Envoy
# builds, a name published with both an A and an unroutable AAAA makes exactly
# one connection attempt, to the AAAA, and 503s when it times out. ALL resolves
# both families and lets Happy Eyeballs pick the one that works.

set -o errexit -o nounset -o pipefail

ROOT="$(git rev-parse --show-toplevel)"
cd "${ROOT}"

# Each dns_cache_config block is bounded by the first line that dedents back to
# its own key, so a missing setting is reported against the block that lacks it
# rather than satisfied by a sibling that has one. Comments are stripped: prose
# about the family must not stand in for the setting.
read -r -d '' AWK_DNS_FAMILY <<'EOF' || true
function flush() {
  if (!open) {
    return
  }
  open = 0
  if (block !~ /dns_lookup_family:/) {
    printf "%s:%d: dns_cache_config does not set dns_lookup_family; egress resolution is left to Envoy's default\n", FILENAME, start
  }
}
match($0, /[^ ]/) && substr($0, RSTART) ~ /^dns_cache_config:[[:space:]]*$/ {
  flush()
  open = 1
  indent = RSTART - 1
  start = FNR
  block = ""
  next
}
open {
  if ($0 ~ /^[[:space:]]*$/) {
    next
  }
  if (match($0, /[^ ]/) - 1 <= indent) {
    flush()
    next
  }
  line = $0
  sub(/^[[:space:]]*#.*/, "", line)
  sub(/[[:space:]]#.*/, "", line)
  block = block line "\n"
}
# Judged across the whole file, not per block: a single-family pin is wrong
# wherever it appears, including in a cluster that declares no cache config.
/^[[:space:]]*dns_lookup_family:[[:space:]]*(V4_ONLY|V6_ONLY)[[:space:]]*$/ {
  printf "%s:%d: dns_lookup_family pins one address family; a cluster of the other family cannot resolve upstream names\n", FILENAME, FNR
}
/^[[:space:]]*dns_lookup_family:[[:space:]]*AUTO[[:space:]]*$/ {
  printf "%s:%d: dns_lookup_family AUTO prefers AAAA and never falls back to A; use ALL\n", FILENAME, FNR
}
END {
  flush()
}
EOF

# Every manifest is scanned, not a fixed list of the two that have egress
# clusters today: the sdsmint variant shows how a new one gets added, and a
# variant that arrived without the setting is the case worth catching.
shopt -s nullglob
manifests=(manifests/ate-install/*.yaml)

if [[ "${#manifests[@]}" -eq 0 ]]; then
  echo "no manifests under manifests/ate-install; this check needs updating" >&2
  exit 1
fi

rc=0
seen=0
for f in "${manifests[@]}"; do
  if grep -q '^[[:space:]]*dns_cache_config:[[:space:]]*$' "${f}"; then
    seen=1
  fi
  problems="$(awk "${AWK_DNS_FAMILY}" "${f}")"
  if [[ -n "${problems}" ]]; then
    echo "${problems}" >&2
    rc=1
  fi
done

if [[ "${seen}" -eq 0 ]]; then
  echo "no dns_cache_config under manifests/ate-install; this check needs updating" >&2
  rc=1
fi

exit "${rc}"
