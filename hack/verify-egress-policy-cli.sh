#!/usr/bin/env bash

# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Smoke-tests `kubectl ate get egress-policy` and `kubectl ate create
# egress-policy` against a live cluster, using nothing but the CLI: the
# no-policy note, create from a manifest without metadata, table and YAML
# output, a `get -o yaml | create -f -` round-trip into a second actor, the
# client- and server-side rejections, and the policy going away with its
# actor. No actor is resumed, so the egress gateway and router are not
# involved and their state does not matter.
#
# Prerequisites: a cluster with the egress demo installed
# (hack/install-ate-kind.sh --deploy-demo-egress) and kubectl-ate on PATH.
# The actors it creates are timestamped and deleted on exit, so it is safe to
# rerun next to existing actors in the atespace.
#
# Respects KUBECTL_CONTEXT and ATESPACE like the other hack scripts; KATE
# points at another kubectl-ate binary.

set -o errexit -o nounset -o pipefail

CTX="${KUBECTL_CONTEXT:-kind-kind}"
ATESPACE="${ATESPACE:-ate-demo-egress}"
TEMPLATE="${TEMPLATE:-egress}"
KATE="${KATE:-kubectl-ate}"
SUFFIX="$(date +%s)"
SRC="epcheck-src-${SUFFIX}"
DST="epcheck-dst-${SUFFIX}"
TMP="$(mktemp -d /tmp/egress-policy-cli.XXXXXX)"
FAILED=0

kate() { "${KATE}" --context "${CTX}" "$@"; }
log()  { printf '\n\033[1;36m== %s\033[0m\n' "$*"; }
pass() { printf '\033[1;32mPASS\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31mFAIL\033[0m %s\n' "$*"; FAILED=1; }
# run <cmd...>: captures stdout in OUT, stderr in ERR, exit status in RC.
run() {
  set +o errexit
  OUT="$("$@" 2>"${TMP}/err")"; RC=$?
  set -o errexit
  ERR="$(<"${TMP}/err")"
}
# check <description> <command...>: passes when the command succeeds.
check() { local desc="$1"; shift; if "$@"; then pass "${desc}"; else fail "${desc}"; fi; }
# must <cmd...>: setup step; aborts the run with the command's stderr on failure.
must() { run "$@"; [[ "${RC}" -eq 0 ]] || { printf '%s\n' "${ERR}" >&2; exit 1; }; }

cleanup() {
  log "cleanup"
  for a in "${SRC}" "${DST}"; do kate delete actor "${a}" -a "${ATESPACE}" >/dev/null 2>&1 || true; done
  rm -rf "${TMP}"
}
trap cleanup EXIT

log "create actor ${ATESPACE}/${SRC} (never resumed)"
must kate create actor "${SRC}" -a "${ATESPACE}" --template "${TEMPLATE}"

log "get on an actor without a policy"
run kate get egress-policy "${SRC}" -a "${ATESPACE}"
check "exits 0" test "${RC}" -eq 0
check "prints nothing on stdout" test -z "${OUT}"
check "notes the deny-all state on stderr" grep -q "has no egress policy" <<<"${ERR}"

log "create from a manifest without metadata"
printf 'rules:\n- hostnames: {patterns: ["api.example.com"]}\n- cidrs: {cidrs: ["10.64.0.0/16"]}\n' >"${TMP}/policy.yaml"
run kate create egress-policy "${SRC}" -a "${ATESPACE}" -f "${TMP}/policy.yaml"
check "exits 0" test "${RC}" -eq 0
check "prints the created policy as a table row" grep -Eq "^${ATESPACE} +${SRC} +2 +1 " <<<"${OUT}"

log "get table and yaml"
run kate get egress-policy "${SRC}" -a "${ATESPACE}"
check "table row shows 2 rules at version 1" grep -Eq "^${ATESPACE} +${SRC} +2 +1 " <<<"${OUT}"
run kate get egress-policy "${SRC}" -a "${ATESPACE}" -o yaml
check "yaml names the policy default in ${ATESPACE}" grep -Eq "^  (atespace: ${ATESPACE}|name: default)$" <<<"${OUT}"
check "yaml carries the server-filled uid" grep -Eq "^  uid: .+" <<<"${OUT}"
check "yaml carries both rules" grep -q "api.example.com" <<<"${OUT}"
printf '%s\n' "${OUT}" >"${TMP}/src.yaml"

log "round-trip: get -o yaml | create -f - into ${DST}"
must kate create actor "${DST}" -a "${ATESPACE}" --template "${TEMPLATE}"
run kate create egress-policy "${DST}" -a "${ATESPACE}" -f - <"${TMP}/src.yaml"
check "create accepts get's output as is" test "${RC}" -eq 0
run kate get egress-policy "${DST}" -a "${ATESPACE}" -o yaml
check "rules survive the round-trip unchanged" \
  diff <(sed -n '/^rules:/,$p' "${TMP}/src.yaml") <(sed -n '/^rules:/,$p' <<<"${OUT}")

log "rejections"
run kate create egress-policy "${SRC}" -a "${ATESPACE}" -f "${TMP}/policy.yaml"
check "second create fails with AlreadyExists" test "${RC}" -ne 0
check "  ...and says so" grep -q "AlreadyExists" <<<"${ERR}"
printf 'metadata: {atespace: not-%s}\nrules:\n- all: {}\n' "${ATESPACE}" >"${TMP}/mismatch.yaml"
run kate create egress-policy "${SRC}" -a "${ATESPACE}" -f "${TMP}/mismatch.yaml"
check "metadata.atespace mismatch is rejected before any RPC" test "${RC}" -ne 0
check "  ...naming both atespaces" grep -q "does not match --atespace" <<<"${ERR}"
run kate create egress-policy "${SRC}-missing" -a "${ATESPACE}" -f "${TMP}/policy.yaml"
check "create for a missing actor fails" test "${RC}" -ne 0

log "delete the actors; their policies go with them"
must kate delete actor "${SRC}" -a "${ATESPACE}"
must kate delete actor "${DST}" -a "${ATESPACE}"
run kate get egress-policy "${SRC}" -a "${ATESPACE}"
check "get exits 0 with nothing on stdout" test "${RC}" -eq 0
check "  ...and empty stdout" test -z "${OUT}"
check "  ...and the no-policy note on stderr" grep -q "has no egress policy" <<<"${ERR}"

echo
if [[ "${FAILED}" == "0" ]]; then
  printf '\033[1;32mALL CHECKS PASSED\033[0m — get and create egress-policy work against %s.\n' "${CTX}"
else
  printf '\033[1;31mSOME CHECKS FAILED\033[0m\n'; exit 1
fi
