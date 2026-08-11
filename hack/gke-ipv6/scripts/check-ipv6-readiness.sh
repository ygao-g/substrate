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

# Read-only readiness check for single-stack IPv6-only GKE (--stack-type=ipv6).
# Creates and deletes nothing. Exits non-zero if any check fails.

set -o errexit -o nounset -o pipefail

PROJECT="${PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
MIN_VERSION="${MIN_VERSION:-1.36.3-gke.1253000}"
REGIONS="${REGIONS:-us-central1 us-east1 us-west1 europe-west4}"

failures=0

pass() { printf '  PASS  %s\n' "$*"; }
fail() { printf '  FAIL  %s\n' "$*"; failures=$((failures + 1)); }
info() { printf '  ....  %s\n' "$*"; }

# Sorts $1 and $MIN_VERSION and checks $1 is not the lesser. GKE versions are
# dot/dash-separated numerics, so `sort -V` orders them correctly.
version_ge() {
  [ "$(printf '%s\n%s\n' "$MIN_VERSION" "$1" | sort -V | head -1)" = "$MIN_VERSION" ]
}

echo "project=${PROJECT}  min-version=${MIN_VERSION}"

echo
echo "== gcloud components =="
if gcloud components list --format="value(id,state.name)" 2>/dev/null |
  grep -qE '^beta[[:space:]]+(Installed|Update Available)'; then
  pass "beta component present (every cluster-create in the guide is 'gcloud beta')"
else
  fail "beta component missing -- run: gcloud components install beta"
fi

echo
echo "== gcloud --stack-type surface =="
# The enum wraps across lines in --help output, so flatten before matching.
beta_help="$(gcloud beta container clusters create --help 2>/dev/null |
  sed 's/\x1b\[[0-9;]*m//g' | tr '\n' ' ' | tr -s ' ' || true)"
if printf '%s' "$beta_help" | grep -qE 'STACK_TYPE must be one of:[^.]*[ ,]ipv6[ ,.]'; then
  pass "gcloud beta offers --stack-type=ipv6"
else
  fail "gcloud beta does not offer --stack-type=ipv6"
fi
if printf '%s' "$beta_help" | grep -q -- '--linked-runners-mode'; then
  pass "--linked-runners-mode available"
else
  info "--linked-runners-mode absent -- the guide's Linked Runners section is unrunnable (expected)"
fi

echo
echo "== GKE API discovery =="
# The IPV6 enum is unpublished even where gcloud accepts the flag, so its absence
# is not disqualifying -- but its arrival is the clearest public signal of a launch.
disc_enum="$(curl -fsS "https://container.googleapis.com/\$discovery/rest?version=v1beta1" |
  python3 -c 'import json,sys; print(",".join(json.load(sys.stdin)["schemas"]["IPAllocationPolicy"]["properties"]["stackType"].get("enum", [])))' 2>/dev/null || echo "unavailable")"
if printf '%s' ",${disc_enum}," | grep -q ',IPV6,'; then
  pass "v1beta1 stackType enum publishes IPV6: ${disc_enum}"
else
  info "v1beta1 stackType enum is [${disc_enum}] -- IPV6 unpublished, expected during preview"
fi

echo
echo "== version floor per region =="
region_ok=0
for region in ${REGIONS}; do
  # A region mid-rollout answers inconsistently for minutes at a time -- on 2026-08-11
  # europe-west4 returned 1.36.2 and 1.36.3 lists alternately. Re-run before trusting
  # a single region's FAIL.
  versions="$(gcloud container get-server-config --region="${region}" --project="${PROJECT}" \
    --format="value(validMasterVersions)" 2>/dev/null | tr ';' '\n' | tr -d '[] ' || true)"
  best="$(printf '%s\n' "${versions}" | grep -v '^$' | sort -V | tail -1)"
  if [ -n "${best}" ] && version_ge "${best}"; then
    channel="$(gcloud container get-server-config --region="${region}" --project="${PROJECT}" \
      --format="value(channels)" 2>/dev/null | tr ';' '\n' | grep -F "${MIN_VERSION}" |
      sed -n "s/.*'channel': '\([A-Z]*\)'.*/\1/p" | sort -u | paste -sd, - || true)"
    pass "${region}: ${best} (>= floor)${channel:+, ${MIN_VERSION} in ${channel}}"
    region_ok=1
  else
    info "${region}: newest is ${best:-unknown} -- below floor, do not use"
  fi
done
[ "${region_ok}" = 1 ] || fail "no region in [${REGIONS}] meets the ${MIN_VERSION} floor"

echo
echo "== project preconditions =="
enabled="$(gcloud services list --enabled --project="${PROJECT}" --format="value(config.name)" 2>/dev/null || true)"
for api in container.googleapis.com compute.googleapis.com dns.googleapis.com; do
  if printf '%s\n' "${enabled}" | grep -qx "${api}"; then
    pass "${api} enabled"
  else
    fail "${api} not enabled"
  fi
done

quota="$(gcloud compute project-info describe --project="${PROJECT}" --format="value(quotas)" 2>/dev/null |
  tr ';' '\n' | grep "'NETWORKS'" || true)"
net_usage="$(printf '%s' "${quota}" | sed -n "s/.*'usage': \([0-9]*\).*/\1/p")"
net_limit="$(printf '%s' "${quota}" | sed -n "s/.*'limit': \([0-9]*\).*/\1/p")"
if [ -n "${net_limit}" ] && [ "$((net_usage + 1))" -le "${net_limit}" ]; then
  pass "VPC network quota ${net_usage}/${net_limit}"
else
  fail "VPC network quota exhausted or unreadable (${net_usage:-?}/${net_limit:-?})"
fi

echo
if [ "${failures}" -eq 0 ]; then
  echo "READY -- but the only authoritative test is an actual create; the API rejects an"
  echo "un-allowlisted project with a bare INVALID_ARGUMENT and no field detail."
  exit 0
fi
echo "NOT READY -- ${failures} check(s) failed."
exit 1
