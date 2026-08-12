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
#
#   --create-probe   also probe the project's IPv6 feature gate (see below)

set -o errexit -o nounset -o pipefail

PROJECT="${PROJECT:-$(gcloud config get-value project 2>/dev/null)}"
MIN_VERSION="${MIN_VERSION:-1.36.3-gke.1253000}"
REGIONS="${REGIONS:-us-central1 us-east1 us-west1 europe-west4}"
# A region mid-rollout answers get-server-config inconsistently for minutes at a
# time -- on 2026-08-11 europe-west4 alternated between a 1.36.2 and a 1.36.3
# list. Ask more than once and report disagreement as disagreement.
RETRIES="${RETRIES:-3}"
PROBE_REGION="${PROBE_REGION:-us-east1}"
# The gate probe needs a network and subnet that resolve; they need not be IPv6.
PROBE_NETWORK="${PROBE_NETWORK:-default}"
PROBE_SUBNET="${PROBE_SUBNET:-default}"

create_probe=0
while [ $# -gt 0 ]; do
  case "$1" in
  --create-probe) create_probe=1 ;;
  *)
    echo "unknown argument: $1" >&2
    exit 2
    ;;
  esac
  shift
done

failures=0

pass() { printf '  PASS  %s\n' "$*"; }
fail() { printf '  FAIL  %s\n' "$*"; failures=$((failures + 1)); }
info() { printf '  ....  %s\n' "$*"; }

# Sorts $1 and $MIN_VERSION and checks $1 is not the lesser. GKE versions are
# dot/dash-separated numerics, so `sort -V` orders them correctly.
version_ge() {
  [ "$(printf '%s\n%s\n' "$MIN_VERSION" "$1" | sort -V | head -1)" = "$MIN_VERSION" ]
}

sdk_version="$(gcloud version --format="value(['Google Cloud SDK'])" 2>/dev/null || echo unknown)"

echo "project=${PROJECT}  min-version=${MIN_VERSION}  gcloud=${sdk_version}"

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
for flag in --private-endpoint-subnetwork --dataplane-optimization-mode; do
  if printf '%s' "$beta_help" | grep -q -- "${flag}="; then
    pass "${flag} available"
  else
    fail "${flag} absent -- guide Step 4 cannot be run verbatim"
  fi
done
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
echo "== version floor per region (${RETRIES} queries each) =="
region_ok=0
for region in ${REGIONS}; do
  answers=""
  for _ in $(seq "${RETRIES}"); do
    versions="$(gcloud container get-server-config --region="${region}" --project="${PROJECT}" \
      --format="value(validMasterVersions)" 2>/dev/null | tr ';' '\n' | tr -d '[] ' || true)"
    best="$(printf '%s\n' "${versions}" | grep -v '^$' | sort -V | tail -1)"
    answers="${answers}${best:-unknown}"$'\n'
  done
  distinct="$(printf '%s' "${answers}" | grep -v '^$' | sort -u | paste -sd' ' -)"
  # Highest answer wins the floor test: a rollout only ever adds versions, so the
  # low answer is the stale one.
  best="$(printf '%s' "${answers}" | grep -v '^$' | sort -V | tail -1)"

  if [ "$(printf '%s' "${distinct}" | wc -w)" -gt 1 ]; then
    info "${region}: FLAPPING across ${RETRIES} queries [${distinct}] -- mid-rollout, do not rely on it"
  fi

  if [ "${best}" != "unknown" ] && version_ge "${best}"; then
    channel="$(gcloud container get-server-config --region="${region}" --project="${PROJECT}" \
      --format="value(channels)" 2>/dev/null | tr ';' '\n' | grep -F "${MIN_VERSION}" |
      sed -n "s/.*'channel': '\([A-Z]*\)'.*/\1/p" | sort -u | paste -sd, - || true)"
    pass "${region}: ${best} (>= floor)${channel:+, ${MIN_VERSION} in ${channel}}"
    region_ok=1
  else
    info "${region}: newest is ${best} -- below floor, do not use"
  fi
done
[ "${region_ok}" = 1 ] || fail "no region in [${REGIONS}] meets the ${MIN_VERSION} floor"

echo
echo "== project preconditions =="
enabled="$(gcloud services list --enabled --project="${PROJECT}" --format="value(config.name)" 2>/dev/null || true)"
for api in container.googleapis.com compute.googleapis.com dns.googleapis.com \
  networkconnectivity.googleapis.com; do
  if printf '%s\n' "${enabled}" | grep -qx "${api}"; then
    pass "${api} enabled"
  else
    fail "${api} not enabled"
  fi
done

# A create needs headroom for one more VPC and its subnets. SUBNETWORKS is the
# likelier binding limit: the project carries ~90 subnets left over from an
# earlier attempt.
quotas="$(gcloud compute project-info describe --project="${PROJECT}" --format="value(quotas)" 2>/dev/null |
  tr ';' '\n' || true)"
for metric in NETWORKS SUBNETWORKS; do
  line="$(printf '%s\n' "${quotas}" | grep "'${metric}'" || true)"
  usage="$(printf '%s' "${line}" | sed -n "s/.*'usage': \([0-9]*\).*/\1/p")"
  limit="$(printf '%s' "${line}" | sed -n "s/.*'limit': \([0-9]*\).*/\1/p")"
  if [ -n "${limit}" ] && [ -n "${usage}" ] && [ "$((usage + 2))" -le "${limit}" ]; then
    pass "${metric} quota ${usage}/${limit}"
  else
    fail "${metric} quota exhausted or unreadable (${usage:-?}/${limit:-?})"
  fi
done

if [ "${create_probe}" = 1 ]; then
  echo
  echo "== IPv6 feature gate (create probe) =="
  # There is no --validate-only on cluster create, so the gate can only be read
  # from a real create call. Two properties make this one safe:
  #
  #   * it names an existing network and subnet, so a bare rejection cannot be
  #     blamed on an unresolvable field -- that was the flaw in the first version
  #     of this probe, which used a ghost subnet and got an identical 404 whether
  #     or not the gate was open;
  #   * it deliberately omits --private-endpoint-subnetwork, which the API
  #     requires for single-stack IPv6. So once the request is past the gate it
  #     is guaranteed to fail validation. The probe cannot create a cluster.
  #
  # Read the response:
  #   bare "Request contains an invalid argument.", no details  -> gated
  #   an IPv6-single-stack-specific complaint                   -> past the gate
  if ! gcloud compute networks subnets describe "${PROBE_SUBNET}" \
    --project="${PROJECT}" --region="${PROBE_REGION}" >/dev/null 2>&1; then
    fail "gate-unknown: probe subnet ${PROBE_SUBNET} not found in ${PROBE_REGION} -- set PROBE_NETWORK/PROBE_SUBNET"
  else
    probe_out="$(gcloud beta container clusters create "ipv6-gate-probe-$$" \
      --project="${PROJECT}" --region="${PROBE_REGION}" \
      --network="${PROBE_NETWORK}" --subnetwork="${PROBE_SUBNET}" \
      --enable-dataplane-v2 --cluster-dns=clouddns --quiet \
      --stack-type=ipv6 2>&1 || true)"

    if printf '%s' "${probe_out}" | grep -qiE 'ipv6 single stack|private_endpoint_subnetwork is required'; then
      pass "gate-open: the API validated this as an IPv6 single-stack request"
    elif printf '%s' "${probe_out}" | grep -q 'Request contains an invalid argument'; then
      fail "gate-closed: --stack-type=ipv6 rejected with a bare INVALID_ARGUMENT (project not allowlisted)"
    else
      fail "gate-unknown: unrecognised response -- read it by hand"
    fi
    printf '    %s\n' "$(printf '%s' "${probe_out}" | tr '\n' ' ' | tr -s ' ' | cut -c1-220)"
  fi
else
  echo
  info "IPv6 feature gate NOT probed -- pass --create-probe (the gate is the only real blocker)"
fi

echo
printf 'checked %s on %s (gcloud %s)\n' "${PROJECT}" "$(date -u +%Y-%m-%d)" "${sdk_version}"
if [ "${failures}" -eq 0 ]; then
  if [ "${create_probe}" = 1 ]; then
    echo "READY -- the gate probe reached field validation. Create for real in ${PROBE_REGION}"
    echo "with --release-channel=rapid (${MIN_VERSION} is RAPID-only)."
  else
    echo "READY on every local check -- but the gate itself is untested. Re-run with"
    echo "--create-probe; an un-allowlisted project is rejected with a bare INVALID_ARGUMENT."
  fi
  exit 0
fi
echo "NOT READY -- ${failures} check(s) failed."
exit 1
