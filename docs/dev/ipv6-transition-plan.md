# Execution Plan: Refactoring hack/create-kind-cluster.sh

The core idea is to remove the KIND_IP_FAMILY environment variable logic entirely, eliminating all IPv4 and Dual-Stack conditionals to streamline the file as a strictly IPv6-only bootstrapper.

## Step 1: Simplify Subnet Configuration
Remove the `case "${KIND_IP_FAMILY}" in` block (around lines 24–57). Hardcode the exact IPv6 subnets required:
```bash
# We use an IPv6-only cluster to exercise single-family code paths.
pod_subnet="fd00:10:244::/56"
service_subnet="fd00:10:96::/112"
```

## Step 2: Hardcode ipFamily
Update the output generation of `bin/kind-config.yaml` to ensure it unconditionally formats `ipFamily: ipv6`.

## Step 3: Enforce IPv6 Network Validation
Remove the conditional `if [ "${KIND_IP_FAMILY}" != "ipv4" ]` wrappers around your Docker network validation, enforcing the check for a backend IPv6 `kind` network on *every* run.

## Step 4: Unconditional Node Parameters
At the bottom of the script, strip the `if` checks wrapping the `sysctl` commands so that `forwarding=1` and `proxy_ndp=1` persist unconditionally for all nodes in the cluster.
