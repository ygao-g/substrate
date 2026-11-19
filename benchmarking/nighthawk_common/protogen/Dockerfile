# Copyright 2026 The Agent Substrate Authors
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

# Builds nighthawk.desc for the nighthawk-ingress and nighthawk-egress runner
# images. ../docker-bake.hcl gives the result to both runner Dockerfiles as
# the "protogen" context.
#
# nighthawk_service speaks protobuf: spec.py builds the adaptive-load session
# spec and output.py parses the result, and those message trees pull in types
# from envoy, xds, protoc-gen-validate and googleapis. This compiles that
# whole closure into one FileDescriptorSet, which spec.py/output.py load at
# runtime.

# The three pins move together: NIGHTHAWK_IMAGE_DIGEST in the two runner
# Dockerfiles is the digest of the Docker Hub tag
# envoyproxy/nighthawk-dev:<NIGHTHAWK_COMMIT> (resolve with docker buildx
# imagetools inspect), and ENVOY_COMMIT is nighthawk's envoy pin from
# bazel/repositories.bzl at that commit.
ARG NIGHTHAWK_COMMIT=21f3f100b86b11e5211e5672ade956c87baaa75c
ARG ENVOY_COMMIT=ad01ae36a702169140334ebadb2d0fc3be56327e

FROM python:3.12-slim@sha256:b18b261e1b4974b4f321451ad5251ded85abb22c2b8b262da527dda1d031ab30 AS build
ARG NIGHTHAWK_COMMIT
ARG ENVOY_COMMIT
# Same grpcio-tools pin as the ateapi client stage of the nighthawk-ingress
# Dockerfile.
COPY benchmarking/locust/codegen/requirements.txt /tmp/codegen-requirements.txt
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r /tmp/codegen-requirements.txt

# Shallow-fetch each tree: xds, pgv and googleapis at HEAD, nighthawk and
# envoy at their pins. The pins are bare commits, which clone --branch can't
# fetch, hence init+fetch.
RUN git clone --depth 1 https://github.com/cncf/xds /src/xds \
    && git clone --depth 1 https://github.com/bufbuild/protoc-gen-validate /src/pgv \
    && git clone --depth 1 https://github.com/googleapis/googleapis /src/googleapis
RUN git init /src/nighthawk \
    && git -C /src/nighthawk fetch --depth 1 https://github.com/envoyproxy/nighthawk "${NIGHTHAWK_COMMIT}" \
    && git -C /src/nighthawk checkout FETCH_HEAD
RUN git init /src/envoy \
    && git -C /src/envoy fetch --depth 1 https://github.com/envoyproxy/envoy "${ENVOY_COMMIT}" \
    && git -C /src/envoy checkout FETCH_HEAD

COPY benchmarking/nighthawk_common/protogen/generate_descriptor.sh /gen.sh
RUN bash /gen.sh /out/nighthawk.desc

# The result that the runner Dockerfiles copy from.
FROM scratch
COPY --from=build /out/nighthawk.desc /out/nighthawk.desc
