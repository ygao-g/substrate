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

# Nighthawk egress-capacity benchmark runner image.
# Build context is the repo root; always build with --platform linux/amd64:
#   docker build --platform linux/amd64 -f benchmarking/nighthawk-egress/Dockerfile .

ARG NIGHTHAWK_COMMIT=21f3f100b86b11e5211e5672ade956c87baaa75c
ARG ENVOY_COMMIT=ad01ae36a702169140334ebadb2d0fc3be56327e
ARG NIGHTHAWK_IMAGE_DIGEST=sha256:7729902afea9d83f593b240690ba7d6e8fff04f6215ada538cfb4c1452b2553e

# --- Stage 1: proto descriptor set -----------------------------------------
FROM python:3.12-slim AS protogen
ARG NIGHTHAWK_COMMIT
ARG ENVOY_COMMIT
COPY benchmarking/locust/codegen/requirements.txt /tmp/codegen-requirements.txt
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r /tmp/codegen-requirements.txt

RUN git clone --depth 1 https://github.com/cncf/xds /src/xds \
    && git clone --depth 1 https://github.com/bufbuild/protoc-gen-validate /src/pgv \
    && git clone --depth 1 https://github.com/googleapis/googleapis /src/googleapis
RUN git init /src/nighthawk \
    && git -C /src/nighthawk fetch --depth 1 https://github.com/envoyproxy/nighthawk "${NIGHTHAWK_COMMIT}" \
    && git -C /src/nighthawk checkout FETCH_HEAD
RUN git init /src/envoy \
    && git -C /src/envoy fetch --depth 1 https://github.com/envoyproxy/envoy "${ENVOY_COMMIT}" \
    && git -C /src/envoy checkout FETCH_HEAD

COPY benchmarking/nighthawk-ingress/protogen/generate_descriptor.sh /gen.sh
RUN bash /gen.sh /out/nighthawk.desc

# --- Stage 2: ateapi Python clients ------------------------------------------
FROM python:3.12-slim AS ateprotos
WORKDIR /src
COPY benchmarking/locust/codegen/requirements.txt benchmarking/locust/codegen/
RUN pip install --no-cache-dir -r benchmarking/locust/codegen/requirements.txt
COPY benchmarking/locust/codegen/generate.sh benchmarking/locust/codegen/
COPY api api
RUN PYTHON=python3 OUT_DIR=/out benchmarking/locust/codegen/generate.sh api/ateapi/v1/ateapi.proto

# --- Stage 3: runtime -------------------------------------------------------
FROM envoyproxy/nighthawk-dev@${NIGHTHAWK_IMAGE_DIGEST}

COPY benchmarking/nighthawk-egress/requirements.txt /tmp/requirements.txt
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip ca-certificates \
    && python3 -m pip install --no-cache-dir --break-system-packages \
        --target=/app/deps -r /tmp/requirements.txt \
    && apt-get purge -y python3-pip \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* /tmp/requirements.txt

WORKDIR /app
COPY --from=protogen /out/nighthawk.desc /app/nighthawk.desc

COPY benchmarking/locust/common/__init__.py /app/common/__init__.py
COPY benchmarking/locust/common/ateapi_channel.py /app/common/ateapi_channel.py
COPY --from=ateprotos /out/ /app/common/

COPY benchmarking/nighthawk-egress/runner.py \
     benchmarking/nighthawk-egress/spec.py \
     benchmarking/nighthawk-egress/output.py \
     benchmarking/nighthawk-egress/actors.py \
     benchmarking/nighthawk-egress/kubeapi.py \
     benchmarking/nighthawk-egress/resources_common.py \
     benchmarking/nighthawk-egress/resource_sampler.py \
     benchmarking/nighthawk-egress/envoy_stats.py \
     /app/
COPY benchmarking/nighthawk-egress/tests /app/tests

ENV PYTHONPATH=/app:/app/deps
ENV PYTHONUNBUFFERED=1
ENV NIGHTHAWK_DESC=/app/nighthawk.desc

ENTRYPOINT ["python3", "/app/runner.py"]
