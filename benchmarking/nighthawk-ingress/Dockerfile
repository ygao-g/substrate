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

# Nighthawk router-capacity benchmark runner image. Build context is the
# repo root. Build it through the bake file, which supplies nighthawk.desc
# as the "protogen" context:
#   docker buildx bake -f benchmarking/nighthawk_common/docker-bake.hcl --load \
#     --set nighthawk-ingress.tags=<image> nighthawk-ingress
# A plain docker build -f of this file fails: it looks for an image named
# protogen.

# NIGHTHAWK_IMAGE_DIGEST moves together with the commit pins in
# benchmarking/nighthawk_common/protogen/Dockerfile.
ARG NIGHTHAWK_IMAGE_DIGEST=sha256:7729902afea9d83f593b240690ba7d6e8fff04f6215ada538cfb4c1452b2553e

# --- Stage 1: ateapi Python clients ------------------------------------------
# Generated from the protos rather than checked in; see
# benchmarking/locust/codegen/generate.sh. Only ateapi: the ingress benchmark
# never talks to glutton.
FROM python:3.12-slim AS ateprotos
WORKDIR /src
COPY benchmarking/locust/codegen/requirements.txt benchmarking/locust/codegen/
RUN pip install --no-cache-dir -r benchmarking/locust/codegen/requirements.txt
COPY benchmarking/locust/codegen/generate.sh benchmarking/locust/codegen/
COPY api api
RUN PYTHON=python3 OUT_DIR=/out benchmarking/locust/codegen/generate.sh api/ateapi/v1/ateapi.proto

# --- Stage 2: runtime -------------------------------------------------------
# nighthawk-dev has no stable tags; pinned by digest so the pull is
# immutable. Ships the nighthawk_* binaries in /usr/local/bin on ubuntu.
FROM envoyproxy/nighthawk-dev@${NIGHTHAWK_IMAGE_DIGEST}

# Deps must be installed with this image's own interpreter: grpcio/protobuf
# native extensions are python-version-specific.
COPY benchmarking/nighthawk-ingress/requirements.txt /tmp/requirements.txt
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip ca-certificates \
    && python3 -m pip install --no-cache-dir --break-system-packages \
        --target=/app/deps -r /tmp/requirements.txt \
    && apt-get purge -y python3-pip \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* /tmp/requirements.txt

WORKDIR /app
COPY --from=protogen /out/nighthawk.desc /app/nighthawk.desc

# ateapi client reused from the locust harness (atespace.py excluded: it
# imports locust).
COPY benchmarking/locust/common/__init__.py /app/common/__init__.py
COPY benchmarking/locust/common/ateapi_channel.py /app/common/ateapi_channel.py
COPY --from=ateprotos /out/ /app/common/
COPY benchmarking/nighthawk_common /app/nighthawk_common

COPY benchmarking/nighthawk-ingress/runner.py \
     benchmarking/nighthawk-ingress/spec.py \
     benchmarking/nighthawk-ingress/actors.py \
     /app/
COPY benchmarking/nighthawk-ingress/tests /app/tests

ENV PYTHONPATH=/app:/app/deps
ENV PYTHONUNBUFFERED=1
ENV NIGHTHAWK_DESC=/app/nighthawk.desc

ENTRYPOINT ["python3", "/app/runner.py"]
