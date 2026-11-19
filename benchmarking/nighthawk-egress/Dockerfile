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

# Nighthawk egress-capacity benchmark runner image. Build context is the
# repo root. Build it through the bake file, which supplies nighthawk.desc
# as the "protogen" context:
#   docker buildx bake -f benchmarking/nighthawk_common/docker-bake.hcl --load \
#     --set nighthawk-egress.tags=<image> nighthawk-egress
# A plain docker build -f of this file fails: it looks for an image named
# protogen.

# NIGHTHAWK_IMAGE_DIGEST moves together with the commit pins in
# benchmarking/nighthawk_common/protogen/Dockerfile. It is also the
# nighthawk_service image in traffic-driver-template.yaml.tmpl: the adaptive
# client here calls that service.
ARG NIGHTHAWK_IMAGE_DIGEST=sha256:7729902afea9d83f593b240690ba7d6e8fff04f6215ada538cfb4c1452b2553e

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
COPY benchmarking/nighthawk_common /app/nighthawk_common
COPY benchmarking/nighthawk-egress/runner.py \
     benchmarking/nighthawk-egress/spec.py \
     /app/
COPY benchmarking/nighthawk-egress/tests/check_descriptor.py /app/tests/check_descriptor.py

ENV PYTHONPATH=/app:/app/deps
ENV PYTHONUNBUFFERED=1
ENV NIGHTHAWK_DESC=/app/nighthawk.desc

RUN python3 /app/tests/check_descriptor.py
USER 65532:65532
ENTRYPOINT ["python3", "/app/runner.py"]
