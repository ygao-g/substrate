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

# Runner image for `type: egress-tunnels`: the Go driver plus runner.py.
# Built from the repository root by benchmarking/automation (docker build
# -f benchmarking/egress-tunnels/Dockerfile .).

FROM golang:1.27-bookworm AS build
ENV GOTOOLCHAIN=auto
WORKDIR /src
# vendor/ is in .dockerignore, so the module cache is filled from the
# network once, before the sources change the layer.
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
RUN CGO_ENABLED=0 GOFLAGS=-mod=mod go build -o /out/egress-tunnels ./cmd/benchmarking/egress-tunnels

FROM python:3.12-slim
COPY benchmarking/egress-tunnels/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --target=/app/deps -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt
WORKDIR /app
COPY --from=build /out/egress-tunnels /app/egress-tunnels
COPY benchmarking/egress-tunnels/runner.py /app/runner.py
ENV PYTHONPATH=/app/deps
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python3", "/app/runner.py"]
