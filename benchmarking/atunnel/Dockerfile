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

# Build context is the repository root:
#
#   docker build --target bench -f benchmarking/atunnel/Dockerfile .
#   docker build -f benchmarking/atunnel/Dockerfile .
FROM --platform=$BUILDPLATFORM golang:1.27.3-bookworm@sha256:d9b6a1beb9474adc2a1ce842573639b44b2a0dd49b6669f51d3821baf9ec37c9 AS build
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
COPY vendor ./vendor
COPY api ./api
COPY cmd ./cmd
COPY internal ./internal
COPY pkg ./pkg
COPY benchmarking/atunnel/cmd ./benchmarking/atunnel/cmd
COPY benchmarking/atunnel/internal ./benchmarking/atunnel/internal
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=linux GOARCH="${TARGETARCH}" GOFLAGS=-mod=vendor \
    go build -o /out/atunnel-bench ./benchmarking/atunnel/cmd/atunnel-bench

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab AS bench
COPY --from=build /out/atunnel-bench /usr/local/bin/atunnel-bench

FROM python:3.11-slim@sha256:db20d8506e2f15b6db39b40b6015ca46bae855dec6dd326d462cfe79ae972893 AS pydeps
WORKDIR /app
COPY benchmarking/atunnel/requirements.txt .
RUN pip install --no-cache-dir --target=/app/deps -r requirements.txt

FROM gcr.io/distroless/python3-debian12:nonroot@sha256:7d1042ce588ab97019fe95c24ffca7bc5a82ccdac572511d5e09bda4435c89c5 AS runner
WORKDIR /app
COPY --from=build /out/atunnel-bench /usr/local/bin/atunnel-bench
COPY --from=pydeps /app/deps /app/deps
COPY benchmarking/atunnel/output.py benchmarking/atunnel/runner.py /app/
ENV PYTHONPATH=/app:/app/deps
ENV PYTHONUNBUFFERED=1
