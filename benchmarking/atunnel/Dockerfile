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

# One image holding the benchmark's one program; each container picks its
# subcommand. The build context is the repo root; BuildKit transfers only the
# paths the COPY lines name. The build stage compiles natively for the target
# architecture, so an arm64 host cross-compiles amd64 without emulation.
#
#   docker build --target bench -t atunnel-bench:local -f benchmarking/atunnel/Dockerfile .   # run-docker.sh
#   docker build --platform linux/amd64 -f benchmarking/atunnel/Dockerfile .                  # the cluster image

FROM --platform=$BUILDPLATFORM golang:1.27.3-bookworm@sha256:d9b6a1beb9474adc2a1ce842573639b44b2a0dd49b6669f51d3821baf9ec37c9 AS build
ARG TARGETARCH
WORKDIR /src
# Only what the program imports: a developer's runs/ or venv/ never enters.
COPY go.mod go.sum ./
COPY vendor ./vendor
COPY api ./api
COPY internal ./internal
COPY pkg ./pkg
COPY benchmarking/atunnel/cmd ./benchmarking/atunnel/cmd
COPY benchmarking/atunnel/internal ./benchmarking/atunnel/internal
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH GOFLAGS=-mod=vendor go build -o /out/ ./benchmarking/atunnel/cmd/...

FROM gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872c891853ca7fcf1f12c3edb23f7eeef36189728842dd51042ff57f7ab AS bench
COPY --from=build /out/atunnel-bench /usr/local/bin/
