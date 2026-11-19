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

# Builds the Nighthawk runner images. Both copy nighthawk.desc from the
# nighthawk-protogen target. Run from the repo root:
#   docker buildx bake -f benchmarking/nighthawk_common/docker-bake.hcl --load \
#     --set nighthawk-ingress.tags=<image> nighthawk-ingress
#   docker buildx bake -f benchmarking/nighthawk_common/docker-bake.hcl --load \
#     --set nighthawk-egress.tags=<image> nighthawk-egress
# linux/amd64 only: envoyproxy/nighthawk-dev publishes no arm64 images.

target "nighthawk-protogen" {
  dockerfile = "benchmarking/nighthawk_common/protogen/Dockerfile"
  platforms  = ["linux/amd64"]
}

target "nighthawk-ingress" {
  dockerfile = "benchmarking/nighthawk-ingress/Dockerfile"
  contexts   = { protogen = "target:nighthawk-protogen" }
  platforms  = ["linux/amd64"]
}

target "nighthawk-egress" {
  dockerfile = "benchmarking/nighthawk-egress/Dockerfile"
  contexts   = { protogen = "target:nighthawk-protogen" }
  platforms  = ["linux/amd64"]
}
