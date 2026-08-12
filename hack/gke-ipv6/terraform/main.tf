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

variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region"
  type        = string
  default     = "us-central1"
}

variable "network_name" {
  description = "The VPC network name"
  type        = string
  default     = "gke-ipv6-vpc"
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_compute_network" "main" {
  name                    = var.network_name
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "node_subnet" {
  name          = "${var.network_name}-node-subnet"
  network       = google_compute_network.main.id
  region        = var.region
  ip_cidr_range = "10.0.0.0/20"

  stack_type       = "IPV4_IPV6"
  ipv6_access_type = "EXTERNAL"
}

resource "google_compute_subnetwork" "psc_subnet" {
  name          = "${var.network_name}-psc-subnet"
  network       = google_compute_network.main.id
  region        = var.region
  ip_cidr_range = "10.0.16.0/24"
  purpose       = "PRIVATE_SERVICE_CONNECT"

  stack_type = "IPV6_ONLY"
}

resource "google_compute_firewall" "default_deny_ipv6_ingress" {
  name    = "${var.network_name}-default-deny-ipv6-ingress"
  network = google_compute_network.main.name

  deny {
    protocol = "all"
  }

  direction     = "INGRESS"
  priority      = 65534
  source_ranges = ["::/0"]
}
