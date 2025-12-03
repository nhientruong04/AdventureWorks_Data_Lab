terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.12.0"
    }
  }
}

locals {
  ssh_key_file = trimspace(file(var.ssh_key_path))
}

resource "google_compute_network" "vpc_network" {
  name                    = "general-network"
  description             = "Network for all elements for illustration."
  auto_create_subnetworks = true
}

resource "google_compute_firewall" "ssh_rule" {
  name    = "allow-ssh-rule"
  network = google_compute_network.vpc_network.id
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["0.0.0.0/0"]
}

resource "google_compute_instance" "db_instance" {
  name         = "production-database"
  description  = "OLTP database instance."
  machine_type = "e2-standard-2"

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2404-lts-amd64"
    }
  }

  network_interface {
    network = google_compute_network.vpc_network.id

    access_config {
    }
  }

  metadata = {
    ssh-keys = "nhien:${local.ssh_key_file}"
  }
}
