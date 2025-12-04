terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.12.0"
    }
  }
}

locals {
  ssh_key_file      = trimspace(file(var.ssh_key_path))
  db_startup_script = file("${path.module}/scripts/database-startup.sh")
}

resource "google_compute_network" "vpc_network" {
  name                    = "general-network"
  description             = "Network for all elements for illustration."
  auto_create_subnetworks = true
}

resource "google_compute_firewall" "ssh_rule" {
  name        = "allow-ssh"
  description = "Allow SSH to service node via IAP."
  network     = google_compute_network.vpc_network.id
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
  source_ranges      = ["35.235.240.0/20"]
  destination_ranges = [google_compute_instance.service_instance.network_interface[0].network_ip]
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
  }

  metadata = {
    ssh-keys = "nhien:${local.ssh_key_file}"
  }

  tags = ["db-instance"]

  metadata_startup_script = local.db_startup_script
}

resource "google_compute_instance" "service_instance" {
  name         = "service-instance"
  description  = "Instance for running Airflow and ELT pipeline"
  machine_type = "e2-standard-2"

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2404-lts-amd64"
    }
  }

  network_interface {
    network = google_compute_network.vpc_network.id
  }

  metadata = {
    ssh-keys = "nhien:${local.ssh_key_file}"
  }

  tags = ["service-instance"]
}
