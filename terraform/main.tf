terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.12.0"
    }
  }
}

locals {
  ssh_key_file                    = trimspace(file(var.ssh_key_path))
  db_startup_script               = file("${path.module}/scripts/database-startup.sh")
  service_instance_startup_script = file("${path.module}/scripts/service-node-startup.sh")
}

resource "google_service_account" "airbyte_account" {
  account_id  = "airbyte-adventureworks2022"
  description = "Service account for Airbyte process in the service instance."
}

resource "google_project_iam_binding" "bigquery_data_editor_role" {
  project = var.project
  role    = "roles/bigquery.dataEditor"
  members = ["serviceAccount:${google_service_account.airbyte_account.email}"]
}

resource "google_project_iam_binding" "bigquery_data_user_role" {
  project = var.project
  role    = "roles/bigquery.user"
  members = ["serviceAccount:${google_service_account.airbyte_account.email}"]
}

resource "google_compute_network" "vpc_network" {
  name                    = "general-network"
  description             = "Network for all elements for illustration."
  auto_create_subnetworks = true
}

resource "google_compute_router_nat" "public_nat" {
  name                               = "public-nat"
  source_subnetwork_ip_ranges_to_nat = "ALL_SUBNETWORKS_ALL_IP_RANGES"
  router                             = google_compute_router.public_router.name
  nat_ip_allocate_option             = "AUTO_ONLY"
}

resource "google_compute_router" "public_router" {
  name        = "public-router"
  description = "Router for public NAT."
  network     = google_compute_network.vpc_network.id
}

resource "google_compute_firewall" "internet_outbound_rule" {
  name        = "restrict-internet-outbound-rule"
  description = "Allow Internet outbound traffic to HTTP/HTTPS only."
  network     = google_compute_network.vpc_network.id
  allow {
    protocol = "tcp"
    ports    = ["80", "443"]
  }

  source_ranges      = ["0.0.0.0/0"]
  destination_ranges = ["0.0.0.0/0"]
}

resource "google_compute_firewall" "db_inbound_rule" {
  name        = "allow-inbound-db-port"
  description = "Allow service-node to read database in 1433 port."
  network     = google_compute_network.vpc_network.id
  allow {
    protocol = "tcp"
    ports    = ["1433"]
  }

  source_ranges      = [google_compute_instance.service_instance.network_interface[0].network_ip]
  destination_ranges = [google_compute_instance.db_instance.network_interface[0].network_ip]
}

# NOTE: should be only service_instance, this is for all instances currently
resource "google_compute_firewall" "ssh_rule" {
  name        = "allow-ssh"
  description = "Allow SSH to service node via IAP."
  network     = google_compute_network.vpc_network.id
  allow {
    protocol = "tcp"
    ports    = ["22", "8000"]
  }
  source_ranges      = ["35.235.240.0/20"]
  destination_ranges = ["0.0.0.0/0"]
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
  machine_type = "e2-standard-4"

  boot_disk {
    initialize_params {
      image = "ubuntu-os-cloud/ubuntu-2404-lts-amd64"
      size  = 80
      type  = "pd-balanced"
    }
  }

  network_interface {
    network = google_compute_network.vpc_network.id
  }

  metadata = {
    ssh-keys = "nhien:${local.ssh_key_file}"
  }

  tags                    = ["service-instance"]
  metadata_startup_script = local.service_instance_startup_script
}
