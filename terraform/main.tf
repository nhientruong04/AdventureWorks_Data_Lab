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

resource "google_service_account" "service_instance_account" {
  account_id  = "service-vm-instance"
  description = "Service account for all necessary processes in the service instance."
}

resource "google_project_iam_binding" "bigquery_data_editor_role" {
  project = var.project
  role    = "roles/bigquery.dataEditor"
  members = ["serviceAccount:${google_service_account.service_instance_account.email}"]
}

resource "google_project_iam_binding" "bigquery_data_user_role" {
  project = var.project
  role    = "roles/bigquery.user"
  members = ["serviceAccount:${google_service_account.service_instance_account.email}"]
}

resource "google_project_iam_member" "gcs_object_user_role" {
  project = var.project
  role    = "roles/storage.objectUser"
  member  = "serviceAccount:${google_service_account.service_instance_account.email}"
}

resource "google_project_iam_member" "gcs_bucket_view_role" {
  project = var.project
  role    = "roles/storage.bucketViewer"
  member  = "serviceAccount:${google_service_account.service_instance_account.email}"
}

resource "google_project_iam_member" "dataproc_editor_role" {
  project = var.project
  role    = "roles/dataproc.editor"
  member  = "serviceAccount:${google_service_account.service_instance_account.email}"
}

resource "google_project_iam_member" "logging_role" {
  project = var.project
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.service_instance_account.email}"
}

resource "google_project_iam_member" "metric_writer_role" {
  project = var.project
  role    = "roles/monitoring.metricWriter"
  member  = "serviceAccount:${google_service_account.service_instance_account.email}"
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
  name                      = "service-instance"
  description               = "Instance for running Airflow and ELT pipeline"
  machine_type              = "e2-standard-4"
  allow_stopping_for_update = true

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

  service_account {
    email  = google_service_account.service_instance_account.email
    scopes = ["cloud-platform"]
  }

  tags                    = ["service-instance"]
  metadata_startup_script = local.service_instance_startup_script
}

resource "google_storage_bucket" "staging_bucket" {
  name                     = "airbyte-adventureworks2022-staging-bucket"
  location                 = var.region
  force_destroy            = true
  public_access_prevention = "enforced"
  ip_filter {
    mode                 = "Disabled" # terraform cannot get the state after changing to Enabled
    allow_cross_org_vpcs = true
    vpc_network_sources {
      network                = "projects/${var.project}/global/networks/${google_compute_network.vpc_network.name}"
      allowed_ip_cidr_ranges = ["0.0.0.0/0"]
    }
  }
}
