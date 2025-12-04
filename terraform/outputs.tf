output "db_internal_ip" {
  value = google_compute_instance.db_instance.network_interface[0].network_ip
}

output "service_node_internal_ip" {
  value = google_compute_instance.service_instance.network_interface[0].network_ip
}
