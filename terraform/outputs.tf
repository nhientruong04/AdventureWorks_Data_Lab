output "db_instance_ip" {
  value = google_compute_instance.db_instance.network_interface[0].network_ip
}

output "service_instance_ip" {
  value = google_compute_instance.service_instance.network_interface[0].network_ip
}

output "gcs_hmac_accessId" {
  value = google_storage_hmac_key.gcs_hmacKey.access_id
}

output "gcs_hmac_secret" {
  value     = google_storage_hmac_key.gcs_hmacKey.secret
  sensitive = true
}
