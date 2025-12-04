#!/bin/bash

set -euo pipefail
export DEBIAN_FRONTEND=noninteractive

echo "=== Installing Docker Engine ==="
# Add Docker's official GPG key:
sudo apt update
sudo apt install ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

# Add the repository to Apt sources:
sudo tee /etc/apt/sources.list.d/docker.sources <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Signed-By: /etc/apt/keyrings/docker.asc
EOF

sudo apt update
sudo apt upgrade -y

sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl enable --now docker

echo "=== Docker installed ==="

echo "=== Installing Airbyte abctl CLI ==="
curl -LsfS https://get.airbyte.com | bash -
echo "=== Installing Airbyte locally ==="

sudo abctl local install
echo "=== Airbyte installation completed ==="
