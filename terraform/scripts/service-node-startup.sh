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

# INFO: somehow 4 CPUs and 16GBs still not enough for Airbyte
sudo abctl local install --low-resource-mode
echo "=== Airbyte installation completed ==="

echo "=== Setting up Airbyte ==="
sudo apt-get install -y \
    git \
    python3 \
    python3-pip \
    python3-venv \
    build-essential
git clone https://github.com/nhientruong04/AdventureWorks_Data_Lab.git AdventureWorks_source
cd AdventureWorks_source/Airbyte
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
