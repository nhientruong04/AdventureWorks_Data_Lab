#!/bin/bash

set -euo pipefail

# INFO: somehow 4 CPUs and 16GBs still not enough for Airbyte
echo "=== Installing Airbyte ==="
curl -LsfS https://get.airbyte.com | bash -
su -l "$TARGET_USER" -c "abctl local install --low-resource-mode"
echo "=== Airbyte installation completed ==="

apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    build-essential
echo "=== Setting up Airbyte and Airflow ==="
su -l "$TARGET_USER" <<EOF
cd ~/AdventureWorks_source/Airbyte
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

cd ~/AdventureWorks_source/Airflow
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker compose up -d
EOF
echo "=== Setup completed ==="

exit 0
