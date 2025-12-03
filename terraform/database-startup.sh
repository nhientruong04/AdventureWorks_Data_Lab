#!/bin/bash

# Download mssql-server Ubuntu 22.04 version
curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | sudo gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg
curl https://packages.microsoft.com/keys/microsoft.asc | sudo tee /etc/apt/trusted.gpg.d/microsoft.asc
curl -fsSL https://packages.microsoft.com/config/ubuntu/22.04/mssql-server-2022.list | sudo tee /etc/apt/sources.list.d/mssql-server-2022.list
# Tweak to make it run on Ubuntu 24.04, found on https://learn.microsoft.com/en-us/answers/questions/1693491/how-to-install-sql-server-2022-on-ubuntu-server-24
wget http://mirrors.kernel.org/ubuntu/pool/main/o/openldap/libldap-2.5-0_2.5.11+dfsg-1%7Eexp1ubuntu3_amd64.deb
sudo dpkg -i libldap-2.5-0_2.5.11+dfsg-1~exp1ubuntu3_amd64.deb
sudo apt-get update
sudo apt-get install -y mssql-server

# Setup SA account
export MSSQL_PID="Express"
export MSSQL_SA_PASSWORD="Passw0rd!PassWoRD"
export ACCEPT_EULA="Y"
sudo -E /opt/mssql/bin/mssql-conf -n setup accept-eula

# Start manually
sudo systemctl start mssql-server
