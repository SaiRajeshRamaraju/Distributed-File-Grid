#!/usr/bin/env bash

if [ -d /var/cluster_storage/ ]; then
    echo "/var/cluster_storage/ directory already exists"
else
    sudo mkdir -p /var/cluster_storage/
    sudo chown -R "$USER:$USER" /var/cluster_storage/
    sudo chmod 755 /var/cluster_storage/
fi
