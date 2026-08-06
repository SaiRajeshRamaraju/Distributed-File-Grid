#!/bin/env bash

if [ -d /var/cluster_storage/ ]; then
    echo "/var/cluster_storage/ directory already exists"
else
    sudo mkdir /var/cluster_storage/
    sudo chown -R "$USER:$USER" /var/cluster_storage/
    sudo chmod 600 /var/cluster_storage/

fi
