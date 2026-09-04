#!/bin/bash

sudo dnf config-manager --add-repo https://download.docker.com/linux/rhel/docker-ce.repo
sudo dnf install rust cargo go bc podman docker-compose-plugin unzip tmux
cargo install macchina
sudo systemctl enable --now podman.socket
systemctl --user enable --now podman.socket
podman pull orioledb/orioledb:beta13-pg17
podman pull postgres:18.1
podman pull quay.io/coreos/etcd:v3.6.7
sudo dnf upgrade
