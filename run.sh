#!/bin/bash
set -euox pipefail
cd /home/ubuntu/nft
docker compose up -d --build --force-recreate --remove-orphans