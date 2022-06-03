#!/bin/bash
set -euox pipefail
cd /home/ubuntu/nft
cp /home/ubuntu/config/nft/.env /home/ubuntu/nft/
docker compose up -d --build --force-recreate --remove-orphans