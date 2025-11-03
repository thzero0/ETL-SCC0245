#!/bin/bash

# minio.sh
# Usage:
#   ./minio.sh up
#   ./minio.sh down
#   ./minio.sh delete

case "$1" in
  up)
    echo "Starting MinIO server..."
    docker compose -f minio/minio.yaml up -d
    ;;
  down)
    echo "Stopping MinIO server..."
    docker compose -f minio/minio.yaml down
    ;;
  delete)
    echo "Deleting MinIO data volume..."
    docker volume rm minio_minio_data
    ;;
  *)
    echo "Usage: $0 {up|down|delete}"
    exit 1
    ;;
esac