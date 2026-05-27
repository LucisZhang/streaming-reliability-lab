#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
mkdir -p "$root/dashboard/public/results"

shopt -s nullglob
for result in "$root"/showcase/results/*.json; do
  cp "$result" "$root/dashboard/public/results/"
done

echo "Synced showcase/results/*.json to dashboard/public/results/"

