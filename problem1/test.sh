#!/usr/bin/env bash
set -euo pipefail

./build.sh
./run.sh

echo ""
echo "Testing all queries..."
for i in {1..10}; do
  docker-compose run --rm app python queries.py --query Q${i} --dbname transit --host db --user transit --password transit123 --format json
done

docker-compose down
