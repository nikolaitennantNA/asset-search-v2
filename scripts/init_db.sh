#!/bin/bash
# Create a minimal Postgres database for asset-discovery.
# Doesn't need the full corp-graph — just the 4 cache tables + pgvector.
#
# Prerequisites:
#   brew install postgresql@17 postgis
#   brew services start postgresql@17
#
# Usage:
#   ./scripts/init_db.sh

set -e

DB_NAME="${1:-asset_search}"

echo "Creating database: $DB_NAME"
createdb "$DB_NAME" 2>/dev/null || echo "Database already exists"

echo "Running schema..."
psql "$DB_NAME" -f "$(dirname "$0")/init_cache_db.sql"

echo ""
echo "Done. Set this in your .env:"
echo "  CORPGRAPH_DB_URL=postgresql://localhost/$DB_NAME"
