#!/usr/bin/env bash
# Clear all pipeline cache for an issuer (keeps profile/corp-graph data intact).
# Usage: ./scripts/clear_cache.sh <issuer_id>
#        ./scripts/clear_cache.sh ffe47e1d-7118-482c-957e-6899539ba030

set -euo pipefail

ISSUER_ID="${1:?Usage: clear_cache.sh <issuer_id>}"
DB_URL="${CORPGRAPH_DB_URL:?Set CORPGRAPH_DB_URL}"

echo "Clearing cache for issuer: $ISSUER_ID"

psql "$DB_URL" -c "
  DELETE FROM extraction_results WHERE issuer_id = '$ISSUER_ID';
  DELETE FROM scraped_pages      WHERE issuer_id = '$ISSUER_ID';
  DELETE FROM discovered_urls    WHERE issuer_id = '$ISSUER_ID';
  DELETE FROM discovered_assets  WHERE issuer_id = '$ISSUER_ID';
  DELETE FROM qa_results         WHERE issuer_id = '$ISSUER_ID';
"

echo "Done."
