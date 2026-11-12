#!/bin/bash
# Failover demo: three game servers share one Redis; bots play through all of them; one
# server is stopped in the middle of the measured window. SIGKILL (the default) is a crash:
# no final snapshots, matches move after the lease. SIGTERM is a rolling deploy: final
# snapshots, the node marks itself leaving, matches move on the next adoption scan.
# The report shows how many bots lost their connection and how many resumed their match
# on another server.
#
# usage: scripts/cluster-failover.sh [PAIRS] [KILL_AFTER_SECONDS] [SIGKILL|SIGTERM]
set -eu
export MSYS_NO_PATHCONV=1 # Git Bash on Windows would rewrite /out/... into a host path
PAIRS=${1:-300}
KILL_AFTER=${2:-35}
SIGNAL=${3:-SIGKILL}
NAME=failover # result file names; SIGKILL keeps the original ones
[ "$SIGNAL" = SIGKILL ] || NAME=$(echo "$SIGNAL" | tr 'A-Z' 'a-z')
cd "$(dirname "$0")/.."
F=(-f docker-compose.cluster.yml)
OUT=docs/loadtest/cluster
mkdir -p "$OUT"

docker compose "${F[@]}" up -d --build --wait redis game-1 game-2 game-3
docker rm -f sabo-bot >/dev/null 2>&1 || true
docker compose "${F[@]}" run --build -d --name sabo-bot bot \
  -url=ws://game-1:8080/ws,ws://game-2:8080/ws,ws://game-3:8080/ws \
  -metrics=http://game-1:8080/metrics,http://game-2:8080/metrics,http://game-3:8080/metrics \
  -proto=bin -resume-on-drop -pairs="$PAIRS" -duration=60s -warmup=10s -label="$NAME" \
  -json="/out/$NAME.json" >/dev/null

sleep "$KILL_AFTER"
{
  echo "=== $(date -u +%FT%TZ) stopping game-2 ($SIGNAL)"
  docker compose "${F[@]}" kill -s "$SIGNAL" game-2 2>&1 | tail -1
} | tee "$OUT/console-$NAME.txt"

docker wait sabo-bot >/dev/null
{
  echo "=== bot report"
  docker logs sabo-bot 2>&1 | grep -v ': VIOLATION' | tail -20
  echo "=== invariant violations reported by bots: $(docker logs sabo-bot 2>&1 | grep -c ': VIOLATION' || true)"
  docker logs sabo-bot 2>&1 | grep ': VIOLATION' | head -20 || true
  echo "=== takeover on the surviving nodes"
  docker compose "${F[@]}" logs --no-log-prefix game-1 game-3 2>&1 | grep -E '"adopted matches|lease' || true
  echo "matches adopted per node:"
  for n in game-1 game-3; do
    echo "  $n: $(docker compose "${F[@]}" logs --no-log-prefix $n 2>&1 | grep -c '"match adopted"' || true)"
  done
  if [ "$SIGNAL" != SIGKILL ]; then
    echo "=== shutdown of game-2"
    docker compose "${F[@]}" logs --no-log-prefix game-2 2>&1 | grep -E 'shutting down|leaving|shutdown complete|cluster' || true
  fi
} | tee -a "$OUT/console-$NAME.txt"
docker rm -f sabo-bot >/dev/null
docker compose "${F[@]}" down
