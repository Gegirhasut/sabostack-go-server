#!/bin/bash
# Runs one load-test step on Linux (used under WSL2 for docs/LOAD_TESTING.md).
# Build first: GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o bin/linux/ ./cmd/...
# usage: scripts/loadtest-step.sh PAIRS DURATION LABEL [extra bot flags...]
set -u
PAIRS=$1; DUR=$2; LABEL=$3; shift 3
REPO=${REPO:-$(cd "$(dirname "$0")/.." && pwd)}
D=/tmp/sabobench; mkdir -p $D
cp $REPO/bin/linux/server $REPO/bin/linux/bot $D/; chmod +x $D/server $D/bot
ulimit -n 65536
$D/server -addr 127.0.0.1:18081 -log-level warn > $D/server-$LABEL.log 2>&1 &
SP=$!
sleep 1
/usr/bin/time -f "bot_max_rss_kb=%M bot_cpu_user_s=%U bot_cpu_sys_s=%S" \
  $D/bot -url ws://127.0.0.1:18081/ws -metrics http://127.0.0.1:18081/metrics \
  -pairs $PAIRS -ramp 1ms -duration $DUR -warmup 15s -label $LABEL \
  -json $REPO/docs/loadtest/$LABEL.json "$@" 2> $D/bot-$LABEL.err
echo "bot exit: $?"
tail -1 $D/bot-$LABEL.err
echo "bot stderr lines (errors): $(($(wc -l < $D/bot-$LABEL.err) - 1))"
grep -E 'VmHWM' /proc/$SP/status | sed 's/^/server_peak_/'
T0=$(date +%s.%N); kill -TERM $SP; wait $SP; RC=$?; T1=$(date +%s.%N)
echo "server exit code: $RC, shutdown took $(echo "$T1 - $T0" | bc)s"
grep -c '"level":"ERROR"' $D/server-$LABEL.log | sed 's/^/server_error_log_lines=/'
free -m | sed -n 2p
