#!/bin/bash
# Before/after load comparison on Linux (used under WSL2 for docs/HARDENING.md).
# Same method as loadtest-step.sh: a fresh server per run, the bot ramps 1 bot/ms, 15 s
# warm-up, then a measured window. "before" and "after" alternate at every step so slow
# drift in background host load hits both versions alike.
#
# usage: scripts/loadtest-compare.sh BEFORE_SERVER AFTER_SERVER BOT DURATION STEP...
#   a STEP is a pair count, or "storm:PAIRS" for a reconnect storm (20 % at +20 s);
#   append "#N" to label a repeat (e.g. 2000#2 -> before-2000-r2.json)
# Results: docs/loadtest/hardening/<before|after>-<step>.json and console-compare.txt
set -u
BEFORE=$1; AFTER=$2; BOT=$3; DUR=$4; shift 4
REPO=${REPO:-$(cd "$(dirname "$0")/.." && pwd)}
OUT=$REPO/docs/loadtest/hardening; mkdir -p "$OUT"
D=/tmp/sabocompare; mkdir -p $D
cp "$BEFORE" $D/server-before && cp "$AFTER" $D/server-after && cp "$BOT" $D/bot || exit 1
chmod +x $D/server-before $D/server-after $D/bot
ulimit -n 65536

run() { # version pairs label [bot flags...]
  local v=$1 pairs=$2 label=$3; shift 3
  echo "=== $label ($v, $pairs pairs) $(date -Is)"
  $D/server-$v -addr 127.0.0.1:18082 -log-level warn > $D/server-$label.log 2>&1 &
  local sp=$!
  sleep 1
  /usr/bin/time -f "bot_max_rss_kb=%M bot_cpu_user_s=%U bot_cpu_sys_s=%S" \
    $D/bot -url ws://127.0.0.1:18082/ws -metrics http://127.0.0.1:18082/metrics \
    -pairs "$pairs" -ramp 1ms -duration "$DUR" -warmup 15s -label "$label" \
    -json "$OUT/$label.json" "$@" > $D/bot-$label.out 2> $D/bot-$label.err
  echo "bot exit: $?"
  grep -E '^(matches|errors|input->ack|server (CPU|RSS|tick))' $D/bot-$label.out
  tail -1 $D/bot-$label.err
  echo "bot stderr lines (errors): $(($(wc -l < $D/bot-$label.err) - 1))"
  grep -E 'VmHWM' /proc/$sp/status | sed 's/^/server_peak_/'
  local t0 t1 rc; t0=$(date +%s.%N); kill -TERM $sp; wait $sp; rc=$?; t1=$(date +%s.%N)
  echo "server exit code: $rc, shutdown took $(echo "$t1 - $t0" | bc)s"
  echo "server_error_log_lines=$(grep -c '"level":"ERROR"' $D/server-$label.log)"
  echo "host load: $(cut -d' ' -f1-3 /proc/loadavg)"
}

{
  echo "uname: $(uname -sr), nproc: $(nproc), duration: $DUR"
  for step in "$@"; do
    rep=""; case $step in *#*) rep="-r${step#*#}"; step=${step%%#*} ;; esac
    for v in before after; do
      case $step in
        storm:*) p=${step#storm:}; run $v "$p" "$v-storm-$(printf %04d "$p")$rep" -storm-at 20s -storm-frac 0.2 ;;
        *) run $v "$step" "$v-$(printf %04d "$step")$rep" ;;
      esac
    done
  done
} 2>&1 | tee -a "$OUT/console-compare.txt"
