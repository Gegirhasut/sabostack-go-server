#!/bin/bash
# Load comparison of several server/bot variants on Linux (used under WSL2).
# Same method as loadtest-step.sh: a fresh server per run, 1 bot/ms ramp, 15 s warm-up, a
# measured window. At every step all variants run back to back, so drift in background
# host load hits them alike; repeat a step ("2000#2") and loadreport takes the median.
#
# usage: scripts/loadtest-variants.sh VARIANTS_FILE OUT_DIR BOT DURATION STEP...
#   VARIANTS_FILE: one variant per line, "name|server binary|server flags|bot flags"
#   STEP: a pair count, optionally with "#N" for a repeat (2000#2 -> <name>-2000-r2.json)
set -u
VARIANTS=$1; OUT=$2; BOT=$3; DUR=$4; shift 4
mkdir -p "$OUT"
D=/tmp/sabovariants; mkdir -p $D
cp "$BOT" $D/bot && chmod +x $D/bot || exit 1
ulimit -n 65536

names=(); servers=(); sflags=(); bflags=()
while IFS='|' read -r name server sf bf; do
  [ -z "$name" ] || [ "${name:0:1}" = "#" ] && continue
  cp "$server" "$D/server-$name" && chmod +x "$D/server-$name" || exit 1
  names+=("$name"); servers+=("$server"); sflags+=("$sf"); bflags+=("$bf")
done < "$VARIANTS"

run() { # index pairs label
  local i=$1 pairs=$2 label=$3
  echo "=== $label (${names[$i]}, $pairs pairs) $(date -Is)"
  # shellcheck disable=SC2086
  $D/server-${names[$i]} -addr 127.0.0.1:18085 -log-level warn ${sflags[$i]} > $D/server-$label.log 2>&1 &
  local sp=$!
  sleep 1
  # shellcheck disable=SC2086
  /usr/bin/time -f "bot_max_rss_kb=%M bot_cpu_user_s=%U bot_cpu_sys_s=%S" \
    $D/bot -url ws://127.0.0.1:18085/ws -metrics http://127.0.0.1:18085/metrics \
    -pairs "$pairs" -ramp 1ms -duration "$DUR" -warmup 15s -label "$label" \
    -json "$OUT/$label.json" ${bflags[$i]} > $D/bot-$label.out 2> $D/bot-$label.err
  echo "bot exit: $?"
  grep -E '^(matches|errors|messages|input->ack|server (CPU|RSS|tick|traffic))' $D/bot-$label.out
  tail -1 $D/bot-$label.err
  echo "bot stderr lines (errors): $(($(wc -l < $D/bot-$label.err) - 1))"
  local t0 t1 rc; t0=$(date +%s.%N); kill -TERM $sp; wait $sp; rc=$?; t1=$(date +%s.%N)
  echo "server exit code: $rc, shutdown took $(echo "$t1 - $t0" | bc)s"
  echo "server_error_log_lines=$(grep -c '"level":"ERROR"' $D/server-$label.log)"
  echo "host load: $(cut -d' ' -f1-3 /proc/loadavg)"
}

{
  echo "uname: $(uname -sr), nproc: $(nproc), duration: $DUR, variants: ${names[*]}"
  for step in "$@"; do
    rep=""; case $step in *#*) rep="-r${step#*#}"; step=${step%%#*} ;; esac
    for i in "${!names[@]}"; do
      run "$i" "$step" "${names[$i]}-$(printf %04d "$step")$rep"
    done
  done
} 2>&1 | tee -a "$OUT/console-variants.txt"
