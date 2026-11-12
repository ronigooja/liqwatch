#!/bin/bash
# Replace the running collector slot with a new release without a data gap.
# Usage: liqwatch-handoff.sh <release-dir> [--bootstrap-legacy]
#   Starts the idle slot on the new release, waits until it is ready, then stops the old one.
#   On any failure the new slot is stopped and the old one is left untouched.
# --bootstrap-legacy: the old process is the unnumbered liqwatch-collector.service (first handoff).
set -euo pipefail

REL=${1:?release dir, e.g. /opt/liqwatch/releases/p0-4}
LEGACY=0; [ "${2:-}" = "--bootstrap-legacy" ] && LEGACY=1
READY_HOLD=${READY_HOLD:-60}      # seconds the new slot must stay ready before the old one stops
READY_TIMEOUT=${READY_TIMEOUT:-600}
PORT_A=9101; PORT_B=9102

[ -x "$REL/liqwatch-collector" ] || { echo "no collector binary in $REL" >&2; exit 2; }

active() { systemctl is-active --quiet "$1"; }
port_of() { [ "$1" = a ] && echo $PORT_A || echo $PORT_B; }

if [ $LEGACY = 1 ]; then
  OLD=legacy; NEW=b; OLD_UNIT=liqwatch-collector.service
else
  if active liqwatch-collector@a.service && active liqwatch-collector@b.service; then
    echo "both slots are active; stop one first" >&2; exit 3
  elif active liqwatch-collector@a.service; then OLD=a; NEW=b
  elif active liqwatch-collector@b.service; then OLD=b; NEW=a
  else echo "no active slot; start one with systemctl start liqwatch-collector@a" >&2; exit 3
  fi
  OLD_UNIT=liqwatch-collector@$OLD.service
fi
# The legacy process listens on 9101 (config). Slot b uses 9102, slot a 9101 after legacy is gone.
NEW_PORT=$(port_of $NEW)
[ $LEGACY = 1 ] && NEW=b && NEW_PORT=$PORT_B

echo "old=$OLD new=$NEW release=$REL port=$NEW_PORT"
# Each slot runs the binary through its own link /opt/liqwatch/slots/<slot>; /opt/liqwatch/current only
# moves after success, so a crash-restart of the old slot meanwhile still uses the old release.
mkdir -p /opt/liqwatch/slots
ln -sfn "$REL" /opt/liqwatch/slots/$NEW
printf 'LIQWATCH_METRICS_ADDR=127.0.0.1:%s\n' "$NEW_PORT" > /etc/liqwatch/slot-$NEW.env
NEW_UNIT=liqwatch-collector@$NEW.service

fail() {
  echo "handoff failed: $1; stopping $NEW_UNIT, keeping $OLD_UNIT" >&2
  systemctl stop "$NEW_UNIT" || true
  exit 1
}

systemctl start "$NEW_UNIT" || fail "start"
t0=$(date +%s); ok_since=0
while :; do
  now=$(date +%s)
  [ $((now - t0)) -gt "$READY_TIMEOUT" ] && fail "not ready within ${READY_TIMEOUT}s"
  active "$NEW_UNIT" || fail "new slot exited"
  if curl -fsS "http://127.0.0.1:$NEW_PORT/ready" >/dev/null 2>&1; then
    [ "$ok_since" = 0 ] && ok_since=$now
    [ $((now - ok_since)) -ge "$READY_HOLD" ] && break
  else
    ok_since=0
  fi
  sleep 2
done

m1=$(curl -fsS "http://127.0.0.1:$NEW_PORT/metrics" | awk '/^liqwatch_messages_total /{print $2}')
sleep 5
m2=$(curl -fsS "http://127.0.0.1:$NEW_PORT/metrics" | awk '/^liqwatch_messages_total /{print $2}')
[ "${m2:-0}" -gt "${m1:-0}" ] || fail "no messages flowing"

echo "new slot ready, stopping $OLD_UNIT"
ln -sfn "$REL" /opt/liqwatch/current
systemctl stop "$OLD_UNIT"
[ $LEGACY = 1 ] && systemctl disable "$OLD_UNIT" >/dev/null 2>&1 || true
systemctl enable "$NEW_UNIT" >/dev/null 2>&1 || true
echo "handoff done: active slot $NEW on release $REL"
