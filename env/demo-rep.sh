#!/usr/bin/env bash
# 主备复制 + 故障切换演示：
#   启动主 :8080 与备 :8081（独立数据卷）-> 备跟随 -> 主交接 -> 备提升 -> 验证
# 前置：python -m app 已可运行（标准库，无依赖）。
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
P_PORT=8080
S_PORT=8081
BASE_P="http://127.0.0.1:${P_PORT}"
BASE_S="http://127.0.0.1:${S_PORT}"
DATA="$(mktemp -d)"
trap 'kill ${P_PID:-0} ${S_PID:-0} 2>/dev/null || true; rm -rf "$DATA"' EXIT

echo "== 数据目录：$DATA =="
PORT=$P_PORT APP_DATA_DIR="$DATA/p" NODE_ID=P \
  PEERS="P=http://127.0.0.1:${P_PORT},S=http://127.0.0.1:${S_PORT}" \
  GRANT_TTL_MS=600000 QUIET=1 python3 -m app &
P_PID=$!
PORT=$S_PORT APP_DATA_DIR="$DATA/s" NODE_ID=S BOOTSTRAP_ROLE=standby \
  PEERS="P=http://127.0.0.1:${P_PORT},S=http://127.0.0.1:${S_PORT}" \
  REPLICA_SOURCE="$BASE_P" GRANT_TTL_MS=600000 REPLICATION_INTERVAL_MS=200 \
  QUIET=1 python3 -m app &
S_PID=$!

for _ in $(seq 1 50); do
  curl -fsS "$BASE_P/health" >/dev/null 2>&1 && \
  curl -fsS "$BASE_S/health" >/dev/null 2>&1 && break
  sleep 0.1
done

j() { python3 -c "import sys,json;print(json.dumps(json.load(sys.stdin),ensure_ascii=False,indent=2))"; }

echo "== 主写入 8 条 =="
for i in $(seq 0 7); do
  curl -fsS -XPOST "$BASE_P/append" -H 'content-type: application/json' \
    -d "{\"type\":\"put\",\"payload\":{\"key\":\"k$((i%3))\",\"value\":$i}}" >/dev/null
done

echo "== 等待备追平 =="
for _ in $(seq 1 30); do
  phase=$(curl -fsS "$BASE_S/replica" | python3 -c 'import sys,json;print(json.load(sys.stdin)["phase"])')
  [ "$phase" = caught_up ] && break
  sleep 0.2
done
curl -fsS "$BASE_S/replica" | python3 -c 'import sys,json;d=json.load(sys.stdin);print("phase=",d["phase"],"synced_seq=",d["replication"]["synced_seq"],"tip=",d["tip_seq"])'

echo "== 备拒绝写入（403 not_primary）=="
curl -s -o /dev/null -w "%{http_code}\n" -XPOST "$BASE_S/append" \
  -H 'content-type: application/json' -d '{"type":"put","payload":{"key":"x","value":1}}'

echo "== 主交接 stepdown -> 备以 term=2 竞选提升（未追平时会被拒，重试到追平）=="
curl -fsS -XPOST "$BASE_P/cluster/stepdown" -d '{}' | j
for _ in $(seq 1 50); do
  out=$(curl -s -XPOST "$BASE_S/cluster/promote" -H 'content-type: application/json' -d "{
    \"term\":2,\"ttl_ms\":60000,
    \"voters\":[\"$BASE_P\",\"$BASE_S\"]}")
  echo "$out" | python3 -c 'import sys,json;d=json.load(sys.stdin);sys.exit(0 if d.get("term")==2 else 1)' 2>/dev/null \
    && { echo "$out" | j; break; }
  sleep 0.2
done

echo "== 旧主旧任期写入被拒（403）；新主写入成功 =="
curl -s -o /dev/null -w "old primary append -> %{http_code}\n" -XPOST "$BASE_P/append" \
  -H 'content-type: application/json' -d '{"type":"put","payload":{"key":"stale","value":1}}'
curl -fsS -XPOST "$BASE_S/append" -H 'content-type: application/json' \
  -d '{"type":"put","payload":{"key":"after_failover","value":1}}' >/dev/null

echo "== 旧主转为备跟随新主 =="
curl -fsS -XPOST "$BASE_P/replica" -H 'content-type: application/json' \
  -d "{\"peer_url\":\"$BASE_S\"}" >/dev/null
for _ in $(seq 1 30); do
  synced=$(curl -fsS "$BASE_P/replica" | python3 -c 'import sys,json;print(json.load(sys.stdin)["replication"]["synced_seq"])')
  [ "$synced" = "9" ] && break
  sleep 0.2
done
curl -fsS "$BASE_P/state" | python3 -c 'import sys,json;print("old primary now standby, state=",json.load(sys.stdin)["state"])'
curl -fsS "$BASE_S/state" | python3 -c 'import sys,json;print("new primary state           =",json.load(sys.stdin)["state"])'
echo "== 演示完成 =="
