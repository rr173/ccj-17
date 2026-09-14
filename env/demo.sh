#!/usr/bin/env bash
# 端到端小演示：起服务 -> 写入 -> 登记钉住读者 -> 压缩 -> 查询钉位/校验/状态
set -euo pipefail
BASE="${BASE:-http://127.0.0.1:${PORT:-8080}}"
j() { python3 -c "import sys,json;d=json.load(sys.stdin);print(json.dumps(d,ensure_ascii=False,indent=2))"; }

echo "== health =="
curl -fsS "$BASE/health"; echo

echo "== 写入 40 条 put（SEGMENT_BYTES 小时会自动滚段）=="
for i in $(seq 0 39); do
  curl -fsS -XPOST "$BASE/append" -H 'content-type: application/json' \
    -d "{\"type\":\"put\",\"payload\":{\"key\":\"k$((i%3))\",\"value\":$i}}" >/dev/null
done

echo "== 钉住读者 alice 于 seq 25，TTL 60s =="
curl -fsS -XPOST "$BASE/readers/alice" -H 'content-type: application/json' \
  -d '{"pin_seq":25,"ttl_ms":60000}'; echo

echo "== 可回收范围（钉点之前）=="
curl -fsS "$BASE/pins" | j

echo "== 手动压缩（只能回收钉点之前）=="
curl -fsS -XPOST "$BASE/compact" -H 'content-type: application/json' -d '{"force":true}' | j

echo "== 最近一次压缩结果（含业务等价证明）=="
curl -fsS "$BASE/compact/result" | j

echo "== 复核当前边界 =="
curl -fsS -XPOST "$BASE/compact/verify" -H 'content-type: application/json' -d '{}' | j

echo "== 从头读取的业务状态 =="
curl -fsS "$BASE/state" | j

echo "== 释放钉位 =="
curl -fsS -XDELETE "$BASE/readers/alice" -o /dev/null -w "deleted %{http_code}\n"
