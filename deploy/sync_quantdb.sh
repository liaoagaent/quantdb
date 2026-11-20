#!/usr/bin/env bash
# QuantDB 同步：Windows 仓库目录 -> WSL ext4（供容器 /data 挂载）
# 用法（PowerShell）：wsl -d Ubuntu-24.04 -u root -- bash /mnt/d/gitProject/QuantMind/deploy/sync_quantdb.sh
set -uo pipefail

SRC=/mnt/d/gitProject/QuantMind/data/quantdb/
DST=/home/liao/quantmind-data/quantdb/
LOG=/tmp/sync_quantdb.log

echo "[$(date -Is)] start sync" | tee -a "$LOG"
echo "  src=$SRC" | tee -a "$LOG"
echo "  dst=$DST" | tee -a "$LOG"

mkdir -p "$DST"

rsync -a --info=progress2 "$SRC" "$DST" 2>&1 | tee -a "$LOG"
rc=${PIPESTATUS[0]}
echo "[$(date -Is)] rsync exit=$rc" | tee -a "$LOG"

if [ "$rc" -ne 0 ]; then
  echo "[$(date -Is)] FAILED rc=$rc" | tee -a "$LOG"
  exit "$rc"
fi

echo "[$(date -Is)] fixing ownership" | tee -a "$LOG"
chown -R liao:liao /home/liao/quantmind-data
chmod -R u+rwX,go+rX "$DST"

echo "[$(date -Is)] result:" | tee -a "$LOG"
du -sh "$DST" | tee -a "$LOG"
ls -1 "$DST" | tee -a "$LOG"
du -sh "$DST"6_ml_datasets/* 2>/dev/null | tee -a "$LOG"

echo "[$(date -Is)] ALL DONE" | tee -a "$LOG"
