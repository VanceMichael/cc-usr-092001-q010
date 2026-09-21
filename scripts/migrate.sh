#!/bin/sh
# 初始化本地事件日志目录。DATABASE_PATH 默认指向 data/events.jsonl。
set -eu
DATABASE_PATH="${DATABASE_PATH:-data/events.jsonl}"
mkdir -p "$(dirname "$DATABASE_PATH")"
if [ ! -f "$DATABASE_PATH" ]; then
  : > "$DATABASE_PATH"
  printf '%s\n' "已创建空事件日志: $DATABASE_PATH"
else
  printf '%s\n' "事件日志已存在: $DATABASE_PATH（仅追加存储，初始化不会改动既有事实）"
fi
