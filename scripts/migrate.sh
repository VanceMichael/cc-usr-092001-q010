#!/bin/sh
set -eu
mkdir -p "${DATABASE_PATH%/*}"
printf '%s
' '当前基础服务不预设数据库结构。'
