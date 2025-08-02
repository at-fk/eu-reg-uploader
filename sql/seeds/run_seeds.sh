#!/bin/bash

# データベース接続情報（環境変数から取得）
DB_URL="${DATABASE_URL:-postgresql://localhost:5432/your_database_name}"

# シードファイルを順番に実行
echo "Running seed files..."
psql "$DB_URL" -f 01_tags.sql
psql "$DB_URL" -f 02_content_tags.sql

echo "Verifying tags..."
psql "$DB_URL" -f 03_verify_tags.sql

echo "Seed completed!"
