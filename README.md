# EU Regulation Uploader

EU（欧州連合）の法令を自動的に解析・構造化し、Supabaseデータベースに保存して検索可能にするシステムです。

## 概要

このプロジェクトは、EUR-Lex（EUの法令データベース）からHTML形式の法令文書を取得し、以下の処理を行います：

- **構造化解析**: 前文、章、条、附属書の階層構造を抽出
- **データベース保存**: Supabase（PostgreSQL）に構造化データを保存
- **エンベディング生成**: AIによるセマンティックベクトルを生成
- **検索機能**: 全文検索とベクトル検索の両方を提供

## 対応法令

現在対応しているEU法令：

- **GDPR**: 一般データ保護規則
- **Battery Regulation**: 電池規制
- **CMR**: 化学物質規制
- **ESPR**: エコデザイン規制
- **PPWR**: 包装廃棄物規制
- **EUDR**: 森林破壊規制
- **CSDDD**: 企業持続可能性指令
- **Forced Labour Regulation**: 強制労働規制

## 技術スタック

- **Python**: BeautifulSoup4、psycopg2、supabase、requests
- **データベース**: Supabase（PostgreSQL + vector拡張）
- **AI**: Jina AI（エンベディング生成）
- **検索**: ベクトル検索 + 全文検索

## セットアップ

### 前提条件

- Python 3.8以上
- Supabaseアカウント
- Jina AI APIキー

### インストール

1. リポジトリをクローン
```bash
git clone https://github.com/at-fk/eu-reg-uploader.git
cd eu-reg-uploader
```

2. 仮想環境を作成・アクティベート
```bash
python -m venv myenv
source myenv/bin/activate  # macOS/Linux
# または
myenv\Scripts\activate  # Windows
```

3. 依存関係をインストール
```bash
pip install -r requirements.txt
```

4. 環境変数を設定
```bash
cp .env.example .env
# .envファイルを編集して必要な設定を追加
```

### 環境変数の設定

`.env`ファイルに以下の設定を追加してください：

```env
# Supabase設定
SUPABASE_URL=your_supabase_url
SUPABASE_ANON_KEY=your_supabase_anon_key

# Jina AI設定
JINA_API_KEY=your_jina_api_key

# クラウドSupabase設定（オプション）
CLOUD_SUPABASE_URL=your_cloud_supabase_url
CLOUD_SUPABASE_ANON_KEY=your_cloud_supabase_anon_key
```

## 使用方法

### 1. 法令の解析とアップロード

```bash
python regulation_uploader.py --url "EUR-Lexの法令URL" --metadata "metadata.json"
```

### 2. エンベディングの生成

```bash
python create_embeddings.py --regulation_id "regulation_id"
```

### 3. データベースの同期

```bash
python sync_supabase.py
```

## プロジェクト構造

```
eu-reg-uploader/
├── regulation_uploader.py      # メインのアップロード処理
├── eu_reg_html_analyzer.py     # HTML解析エンジン
├── create_embeddings.py        # エンベディング生成
├── sync_supabase.py           # データ同期
├── structure_analyzer.py       # 構造解析
├── requirements.txt           # Python依存関係
├── sql/                      # データベーススキーマ
│   ├── schema/
│   ├── functions/
│   └── seeds/
├── previews/                 # 解析結果のプレビュー
└── metadata/                 # 法令メタデータ
```

## データベーススキーマ

主要なテーブル：

- `regulations`: 法令の基本情報
- `chapters`: 章情報
- `sections`: 節情報
- `articles`: 条情報
- `paragraphs`: パラグラフ情報
- `recitals`: 前文情報
- `annexes`: 附属書情報
- `embeddings`: エンベディングベクトル

## 検索機能

### 全文検索
```sql
SELECT * FROM search_articles('キーワード', 'AND', regulation_id);
```

### ベクトル検索
```sql
SELECT * FROM search_embeddings('検索クエリ', regulation_id);
```

## 開発

### 新しい法令の追加

1. `metadata/`ディレクトリにメタデータJSONファイルを作成
2. EUR-LexのURLを取得
3. `regulation_uploader.py`で解析・アップロードを実行

### テスト

```bash
# プレビューモードでテスト
python regulation_uploader.py --preview-only --url "URL" --metadata "metadata.json"
```

## ライセンス

このプロジェクトはMITライセンスの下で公開されています。

## 貢献

プルリクエストやイシューの報告を歓迎します。

## 注意事項

- 大量のデータを処理する際は、適切なメモリ管理に注意してください
- APIキーなどの機密情報は`.env`ファイルで管理し、Gitにコミットしないでください
- 法令データの使用には、適切な法的制約を確認してください 