# CELLAR REST API Integration for eu_hierarchical.db

このモジュールは、EUのCELLAR REST APIから取得したXMLデータを`eu_hierarchical.db`に統合するための機能を提供します。

## 概要

CELLAR REST APIは、EUの法令と判例の関連性を示すRDF/XMLデータを提供しています。このモジュールは以下の機能を提供します：

- CELLAR XMLデータの解析
- 引用関係の抽出と構造化
- 既存の`eu_hierarchical.db`スキーマへの統合
- 重複データの処理
- エラーハンドリング

## データ構造

### CELLAR XMLの構造

CELLAR XMLデータは以下の構造を持ちます：

```xml
<!-- 判例の基本情報 -->
<rdf:Description rdf:about="http://publications.europa.eu/resource/cellar/...">
    <owl:sameAs rdf:resource="http://publications.europa.eu/resource/ecli/ECLI:EU:C:2021:470"/>
    <owl:sameAs rdf:resource="http://publications.europa.eu/resource/celex/62021CJ0470"/>
</rdf:Description>

<!-- 引用関係の詳細 -->
<rdf:Description rdf:nodeID="A27835">
    <owl:annotatedSource rdf:resource="http://publications.europa.eu/resource/celex/32016R0679"/>
    <j.2:fragment_citing_source>N 8 95 131</j.2:fragment_citing_source>
    <j.2:fragment_cited_target>A17P1LB</j.2:fragment_cited_target>
    <owl:annotatedTarget rdf:resource="http://publications.europa.eu/resource/celex/62021CJ0470"/>
    <owl:annotatedProperty rdf:resource="http://publications.europa.eu/ontology/cdm#work_cited_by_work"/>
</rdf:Description>
```

### フラグメント参照の解析

CELLARデータは以下の形式のフラグメント参照を使用します：

- **記事参照**: `A17P1LB` (Article 17, Paragraph 1, Letter B)
- **章参照**: `C108` (Chapter 108)
- **番号参照**: `N 8 95 131` (Numbered paragraphs 8, 95, 131)
- **付録参照**: `I`, `II`, `III` (Annex I, II, III)

## 使用方法

### 1. CLIコマンドを使用した取り込み

```bash
# CELLAR XMLファイルを取り込み
python -m eu_link_db.cli_hierarchical ingest-cellar eu_api_results/cellar_rest_results.xml

# 取り込み統計を表示
python -m eu_link_db.cli_hierarchical cellar-stats
```

### 2. Python APIを使用した取り込み

```python
from eu_link_db.models_hierarchical import get_session
from eu_link_db.cellar_citation_ingester import CellarCitationIngester

# データベースセッションを作成
with get_session("sqlite:///eu_hierarchical.db") as session:
    # インジェスターを作成
    ingester = CellarCitationIngester(session)
    
    # XMLファイルを読み込み
    with open("cellar_data.xml", "r", encoding="utf-8") as f:
        xml_content = f.read()
    
    # データを取り込み
    result = ingester.ingest_cellar_data(xml_content)
    print(f"取り込み結果: {result}")
    
    # 統計を取得
    stats = ingester.get_ingestion_stats()
    print(f"データベース統計: {stats}")
```

### 3. テストの実行

```bash
# テストスクリプトを実行
python -m eu_link_db.test_cellar_ingestion
```

## 主要なクラスとメソッド

### CellarCitationIngester

CELLARデータ取り込みのメインクラスです。

#### 主要メソッド

- `parse_cellar_xml(xml_content: str) -> List[Dict]`: XMLデータを解析して引用関係を抽出
- `ingest_cellar_data(xml_content: str) -> Dict[str, int]`: データベースに取り込み
- `get_ingestion_stats() -> Dict[str, Any]`: 取り込み統計を取得

#### 内部メソッド

- `_parse_fragment_reference(fragment: str) -> Dict`: フラグメント参照を解析
- `_find_target_provision(celex_id: str, fragment: str) -> Optional[str]`: 対象条文を検索
- `_ensure_caselaw_exists(ecli: str, celex_id: str) -> bool`: 判例レコードの存在確認・作成
- `_ensure_regulation_exists(celex_id: str) -> bool`: 法令レコードの存在確認・作成

## データマッピング

### 引用関係のマッピング

CELLARデータの引用関係は以下のように`Citation`テーブルにマッピングされます：

| CELLAR要素 | データベースフィールド | 説明 |
|------------|---------------------|------|
| `owl:annotatedSource` | `regulation_id` | 引用元の法令CELEX ID |
| `owl:annotatedTarget` | `ecli` | 引用先の判例ECLI |
| `j.2:fragment_cited_target` | 各種条文ID | 引用された条文の特定 |

### 条文タイプの判定

フラグメント参照に基づいて、以下の条文タイプが判定されます：

- **Article**: `A17P1LB` → `article_id`
- **Chapter**: `C108` → `chapter_id`
- **Recital**: `R1` → `recital_id`
- **Paragraph**: `A17P1` → `paragraph_id`
- **SubParagraph**: `A17P1LB` → `subparagraph_id`
- **Annex**: `I` → `annex_id`

## エラーハンドリング

### 一般的なエラー

1. **対象条文が見つからない**: フラグメント参照に対応する条文がデータベースに存在しない場合
2. **重複データ**: 同じ引用関係が既に存在する場合
3. **XML解析エラー**: 不正なXML形式の場合

### エラー処理

- 各引用関係の取り込みは個別に処理され、エラーが発生しても他のデータの取り込みは継続
- エラーログが詳細に記録される
- 重複データは自動的にスキップされる

## パフォーマンス最適化

### キャッシュ機能

- 法令レコードのキャッシュ
- 判例レコードのキャッシュ
- 引用関係の重複チェックキャッシュ

### バッチ処理

- 大量のデータを効率的に処理
- メモリ使用量の最適化

## 制限事項

1. **既存データの依存**: 引用関係を作成するには、対象の条文が既にデータベースに存在している必要があります
2. **フラグメント参照の複雑性**: 一部の複雑なフラグメント参照は完全に解析できない場合があります
3. **XMLサイズ**: 非常に大きなXMLファイルの場合、メモリ使用量が増加する可能性があります

## 今後の改善点

1. **より詳細な判例情報**: CELLARから判例の詳細情報（判決日、裁判所等）を取得
2. **フラグメント参照の改善**: より複雑な参照パターンの対応
3. **バッチ処理の最適化**: より効率的な大量データ処理
4. **エラー回復機能**: 部分的な失敗からの回復機能

## トラブルシューティング

### よくある問題

1. **「Could not find target provision」エラー**
   - 対象の条文がデータベースに存在することを確認
   - フラグメント参照の形式を確認

2. **「Failed to create caselaw record」エラー**
   - データベースの権限を確認
   - ECLIの形式を確認

3. **メモリ不足エラー**
   - 大きなXMLファイルの場合は、分割して処理
   - バッチサイズを調整

### ログの確認

詳細なログは以下のレベルで出力されます：
- INFO: 一般的な処理状況
- WARNING: 警告（重複データ等）
- ERROR: エラー（取り込み失敗等）
- DEBUG: デバッグ情報 