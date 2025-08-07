# EU Regulation Uploader

A comprehensive system for ingesting, analyzing, and managing EU law hierarchical data with integrated case law citation analysis.

## 🎯 Overview

This system processes EU regulations and their associated case law data from multiple sources:
- **Structured JSON files** containing regulation hierarchy (articles, paragraphs, subparagraphs, etc.)
- **EUR-Lex NOTICE format XML** containing case law interpretations and citations
- **RDF/XML format** from CELLAR REST API

The system ensures **no dummy data** is ever generated - all data comes from official EU sources only.

## 🏗️ Architecture

### Core Components

1. **Hierarchical Data Model** (`models_hierarchical.py`)
   - SQLModel-based schema for EU law structure
   - Supports regulations, chapters, recitals, articles, paragraphs, subparagraphs, annexes
   - Integrated case law and citation relationship modeling

2. **Multi-Format Parsers**
   - **JSON Ingester** (`ingest_structured_json.py`) - Processes structured regulation data
   - **EUR-Lex NOTICE Parser** (`eurlex_notice_parser.py`) - Extracts case law from EUR-Lex API
   - **CELLAR RDF/XML Parser** (`cellar_citation_ingester.py`) - Processes CELLAR REST API data

3. **Batch Processing** (`batch_processor.py`)
   - Automatic format detection and processing
   - Handles multiple regulations simultaneously
   - Progress tracking and error reporting

## 📊 Database Schema

### Regulation Hierarchy
```
Regulation
├── Chapters
├── Recitals  
├── Articles
│   └── Paragraphs
│       └── SubParagraphs
└── Annexes
    ├── AnnexSections
    │   └── AnnexSectionItems
    └── AnnexTables
        └── AnnexTableRows
```

### Case Law Integration
```
Caselaw (ECLI, court, title, decision_date)
├── Citations → Articles
├── Citations → Paragraphs  
├── Citations → SubParagraphs
├── Citations → Chapters
├── Citations → Recitals
└── Citations → Annexes
```

### Staged Implementation Management
```
StagedImplementation
├── effective_date (施行日)
├── implementation_type (施行タイプ)
├── scope_description (適用範囲説明)
├── article_references (根拠条項：Article 113関連)
├── affected_articles (影響条項：将来拡張用)
└── is_main_application (メイン適用日フラグ)
```

## 🚀 Installation

### Prerequisites
- Python 3.8+
- SQLite (or other SQLModel-compatible database)

### Setup
```bash
# Clone repository
git clone https://github.com/at-fk/eu-reg-uploader.git
cd eu-reg-uploader5

# Create virtual environment
python -m venv venv
source venv/bin/activate  # macOS/Linux
# or
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt

# Verify installation
python -m eu_link_db.cli_hierarchical --help
```

### Key Dependencies
- `sqlmodel>=0.0.24` - Database ORM
- `click>=8.0.0` - CLI framework  
- `rich>=13.0.0` - Terminal UI
- `requests>=2.28.0` - HTTP client for EUR-Lex API
- `pytest>=7.0.0` - Testing framework

## 📖 Usage

### Command Line Interface

#### 0. EDPB Document Collection and Processing

Collect and process GDPR-related guidelines and recommendations from the European Data Protection Board (EDPB):

```bash
# Collect all EDPB documents (Guidelines, Recommendations, Article 29 WP documents)
python collect_final_complete_edpb.py

# Process EDPB documents with AI analysis
python edpb_cli.py process-batch edpb_final_complete_collection/

# Check processing status
python edpb_cli.py status

# List processed guidelines
python edpb_cli.py list-guidelines

# Show detailed information for specific guideline
python edpb_cli.py show-detail 1
```

**Collection Features:**
- **Complete coverage**: All pages (0-6) with 59 documents
- **Article 29 Working Party integration**: Automatically detects and downloads WP documents
- **Smart deduplication**: Prevents filename conflicts with automatic numbering
- **Enhanced titles**: Article 29 documents get improved titles (e.g., "WP29 - Guidelines on transparency")
- **Multiple document types**: Guidelines, Recommendations, Transparency docs, DPO guides, DPIA materials
- **JSON logging**: Complete metadata and statistics saved for each collection run

**Processing Features:**
- **AI-powered analysis**: Uses Gemini 2.5 Pro for summary generation
- **Vector embeddings**: 768-dimensional embeddings via gemini-embedding-001
- **Structured metadata**: Automatic extraction of titles, versions, dates, related articles
- **Full-text indexing**: Complete PDF text extraction and chunking
- **SQLite storage**: Integration with eu_hierarchical.db database

**Document Types Collected:**
- **EDPB Guidelines** (e.g., Guidelines 1/2024 on legitimate interests)
- **EDPB Recommendations** (e.g., Recommendations on BCR)
- **Article 29 Working Party documents** (legacy guidelines with "WP29 -" prefix)
- **Special documents** (Transparency, Data Protection Officer, DPIA guides)

#### 1. Basic Data Ingestion

```bash
# Ingest structured regulation JSON
python -m eu_link_db.cli_hierarchical ingest gdpr_structured.json

# Ingest EUR-Lex NOTICE format case law (recommended)
python -m eu_link_db.cli_hierarchical ingest-eurlex gdpr.xml 32016R0679

# Ingest CELLAR RDF/XML format (alternative)
python -m eu_link_db.cli_hierarchical ingest-cellar cellar_data.xml
```

#### 2. Batch Processing (Recommended)
```bash
# Process all files in directory (auto-detects formats)
python -m eu_link_db.cli_hierarchical batch-process eu_link_db/

# Example directory structure:
# eu_link_db/
# ├── gdpr_structured.json      # Regulation structure
# ├── gdpr.xml                  # EUR-Lex NOTICE format case law
# ├── ai_act_structured.json    # AI Act structure  
# └── ai_act.xml                # AI Act case law (when available)
```

#### 3. Database Management
```bash
# Check database status
python -m eu_link_db.cli_hierarchical status

# List all regulations
python -m eu_link_db.cli_hierarchical list-regulations

# Show specific regulation details
python -m eu_link_db.cli_hierarchical show-regulation 32016R0679

# View citation statistics
python -m eu_link_db.cli_hierarchical cellar-stats
```

#### 4. Staged Implementation Management
```bash
# Update regulation metadata from XML (adoption date, validity, etc.)
python xml_to_db_updater.py eu_link_db/gdpr.xml 32016R0679
python xml_to_db_updater.py eu_link_db/ai_act.xml 32024R1689

# Extract and manage staged implementation schedules
python staged_implementation_cli.py show 32024R1689        # Show AI Act implementation schedule
python staged_implementation_cli.py show 32016R0679        # Show GDPR implementation schedule
python staged_implementation_cli.py overview               # Show current/upcoming implementations

# Load staged implementation from XML
python staged_implementation_cli.py load eu_link_db/ai_act.xml 32024R1689
```

## 📝 Data Sources

### 1. Structured JSON Format
Hierarchical regulation data with the following structure:
```json
{
  "regulation": {
    "celex_id": "32016R0679",
    "title": "General Data Protection Regulation"
  },
  "chapters": [...],
  "articles": [
    {
      "article_number": 17,
      "paragraphs": [
        {
          "paragraph_number": "1", 
          "subparagraphs": [
            {
              "element_id": "b",
              "text": "the personal data are no longer necessary..."
            }
          ]
        }
      ]
    }
  ]
}
```

### 2. EUR-Lex NOTICE Format (Primary Source)
XML data from EUR-Lex API containing case law interpretations:
```bash
# Download GDPR case law from EUR-Lex
GET https://eur-lex.europa.eu/legal-content/EN/TXT/XML/?uri=CELEX:32016R0679

# Contains RESOURCE_LEGAL_INTERPRETED_BY_CASE-LAW sections with:
# - CELEX IDs and ECLIs of citing cases
# - REFERENCE_TO_MODIFIED_LOCATION (A67, A58P5, A17P1LB, etc.)
# - Case metadata and decision dates
```

### 3. CELLAR RDF/XML Format (Alternative)
RDF/XML format from CELLAR REST API with detailed citation relationships.

### 4. Amendment History and Staged Implementation
XML-based system for tracking regulation amendments and staged implementation schedules:
```bash
# Example: AI Act staged implementation from Article 113
# 2024-08-01: Legal framework entry into force (Article 113)
# 2025-02-02: Prohibited AI practices start (Article 113(a))
# 2025-08-02: General-purpose AI model obligations (Article 113(b))
# 2026-08-02: Main regulation provisions (Article 113) [Primary application date]
# 2027-08-02: Remaining provisions (Article 113(c))

# Tracks:
# - Legal basis articles (Article 113, 99, 97 etc.)
# - Amendment history (RESOURCE_LEGAL_AMENDED_BY_ACT)
# - Consolidated version information
# - Time series metadata (adoption, entry into force, end of validity)
```

## 🎯 Key Features

### Smart Citation Targeting
The system correctly assigns citations to the most specific provision level:
- `A17P1LB` → Article 17, Paragraph 1, Letter B → `subparagraph_id`
- `A58P5` → Article 58, Paragraph 5 → `paragraph_id`  
- `A67` → Article 67 → `article_id`

This ensures citations are linked to the exact legal provision referenced by the court.

### EDPB Document Collection
- **Complete coverage**: Collects all 59 GDPR-related documents from EDPB website
- **Article 29 integration**: Seamlessly handles legacy Working Party documents
- **Smart deduplication**: Prevents filename conflicts with automatic numbering
- **Enhanced titles**: Article 29 documents get "WP29 -" prefixes for clarity
- **Multi-page support**: Processes all website pages (0-6) automatically
- **Format detection**: Handles Guidelines, Recommendations, and special documents

### Multi-Format Support
- **Auto-detection**: Automatically detects XML format (RDF/XML vs NOTICE)
- **Unified processing**: Single batch command handles multiple formats
- **Error handling**: Graceful failure handling with detailed logging

### Data Integrity
- **No dummy data**: Only processes official EU-provided data sources
- **Deduplication**: Prevents duplicate case law and citation records
- **Validation**: Ensures all relationships link to existing provisions

### Staged Implementation Tracking
- **XML-based extraction**: Parses complex implementation schedules from official EU XML
- **Legal basis tracking**: Identifies which articles (113, 99, 97) define implementation dates
- **Amendment history**: Tracks all amendments, corrections, and consolidations
- **Version management**: Manages consolidated versions with date-specific identifiers

## 📈 Processing Results

### Complete Dataset (as of 2025)
```
📊 Database Statistics:
├── Regulations: 2 (GDPR + AI Act)
│   ├── GDPR (32016R0679): 99 articles, 908 paragraphs, 1,771 subparagraphs
│   └── AI Act (32024R1689): 113 articles with annexes
├── Case Law: 69 unique ECJ cases
├── Citations: 189 citation relationships
│   ├── Article-level: 55 citations
│   ├── Paragraph-level: 95 citations  
│   └── Subparagraph-level: 39 citations
├── EDPB Guidelines: 34+ processed documents
│   ├── AI-generated summaries: 34+ detailed summaries
│   ├── Text chunks: 3,000+ searchable chunks
│   ├── Vector embeddings: 768-dimensional embeddings for semantic search
│   └── Document types: Guidelines, Recommendations, WP29 documents
└── Staged Implementation: 15 implementation phases
    ├── AI Act: 12 phases (2024-2031)
    └── GDPR: 3 phases (2016-2020)
```

### Staged Implementation Examples
```
📅 AI Act Implementation Schedule:
├── 2024-08-01: Legal framework entry (Article 113)
├── 2025-02-02: Prohibited AI practices (Article 113(a))
├── 2025-08-02: AI model obligations (Article 113(b))
├── 2026-08-02: Main provisions [PRIMARY] (Article 113)
└── 2027-08-02: Final provisions (Article 113(c))

📅 GDPR Implementation Schedule:
├── 2016-05-24: Entry into force (Article 99)
├── 2018-05-25: Full application [PRIMARY] (Article 99)
└── 2020-05-25: Transitional provisions (Article 97)
```

### Recent Case Law Examples
- **ECLI:EU:C:2024:1051** (Dec 2024) → Multiple GDPR provisions
- **ECLI:EU:C:2024:988** (Nov 2024) → Article 14, Paragraph 5, Letter C
- **ECLI:EU:C:2024:858** (Oct 2024) → Articles 67, 58, 61, 57

## 🔧 Configuration

### Database Configuration
```bash
# Default SQLite (recommended for development)
DATABASE_URL="sqlite:///eu_hierarchical.db"

# PostgreSQL example (for production)
DATABASE_URL="postgresql://user:pass@localhost/eu_law"
```

### Logging Configuration
```python
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('eu_link_db')
```

## 🧪 Testing

### Run Tests
```bash
# Run all tests
pytest tests/ -v

# Test specific functionality
pytest tests/test_ingest_json.py -v
pytest tests/test_ingest_cellar.py -v

# Test with real data (integration test)
python -m eu_link_db.cli_hierarchical batch-process test_batch/
```

### Data Quality Tests
The test suite ensures:
- No dummy data is ever created
- All data comes from external sources only
- Citation relationships are correctly established
- Fragment parsing works accurately

## 📁 Project Structure

```
eu-reg-uploader5/
├── eu_link_db/                         # Core hierarchical database package
│   ├── models_hierarchical.py          # SQLModel database schema with staged implementation
│   ├── ingest_structured_json.py       # JSON regulation ingestion
│   ├── eurlex_notice_parser.py         # EUR-Lex NOTICE format parser
│   ├── cellar_citation_ingester.py     # CELLAR RDF/XML parser
│   ├── batch_processor.py              # Multi-format batch processing
│   ├── amendment_parser.py             # Amendment history extraction
│   ├── staged_implementation_parser.py # Staged implementation schedule parser
│   └── cli_hierarchical.py             # Command-line interface
├── edpb_guideline_collector.py         # EDPB document collection system
├── collect_final_complete_edpb.py      # Complete EDPB collection script
├── edpb_processor.py                   # EDPB PDF processing with AI analysis
├── edpb_cli.py                         # EDPB processing command-line interface
├── edpb_final_complete_collection/     # Downloaded EDPB PDF documents
├── xml_to_db_updater.py                # XML metadata to database updater
├── staged_implementation_cli.py        # CLI for staged implementation management
├── tests/                              # Comprehensive test suite
│   ├── test_ingest_json.py             # JSON ingestion tests
│   ├── test_ingest_cellar.py           # Case law ingestion tests
│   └── test_*.py                       # Additional test modules
├── test_batch/                         # Sample test data
│   ├── test_structured.json            # Sample regulation data
│   └── test.xml                        # Sample RDF/XML citation data
├── requirements.txt                    # Python dependencies (includes Gemini AI)
├── README.md                           # This documentation
└── legacy files...                     # Original regulation uploader components
```

## 🤝 Contributing

### Adding New Regulations

1. **Create structured JSON** following the established schema
2. **Obtain EUR-Lex XML** using the CELEX ID:
   ```bash
   GET https://eur-lex.europa.eu/legal-content/EN/TXT/XML/?uri=CELEX:{CELEX_ID}
   ```
3. **Add CELEX ID mapping** in `batch_processor.py`:
   ```python
   def _get_celex_id(self, regulation_name: str) -> Optional[str]:
       name_mapping = {
           'gdpr': '32016R0679',
           'ai_act': '32024R1689',
           'new_regulation': '32025R0123',  # Add here
       }
   ```
4. **Process regulation and staged implementation**:
   ```bash
   # Ingest structured data
   python -m eu_link_db.cli_hierarchical batch-process data/
   
   # Update regulation metadata and extract staged implementation
   python xml_to_db_updater.py data/new_regulation.xml 32025R0123
   
   # Verify staged implementation schedule
   python staged_implementation_cli.py show 32025R0123
   ```

### Parser Extensions
- Add new XML format parsers in separate modules
- Integrate with batch processor format detection
- Follow existing error handling patterns
- Maintain the no-dummy-data principle

## 🔍 Troubleshooting

### Common Issues

#### "No CELEX ID found in caselaw element"
**Cause**: XML format detection issue or malformed XML
**Solution**: 
- Verify XML format is NOTICE (`<NOTICE>`) or RDF/XML (`<rdf:RDF>`)
- Check file encoding (should be UTF-8)
- Ensure SAMEAS elements contain proper identifiers

#### "Could not find target provision for fragment"
**Cause**: Fragment reference not found in structured data
**Solution**:
- Verify structured JSON was ingested first
- Check fragment reference format (A17P1LB, A58P5, etc.)
- Ensure CELEX IDs match between JSON and XML

#### Citations appear at article level instead of subparagraph
**Note**: This may be correct behavior - the system assigns to the most specific available level. If subparagraph data doesn't exist in the structured JSON, citations will correctly fall back to paragraph or article level.

### Debug Mode
```bash
# Enable verbose logging
export LOG_LEVEL=DEBUG
python -m eu_link_db.cli_hierarchical batch-process eu_link_db/

# Check specific regulation details
python -m eu_link_db.cli_hierarchical show-regulation 32016R0679

# View database schema
sqlite3 eu_hierarchical.db ".schema"
```

## 📊 Performance Metrics

### Processing Speed
- **JSON ingestion**: ~100 articles/second
- **Case law processing**: ~50 cases/second  
- **Citation creation**: ~200 citations/second
- **Batch processing**: 2 regulations (GDPR + AI Act) in ~30 seconds

### Storage Requirements
- **GDPR complete dataset**: ~2MB SQLite database
- **Memory usage**: <100MB during processing
- **Database growth**: ~1MB per major regulation

## ⚖️ Legal and Compliance

### Data Sources
- **EUR-Lex**: Official EU legal database (public access)
- **CELLAR**: Publications Office of the EU (public API)
- **ECJ Case Law**: European Court of Justice decisions (public domain)

### Usage Guidelines
- This system processes public EU legal data
- Ensure compliance with EUR-Lex terms of service
- Case law data is sourced from official EU institutions
- No proprietary or restricted content is included

### Data Quality Assurance
- **Official sources only**: No synthetic or generated content
- **Strict validation**: All citations verified against actual legal provisions
- **Audit trail**: Complete logging of data sources and processing steps

## 🙏 Acknowledgments

- **EUR-Lex** for providing comprehensive EU legal data APIs
- **CELLAR** for detailed case law citation information  
- **European Court of Justice** for structured case law data
- **Publications Office of the European Union** for maintaining legal metadata

---

**For technical support or questions about this system, please refer to the documentation above or create an issue in the project repository.**

**For legal questions about EU law content, please consult the official EUR-Lex portal: https://eur-lex.europa.eu/**