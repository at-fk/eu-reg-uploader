"""Ingest structured JSON files from eu_reg_html_analyzer.py into hierarchical database."""

import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from sqlmodel import Session, select

from .models_hierarchical import (
    Annex,
    AnnexSection,
    AnnexSectionItem,
    AnnexTable,
    AnnexTableRow,
    Article,
    Caselaw,
    Chapter,
    Citation,
    Paragraph,
    Recital,
    Regulation,
    SubParagraph,
    get_session,
)


def derive_celex_from_filename(json_path: Path) -> str:
    """
    Derive CELEX ID from structured JSON filename.
    
    Args:
        json_path: Path to structured JSON file
        
    Returns:
        CELEX identifier
    """
    filename = json_path.stem.lower()
    
    # Map common filenames to CELEX IDs
    filename_mapping = {
        "gdpr_structured": "32016R0679",
        "ai act_structured": "32024R1689", 
        "ai_act_structured": "32024R1689",
        "test_structured": "32024R1689",  # Assume test is AI Act
    }
    
    for pattern, celex in filename_mapping.items():
        if pattern in filename:
            return celex
    
    # Default fallback
    return "32016R0679"


def determine_regulation_metadata(celex_id: str, json_data: Dict) -> Dict:
    """
    Determine regulation metadata from CELEX and JSON data.
    
    Args:
        celex_id: CELEX identifier
        json_data: Loaded JSON data
        
    Returns:
        Regulation metadata dictionary
    """
    # Get title from JSON metadata if available
    title = "Unknown Regulation"
    if "metadata" in json_data and "title" in json_data["metadata"]:
        json_title = json_data["metadata"]["title"]
        if json_title == "GDPR":
            title = "General Data Protection Regulation"
        elif json_title == "AI Act":
            title = "Artificial Intelligence Act"
        else:
            title = json_title
    
    # Derive adoption date from CELEX
    adoption_date = None
    if len(celex_id) >= 5:
        try:
            year = int(celex_id[1:5])
            adoption_date = datetime(year, 1, 1)
        except ValueError:
            pass
    
    return {
        "title": title,
        "type": "Regulation",
        "adoption_date": adoption_date
    }


def ingest_regulation_metadata(session: Session, celex_id: str, json_data: Dict) -> None:
    """
    Ingest or update regulation metadata.
    
    Args:
        session: Database session
        celex_id: CELEX identifier
        json_data: JSON data containing metadata
    """
    existing = session.get(Regulation, celex_id)
    if existing:
        return  # Already exists
    
    metadata = determine_regulation_metadata(celex_id, json_data)
    
    regulation = Regulation(
        celex_id=celex_id,
        title=metadata["title"],
        type=metadata["type"],
        adoption_date=metadata["adoption_date"],
        extracted_at=datetime.utcnow()
    )
    
    session.add(regulation)
    session.commit()


def ingest_recitals(session: Session, celex_id: str, recitals_data: List[Dict]) -> int:
    """
    Ingest recitals from structured JSON.
    
    Args:
        session: Database session
        celex_id: CELEX identifier
        recitals_data: List of recital dictionaries
        
    Returns:
        Number of recitals inserted
    """
    inserted_count = 0
    
    for idx, recital_data in enumerate(recitals_data):
        recital_number = int(recital_data.get("recital_number", idx + 1))
        recital_id = f"{celex_id}-Rec{recital_number}"
        
        # Check if already exists
        existing = session.get(Recital, recital_id)
        if existing:
            continue
        
        # Get HTML ID from metadata if available
        html_id = None
        if "metadata" in recital_data and "id" in recital_data["metadata"]:
            html_id = recital_data["metadata"]["id"]
        
        recital = Recital(
            recital_id=recital_id,
            celex_id=celex_id,
            recital_number=recital_number,
            text=recital_data.get("text", ""),
            order_index=idx,
            html_id=html_id
        )
        
        session.add(recital)
        inserted_count += 1
    
    session.commit()
    return inserted_count


def ingest_chapters(session: Session, celex_id: str, chapters_data: List[Dict]) -> int:
    """
    Ingest chapters from structured JSON.
    
    Args:
        session: Database session
        celex_id: CELEX identifier
        chapters_data: List of chapter dictionaries
        
    Returns:
        Number of chapters inserted
    """
    inserted_count = 0
    
    for chapter_data in chapters_data:
        chapter_number = chapter_data.get("chapter_number", 1)
        chapter_id = f"{celex_id}-Ch{chapter_number}"
        
        # Check if already exists
        existing = session.get(Chapter, chapter_id)
        if existing:
            continue
        
        chapter = Chapter(
            chapter_id=chapter_id,
            celex_id=celex_id,
            chapter_number=chapter_number,
            title=chapter_data.get("title", ""),
            order_index=chapter_data.get("order_index", chapter_number)
        )
        
        session.add(chapter)
        inserted_count += 1
    
    session.commit()
    return inserted_count


def create_article_to_chapter_mapping(chapters_data: List[Dict]) -> Dict[int, int]:
    """
    Create mapping from article number to chapter number.
    
    Args:
        chapters_data: List of chapter dictionaries from JSON
        
    Returns:
        Dictionary mapping article_number -> chapter_number
    """
    article_to_chapter = {}
    
    for chapter_data in chapters_data:
        chapter_number = chapter_data.get("chapter_number", 1)
        article_numbers = chapter_data.get("article_numbers", [])
        
        for article_num in article_numbers:
            article_to_chapter[article_num] = chapter_number
    
    return article_to_chapter


def find_chapter_for_article(celex_id: str, article_number: int, article_to_chapter: Dict[int, int]) -> Optional[str]:
    """
    Find which chapter contains a given article.
    
    Args:
        celex_id: CELEX identifier
        article_number: Article number to find
        article_to_chapter: Mapping from article number to chapter number
        
    Returns:
        Chapter ID if found, None otherwise
    """
    chapter_number = article_to_chapter.get(article_number)
    if chapter_number:
        return f"{celex_id}-Ch{chapter_number}"
    
    return None


def ingest_articles(session: Session, celex_id: str, articles_data: List[Dict], article_to_chapter: Dict[int, int]) -> int:
    """
    Ingest articles from structured JSON.
    
    Args:
        session: Database session
        celex_id: CELEX identifier
        articles_data: List of article dictionaries
        
    Returns:
        Number of articles inserted
    """
    inserted_count = 0
    
    for article_data in articles_data:
        article_number = article_data.get("article_number", 1)
        article_id = f"{celex_id}-Art{article_number}"
        
        # Check if already exists
        existing = session.get(Article, article_id)
        if existing:
            continue
        
        # Find associated chapter
        chapter_id = find_chapter_for_article(celex_id, article_number, article_to_chapter)
        
        # Determine if this is a definitions article
        is_definitions = False
        if "metadata" in article_data:
            is_definitions = article_data["metadata"].get("is_definitions", False)
        
        article = Article(
            article_id=article_id,
            celex_id=celex_id,
            chapter_id=chapter_id,
            article_number=article_number,
            title=article_data.get("title", ""),
            content_full=article_data.get("content_full", ""),
            order_index=article_data.get("order_index", article_number),
            is_definitions=is_definitions
        )
        
        session.add(article)
        inserted_count += 1
        
        # Ingest paragraphs for this article
        if "paragraphs" in article_data:
            ingest_paragraphs(session, article_id, article_data["paragraphs"])
    
    session.commit()
    return inserted_count


def ingest_paragraphs(session: Session, article_id: str, paragraphs_data: List[Dict]) -> int:
    """
    Ingest paragraphs for an article.
    
    Args:
        session: Database session
        article_id: Parent article ID
        paragraphs_data: List of paragraph dictionaries
        
    Returns:
        Number of paragraphs inserted
    """
    inserted_count = 0
    
    for idx, para_data in enumerate(paragraphs_data):
        para_number = para_data.get("paragraph_number")
        if para_number is None:
            para_number = str(idx + 1)
        paragraph_id = f"{article_id}-Para{para_number}"
        
        # Check if already exists
        existing = session.get(Paragraph, paragraph_id)
        if existing:
            continue
        
        # Get metadata counts
        metadata = para_data.get("metadata", {})
        chapeau_count = metadata.get("chapeau_count", 0)
        subparagraph_count = metadata.get("subparagraph_count", 0)
        
        paragraph = Paragraph(
            paragraph_id=paragraph_id,
            article_id=article_id,
            paragraph_number=para_number,
            content_full=para_data.get("content_full", ""),
            order_index=idx,
            chapeau_count=chapeau_count,
            subparagraph_count=subparagraph_count
        )
        
        session.add(paragraph)
        inserted_count += 1
        
        # Ingest ordered contents as sub-paragraphs
        if "ordered_contents" in para_data:
            ingest_subparagraphs(session, paragraph_id, para_data["ordered_contents"])
    
    session.commit()
    return inserted_count


def ingest_subparagraphs(session: Session, paragraph_id: str, contents_data: List[Dict]) -> int:
    """
    Ingest sub-paragraphs from ordered contents.
    
    Args:
        session: Database session
        paragraph_id: Parent paragraph ID
        contents_data: List of ordered content dictionaries
        
    Returns:
        Number of sub-paragraphs inserted
    """
    inserted_count = 0
    
    for content_data in contents_data:
        content_type = content_data.get("type", "unknown")
        order_index = content_data.get("order_index", 1)
        
        # Create unique ID based on type and order
        if content_type == "subparagraph":
            element_id = content_data.get("element_id", content_data.get("subparagraph_id", str(order_index)))
            subparagraph_id = f"{paragraph_id}-Sub_{element_id}"
        else:
            subparagraph_id = f"{paragraph_id}-{content_type}_{order_index}"
            element_id = content_type
        
        # Check if already exists  
        existing = session.get(SubParagraph, subparagraph_id)
        if existing:
            continue
        
        subparagraph = SubParagraph(
            subparagraph_id=subparagraph_id,
            paragraph_id=paragraph_id,
            element_id=element_id,
            content=content_data.get("content", ""),
            order_index=order_index,
            content_type=content_type
        )
        
        session.add(subparagraph)
        inserted_count += 1
    
    session.commit()
    return inserted_count


def ingest_annexes(session: Session, celex_id: str, annexes_data: List[Dict]) -> Dict[str, int]:
    """
    Ingest hierarchical annexes from structured JSON.
    
    Args:
        session: Database session
        celex_id: CELEX identifier
        annexes_data: List of annex dictionaries
        
    Returns:
        Dictionary with counts of inserted items by type
    """
    counts = {"annexes": 0, "sections": 0, "items": 0, "tables": 0, "rows": 0}
    
    for annex_data in annexes_data:
        annex_number = annex_data.get("annex_id", "I")
        annex_id = f"{celex_id}-Annex{annex_number}"
        
        # Check if annex already exists
        existing = session.get(Annex, annex_id)
        if existing:
            continue
        
        # Create annex
        annex = Annex(
            annex_id=annex_id,
            celex_id=celex_id,
            annex_number=annex_number,
            title=annex_data.get("title", f"ANNEX {annex_number}"),
            subtitle=annex_data.get("subtitle"),
            order_index=annex_data.get("order_index", 1)
        )
        
        session.add(annex)
        counts["annexes"] += 1
        
        # Ingest sections
        if "sections" in annex_data:
            section_counts = ingest_annex_sections(session, annex_id, annex_data["sections"])
            counts["sections"] += section_counts["sections"]
            counts["items"] += section_counts["items"]
            counts["tables"] += section_counts["tables"]
            counts["rows"] += section_counts["rows"]
    
    session.commit()
    return counts


def ingest_annex_sections(session: Session, annex_id: str, sections_data: List[Dict]) -> Dict[str, int]:
    """
    Ingest annex sections.
    
    Args:
        session: Database session
        annex_id: Parent annex ID
        sections_data: List of section dictionaries
        
    Returns:
        Dictionary with counts of inserted items by type
    """
    counts = {"sections": 0, "items": 0, "tables": 0, "rows": 0}
    
    for idx, section_data in enumerate(sections_data):
        section_number = section_data.get("section_id", str(idx + 1))
        section_id = f"{annex_id}-Sec{section_number}"
        
        # Check if section already exists
        existing = session.get(AnnexSection, section_id)
        if existing:
            continue
        
        # Create section
        section = AnnexSection(
            section_id=section_id,
            annex_id=annex_id,
            section_number=section_number,
            heading=section_data.get("heading", ""),
            list_type=section_data.get("list_type"),
            order_index=idx
        )
        
        session.add(section)
        counts["sections"] += 1
        
        # Ingest section items
        if "items" in section_data and section_data["items"]:
            item_count = ingest_annex_section_items(session, section_id, section_data["items"])
            counts["items"] += item_count
        
        # Ingest tables
        if "tables" in section_data and section_data["tables"]:
            table_counts = ingest_annex_tables(session, section_id, section_data["tables"])
            counts["tables"] += table_counts["tables"]
            counts["rows"] += table_counts["rows"]
    
    return counts


def ingest_annex_section_items(session: Session, section_id: str, items_data: List[str]) -> int:
    """
    Ingest items within an annex section.
    
    Args:
        session: Database session
        section_id: Parent section ID
        items_data: List of item strings
        
    Returns:
        Number of items inserted
    """
    inserted_count = 0
    
    for idx, item_content in enumerate(items_data):
        item_id = f"{section_id}-Item{idx + 1}"
        
        # Check if item already exists
        existing = session.get(AnnexSectionItem, item_id)
        if existing:
            continue
        
        # Create item
        item = AnnexSectionItem(
            item_id=item_id,
            section_id=section_id,
            content=item_content,
            order_index=idx
        )
        
        session.add(item)
        inserted_count += 1
    
    return inserted_count


def ingest_annex_tables(session: Session, section_id: str, tables_data: List[Dict]) -> Dict[str, int]:
    """
    Ingest tables within an annex section.
    
    Args:
        session: Database session
        section_id: Parent section ID
        tables_data: List of table dictionaries
        
    Returns:
        Dictionary with counts of inserted items by type
    """
    counts = {"tables": 0, "rows": 0}
    
    for idx, table_data in enumerate(tables_data):
        table_id = f"{section_id}-Tab{idx + 1}"
        
        # Check if table already exists
        existing = session.get(AnnexTable, table_id)
        if existing:
            continue
        
        # Create table
        table = AnnexTable(
            table_id=table_id,
            section_id=section_id,
            caption=table_data.get("caption"),
            order_index=idx
        )
        
        session.add(table)
        counts["tables"] += 1
        
        # Ingest table rows
        if "rows" in table_data and table_data["rows"]:
            row_count = ingest_annex_table_rows(session, table_id, table_data["rows"])
            counts["rows"] += row_count
    
    return counts


def ingest_annex_table_rows(session: Session, table_id: str, rows_data: List[Dict]) -> int:
    """
    Ingest rows within an annex table.
    
    Args:
        session: Database session
        table_id: Parent table ID
        rows_data: List of row dictionaries
        
    Returns:
        Number of rows inserted
    """
    import json
    
    inserted_count = 0
    
    for idx, row_data in enumerate(rows_data):
        row_id = f"{table_id}-Row{idx + 1}"
        
        # Check if row already exists
        existing = session.get(AnnexTableRow, row_id)
        if existing:
            continue
        
        # Create row
        row = AnnexTableRow(
            row_id=row_id,
            table_id=table_id,
            row_data=json.dumps(row_data),
            order_index=idx
        )
        
        session.add(row)
        inserted_count += 1
    
    return inserted_count


def ingest_structured_json_file(json_path: Path, session: Session) -> Dict[str, int]:
    """
    Ingest complete structured JSON file into hierarchical database.
    
    Args:
        json_path: Path to structured JSON file
        session: Database session
        
    Returns:
        Dictionary with counts of inserted items by type
    """
    if not json_path.exists():
        raise FileNotFoundError(f"JSON file not found: {json_path}")
    
    with open(json_path, "r", encoding="utf-8") as f:
        json_data = json.load(f)
    
    celex_id = derive_celex_from_filename(json_path)
    
    # Ingest regulation metadata
    ingest_regulation_metadata(session, celex_id, json_data)
    
    counts = {"regulation": 1}
    
    # Ingest recitals
    if "recitals" in json_data:
        counts["recitals"] = ingest_recitals(session, celex_id, json_data["recitals"])
    
    # Ingest chapters
    if "chapters" in json_data:
        counts["chapters"] = ingest_chapters(session, celex_id, json_data["chapters"])
    
    # Create article-to-chapter mapping
    article_to_chapter = {}
    if "chapters" in json_data:
        article_to_chapter = create_article_to_chapter_mapping(json_data["chapters"])
    
    # Ingest articles (which includes paragraphs and sub-paragraphs)
    if "articles" in json_data:
        counts["articles"] = ingest_articles(session, celex_id, json_data["articles"], article_to_chapter)
    
    # Ingest annexes
    if "annexes" in json_data:
        annex_counts = ingest_annexes(session, celex_id, json_data["annexes"])
        counts.update(annex_counts)
    else:
        counts.update({"annexes": 0, "sections": 0, "items": 0, "tables": 0, "rows": 0})
    
    return counts