"""EUR-Lex NOTICE format parser for extracting caselaw and citation data."""

import xml.etree.ElementTree as ET
import logging
import re
from typing import Dict, List, Optional, Tuple, Any, Set
from datetime import datetime
from urllib.parse import unquote
import requests

from sqlmodel import Session, select
from .models_hierarchical import (
    Regulation, Caselaw, Citation, 
    Chapter, Recital, Article, Paragraph, SubParagraph, Annex
)

logger = logging.getLogger(__name__)

class EurLexNoticeParser:
    """Parse EUR-Lex NOTICE format XML and extract caselaw/citation data."""
    
    def __init__(self, session: Session):
        self.session = session
        
        # Caches for already-seen objects
        self._regulation_cache = {}
        self._caselaw_cache = {}
        self._citation_cache = set()

    def _extract_celex_id(self, uri: str) -> Optional[str]:
        """Extract CELEX ID from URI."""
        if 'celex/' in uri:
            return uri.split('celex/')[-1]
        return None
    
    def _extract_ecli(self, uri: str) -> Optional[str]:
        """Extract ECLI from URI."""
        if 'ecli/' in uri.lower():
            # Handle URL encoded ECLI
            return unquote(uri.split('ecli/')[-1])
        return None
    
    def _parse_fragment_reference(self, fragment: str) -> Dict[str, Any]:
        """Parse fragment reference like 'A67', 'A58P5', 'A17P1LB' etc."""
        result = {
            'type': 'unknown',
            'numbers': [],
            'article': None,
            'paragraph': None,
            'subparagraph': None,
            'annex': None
        }
        
        if not fragment:
            return result
            
        # Handle article references (A67, A58P5, A17P1LB)
        article_match = re.match(r'A(\d+)(?:P(\d+))?(?:L([A-Z]))?', fragment)
        if article_match:
            result['type'] = 'article'
            result['article'] = int(article_match.group(1))
            if article_match.group(2):
                result['paragraph'] = int(article_match.group(2))
            if article_match.group(3):
                result['subparagraph'] = article_match.group(3)
            return result
            
        # Handle chapter references (C108)
        chapter_match = re.match(r'C(\d+)', fragment)
        if chapter_match:
            result['type'] = 'chapter'
            result['numbers'] = [int(chapter_match.group(1))]
            return result
            
        # Handle recital references (R123)
        recital_match = re.match(r'R(\d+)', fragment)
        if recital_match:
            result['type'] = 'recital'
            result['numbers'] = [int(recital_match.group(1))]
            return result
            
        # Handle annex references
        annex_match = re.match(r'([IVX]+)', fragment, re.IGNORECASE)
        if annex_match:
            result['type'] = 'annex'
            result['annex'] = annex_match.group(1).upper()
            return result
            
        return result
    
    def _find_target_provision(self, celex_id: str, fragment: str) -> Optional[str]:
        """Find the target provision ID based on fragment reference."""
        fragment_info = self._parse_fragment_reference(fragment)
        
        if fragment_info['type'] == 'article':
            # Find article
            article_query = select(Article).where(
                Article.celex_id == celex_id,
                Article.article_number == fragment_info['article']
            )
            article = self.session.exec(article_query).first()
            
            if article:
                if fragment_info['paragraph']:
                    # Find paragraph
                    para_query = select(Paragraph).where(
                        Paragraph.article_id == article.article_id,
                        Paragraph.paragraph_number == str(fragment_info['paragraph'])
                    )
                    paragraph = self.session.exec(para_query).first()
                    
                    if paragraph:
                        if fragment_info['subparagraph']:
                            # Find subparagraph
                            subpara_query = select(SubParagraph).where(
                                SubParagraph.paragraph_id == paragraph.paragraph_id,
                                SubParagraph.element_id == fragment_info['subparagraph'].lower()
                            )
                            subparagraph = self.session.exec(subpara_query).first()
                            return subparagraph.subparagraph_id if subparagraph else None
                        return paragraph.paragraph_id
                return article.article_id
                
        elif fragment_info['type'] == 'chapter':
            # Find chapter
            chapter_query = select(Chapter).where(
                Chapter.celex_id == celex_id,
                Chapter.chapter_number == fragment_info['numbers'][0]
            )
            chapter = self.session.exec(chapter_query).first()
            return chapter.chapter_id if chapter else None
            
        elif fragment_info['type'] == 'recital':
            # Find recital
            recital_query = select(Recital).where(
                Recital.celex_id == celex_id,
                Recital.recital_number == fragment_info['numbers'][0]
            )
            recital = self.session.exec(recital_query).first()
            return recital.recital_id if recital else None
            
        elif fragment_info['type'] == 'annex':
            # Find annex
            annex_query = select(Annex).where(
                Annex.celex_id == celex_id,
                Annex.annex_number == fragment_info['annex']
            )
            annex = self.session.exec(annex_query).first()
            return annex.annex_id if annex else None
            
        return None

    def _fetch_caselaw_metadata(self, ecli: str, celex_id: str) -> Dict[str, Any]:
        """Fetch caselaw metadata from EUR-Lex API."""
        url = f"https://eur-lex.europa.eu/legal-content/EN/TXT/XML/?uri=CELEX:{celex_id}"
        
        try:
            r = requests.get(url, headers={"Accept": "application/xml", "User-Agent": "Mozilla/5.0"}, timeout=30)
            if r.status_code != 200:
                logger.warning(f"Failed to fetch caselaw metadata for {celex_id}: HTTP {r.status_code}")
                return {}
            
            # Parse the NOTICE XML to extract title and date
            root = ET.fromstring(r.content)
            
            # Look for title in RESOURCE_LEGAL_TITLE or similar
            title_elem = root.find('.//RESOURCE_LEGAL_TITLE/VALUE')
            if not title_elem:
                title_elem = root.find('.//ID_CELEX')
            
            title = title_elem.text if title_elem is not None else f"Case {celex_id}"
            
            # Look for date
            date_elem = root.find('.//WORK_DATE_DOCUMENT/VALUE')
            decision_date = None
            if date_elem is not None and date_elem.text:
                try:
                    decision_date = datetime.strptime(date_elem.text, "%Y-%m-%d")
                except ValueError:
                    pass
                    
            return {
                "title": title,
                "decision_date": decision_date or datetime(1900, 1, 1),
                "court": "European Court of Justice",
                "ecli": ecli,
            }
            
        except Exception as e:
            logger.exception(f"Failed to fetch caselaw metadata for {celex_id}: {e}")
            return {}
    
    def _ensure_caselaw_exists(self, ecli: str, celex_id: str) -> bool:
        """Ensure caselaw record exists, create if necessary."""
        if ecli in self._caselaw_cache:
            return True
            
        existing = self.session.exec(select(Caselaw).where(Caselaw.ecli == ecli)).first()
        if existing:
            self._caselaw_cache[ecli] = existing
            return True
        
        # Fetch metadata from EUR-Lex API
        metadata = self._fetch_caselaw_metadata(ecli, celex_id)
        if not metadata or not metadata.get("title"):
            logger.warning(f"No metadata for caselaw CELEX {celex_id}; skipping creation of caselaw record for {ecli}")
            return False

        caselaw = Caselaw(
            ecli=ecli,
            court=metadata.get("court", "European Court of Justice"),
            title=metadata["title"],
            summary_text="",
            decision_date=metadata.get("decision_date", datetime(1900, 1, 1)),
            source_url=f"https://eur-lex.europa.eu/legal-content/EN/TXT/?uri=CELEX:{celex_id}"
        )
        
        try:
            self.session.add(caselaw)
            self.session.commit()
            self._caselaw_cache[ecli] = caselaw
            logger.info(f"Created caselaw record for {ecli}")
            return True
        except Exception as e:
            logger.error(f"Failed to create caselaw record for {ecli}: {e}")
            self.session.rollback()
            return False
    
    def _ensure_regulation_exists(self, celex_id: str) -> bool:
        """Ensure regulation record exists."""
        if celex_id in self._regulation_cache:
            return True
            
        existing = self.session.exec(select(Regulation).where(Regulation.celex_id == celex_id)).first()
        if existing:
            self._regulation_cache[celex_id] = existing
            return True
            
        # Create minimal regulation record
        regulation = Regulation(
            celex_id=celex_id,
            title=f"Regulation {celex_id}",
            type="Regulation"
        )
        
        try:
            self.session.add(regulation)
            self.session.commit()
            self._regulation_cache[celex_id] = regulation
            logger.info(f"Created regulation record for {celex_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to create regulation record for {celex_id}: {e}")
            self.session.rollback()
            return False

    def _create_citation_id(self, ecli: str, target_id: str, fragment: str) -> str:
        """Create unique citation ID."""
        return f"{ecli}_{target_id}_{hash(fragment)}"
    
    def _create_citation(self, ecli: str, source_celex: str, fragment: str) -> bool:
        """Create a citation relationship."""
        try:
            # Find target provision
            target_provision_id = self._find_target_provision(source_celex, fragment)
            if not target_provision_id:
                logger.warning(f"Could not find target provision for {source_celex} fragment {fragment}")
                return False
            
            logger.info(f"Found target provision: {target_provision_id}")
            
            # Create citation ID
            citation_id = self._create_citation_id(ecli, target_provision_id, fragment)
            
            # Check for duplicates
            if citation_id in self._citation_cache:
                logger.debug(f"Citation already exists: {citation_id}")
                return True
            
            # Create citation
            citation = Citation(
                ecli=ecli,
                quote_text=f"Cited in case {ecli}",
                confidence=1.0
            )
            
            # Set the appropriate target field based on provision type (check for specific patterns)
            if '-Sub_' in target_provision_id:
                citation.subparagraph_id = target_provision_id
            elif '-Para' in target_provision_id and '-Sub_' not in target_provision_id:
                citation.paragraph_id = target_provision_id
            elif target_provision_id.startswith(source_celex + '-Annex'):
                citation.annex_id = target_provision_id
            elif target_provision_id.startswith(source_celex + '-Ch'):
                citation.chapter_id = target_provision_id
            elif target_provision_id.startswith(source_celex + '-Rec'):
                citation.recital_id = target_provision_id
            elif target_provision_id.startswith(source_celex + '-Art') and '-Para' not in target_provision_id:
                citation.article_id = target_provision_id
            else:
                citation.regulation_id = source_celex
            
            # Save citation
            self.session.add(citation)
            self.session.commit()
            self._citation_cache.add(citation_id)
            
            logger.info(f"Created citation: {ecli} -> {target_provision_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create citation: {e}")
            self.session.rollback()
            return False

    def parse_notice_xml(self, xml_content: str, regulation_celex_id: str) -> Dict[str, int]:
        """Parse EUR-Lex NOTICE XML and extract caselaw references."""
        try:
            root = ET.fromstring(xml_content)
            
            # Find all RESOURCE_LEGAL_INTERPRETED_BY_CASE-LAW elements
            caselaw_elements = root.findall('.//RESOURCE_LEGAL_INTERPRETED_BY_CASE-LAW')
            
            logger.info(f"Found {len(caselaw_elements)} caselaw references")
            
            caselaw_created = 0
            citations_created = 0
            failed = 0
            
            for element in caselaw_elements:
                try:
                    # Extract CELEX ID
                    celex_elem = None
                    for sameas in element.findall('.//SAMEAS'):
                        type_elem = sameas.find('.//TYPE')
                        if type_elem is not None and type_elem.text == 'celex':
                            celex_elem = sameas.find('.//IDENTIFIER')
                            break
                    
                    if celex_elem is None:
                        logger.warning("No CELEX ID found in caselaw element")
                        failed += 1
                        continue
                    
                    celex_id = celex_elem.text
                    
                    # Extract ECLI
                    ecli_elem = None
                    for sameas in element.findall('.//SAMEAS'):
                        type_elem = sameas.find('.//TYPE')
                        if type_elem is not None and type_elem.text == 'ecli':
                            ecli_elem = sameas.find('.//IDENTIFIER')
                            break
                    if ecli_elem is None:
                        logger.warning(f"No ECLI found for case {celex_id}")
                        # Generate ECLI from CELEX ID
                        if celex_id.startswith('6'):
                            year = celex_id[1:5] if len(celex_id) >= 5 else "2000"
                            case_num = celex_id[7:] if len(celex_id) > 7 else "000"
                            ecli = f"ECLI:EU:C:{year}:{case_num}"
                        else:
                            failed += 1
                            continue
                    else:
                        ecli = ecli_elem.text
                    
                    logger.info(f"Processing case: CELEX={celex_id}, ECLI={ecli}")
                    
                    # Ensure caselaw record exists
                    if self._ensure_caselaw_exists(ecli, celex_id):
                        caselaw_created += 1
                    
                    # Extract citation references from ANNOTATION elements
                    annotations = element.findall('.//ANNOTATION/REFERENCE_TO_MODIFIED_LOCATION')
                    
                    for annotation in annotations:
                        fragment = annotation.text
                        if fragment:
                            if self._create_citation(ecli, regulation_celex_id, fragment):
                                citations_created += 1
                            else:
                                failed += 1
                    
                except Exception as e:
                    logger.error(f"Failed to process caselaw element: {e}")
                    failed += 1
            
            logger.info(f"NOTICE parsing complete: {caselaw_created} caselaw records, {citations_created} citations, {failed} failures")
            
            return {
                'caselaw_created': caselaw_created,
                'citations_created': citations_created,
                'failed': failed
            }
            
        except Exception as e:
            logger.error(f"Failed to parse NOTICE XML: {e}")
            return {'caselaw_created': 0, 'citations_created': 0, 'failed': 1}

    def ingest_eurlex_notice_data(self, xml_content: str, regulation_celex_id: str) -> Dict[str, int]:
        """Ingest EUR-Lex NOTICE XML data into the database."""
        logger.info("Starting EUR-Lex NOTICE data ingestion...")
        
        # Ensure regulation exists first
        if not self._ensure_regulation_exists(regulation_celex_id):
            logger.error(f"Failed to ensure regulation exists: {regulation_celex_id}")
            return {'caselaw_created': 0, 'citations_created': 0, 'failed': 1}
        
        # Parse XML and create records
        return self.parse_notice_xml(xml_content, regulation_celex_id)
    
    def get_ingestion_stats(self) -> Dict[str, Any]:
        """Get statistics about ingested data."""
        total_citations = self.session.exec(select(Citation)).all()
        total_caselaw = self.session.exec(select(Caselaw)).all()
        total_regulations = self.session.exec(select(Regulation)).all()
        
        return {
            'total_citations': len(total_citations),
            'total_caselaw': len(total_caselaw),
            'total_regulations': len(total_regulations),
            'citations_by_type': {
                'article': len([c for c in total_citations if c.article_id]),
                'chapter': len([c for c in total_citations if c.chapter_id]),
                'recital': len([c for c in total_citations if c.recital_id]),
                'paragraph': len([c for c in total_citations if c.paragraph_id]),
                'subparagraph': len([c for c in total_citations if c.subparagraph_id]),
                'annex': len([c for c in total_citations if c.annex_id]),
                'regulation': len([c for c in total_citations if c.regulation_id])
            }
        }