"""CELLAR REST API citation data ingester for eu_hierarchical.db."""

import xml.etree.ElementTree as ET
import logging
import re
from typing import Dict, List, Optional, Tuple, Any, Set
from datetime import datetime
from urllib.parse import unquote

import requests  # HTTP fetch for CELLAR

from sqlmodel import Session, select
from .models_hierarchical import (
    Regulation, Caselaw, Citation, 
    Chapter, Recital, Article, Paragraph, SubParagraph, Annex
)

logger = logging.getLogger(__name__)

class CellarCitationIngester:
    """Ingest CELLAR REST API citation data into eu_hierarchical.db."""
    
    def __init__(self, session: Session):
        self.session = session
        self.namespaces = {
            'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'owl': 'http://www.w3.org/2002/07/owl#',
            'j.0': 'http://publications.europa.eu/ontology/cdm#',
            'j.2': 'http://publications.europa.eu/ontology/annotation#'
        }
        

        # Caches for already-seen objects
        self._regulation_cache = {}
        self._caselaw_cache = {}
        self._citation_cache = set()

    def _download_caselaw_metadata(self, celex_id: str) -> Dict[str, Any]:
        """Download RDF/XML of a caselaw CELEX and extract basic metadata."""
        url = f"https://publications.europa.eu/resource/celex/{celex_id}?format=application/rdf+xml"
        try:
            r = requests.get(url, headers={"Accept": "application/rdf+xml", "User-Agent": "Mozilla/5.0"}, timeout=30)
            if r.status_code != 200:
                return {}
            import xml.etree.ElementTree as ET
            NS = {
                'cdm': 'http://publications.europa.eu/ontology/cdm#',
                'dct': 'http://purl.org/dc/terms/'
            }
            root = ET.fromstring(r.content)
            title_elem = root.find('.//cdm:resource_legal_title', NS)
            date_elem = root.find('.//cdm:work_date_document', NS)
            court_elem = root.find('.//dct:title', NS)
            title = title_elem.text if title_elem is not None else ""
            decision_date = None
            if date_elem is not None and date_elem.text:
                try:
                    decision_date = datetime.fromisoformat(date_elem.text)
                except ValueError:
                    pass
            ecli_elem = root.find('.//cdm:case-law_ecli', NS)
            ecli_val = ecli_elem.text if ecli_elem is not None else None
            court = court_elem.text if court_elem is not None else ""
            return {
                "title": title,
                "decision_date": decision_date,
                "court": court,
                "ecli": ecli_val,
            }
        except Exception:
            logger.exception("Failed to fetch caselaw metadata for %s", celex_id)
            return {}


        
    def _extract_celex_id(self, uri: str) -> Optional[str]:
        """Extract CELEX ID from URI."""
        if 'celex/' in uri:
            return uri.split('celex/')[-1]
        return None
    
    def _extract_ecli(self, uri: str) -> Optional[str]:
        """Extract ECLI from URI."""
        if 'ecli/' in uri:
            return unquote(uri.split('ecli/')[-1])
        return None
    
    def _parse_fragment_reference(self, fragment: str) -> Dict[str, Any]:
        """Parse fragment reference like 'N 8 95 131' or 'C108'."""
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
            
        # Handle numbered references (N 8 95 131)
        if fragment.startswith('N '):
            result['type'] = 'numbered'
            result['numbers'] = [int(x) for x in fragment[2:].split() if x.isdigit()]
            return result
            
        # Handle article references (A17P1LB)
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
            
        elif fragment_info['type'] == 'annex':
            # Find annex
            annex_query = select(Annex).where(
                Annex.celex_id == celex_id,
                Annex.annex_number == fragment_info['annex']
            )
            annex = self.session.exec(annex_query).first()
            return annex.annex_id if annex else None
            
        return None
    
    def _ensure_caselaw_exists(self, ecli: str, celex_id: str) -> bool:
        """Ensure caselaw record exists, create if necessary."""
        if ecli in self._caselaw_cache:
            return True
            
        existing = self.session.exec(select(Caselaw).where(Caselaw.ecli == ecli)).first()
        if existing:
            self._caselaw_cache[ecli] = existing
            return True
            
        # Fetch real metadata; if not available, skip creating to avoid dummy records
        metadata = self._download_caselaw_metadata(celex_id)
        if not metadata or not metadata.get("title"):
            logger.warning(f"No metadata for caselaw CELEX {celex_id}; skipping creation of caselaw record for {ecli}")
            return False

        caselaw = Caselaw(
            ecli=ecli,
            court=metadata.get("court", ""),
            title=metadata["title"],
            summary_text="",
            decision_date=metadata.get("decision_date", datetime(1900, 1, 1)),
            source_url=f"https://publications.europa.eu/resource/celex/{celex_id}"
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
    
    def _ensure_caselaw_exists_with_metadata(self, ecli: str, celex_id: str, metadata: Dict[str, Any]) -> bool:
        """Ensure caselaw record exists using provided metadata."""
        if ecli in self._caselaw_cache:
            return True
            
        existing = self.session.exec(select(Caselaw).where(Caselaw.ecli == ecli)).first()
        if existing:
            self._caselaw_cache[ecli] = existing
            return True
            
        # Create caselaw record using provided metadata
        caselaw = Caselaw(
            ecli=ecli,
            court=metadata.get("court", "European Court of Justice"),
            title=metadata["title"],
            summary_text="",
            decision_date=metadata.get("decision_date") or datetime(1900, 1, 1),
            source_url=f"https://publications.europa.eu/resource/celex/{celex_id}"
        )
        
        try:
            self.session.add(caselaw)
            self.session.commit()
            self._caselaw_cache[ecli] = caselaw
            logger.info(f"Created caselaw record for {ecli} using provided metadata")
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
    
    def _ingest_citation_relationship(self, citation_data: Dict[str, Any]) -> bool:
        """Ingest a single citation relationship."""
        try:
            # Extract basic information
            source_celex = citation_data.get('source_celex')
            target_celex = citation_data.get('target_celex')
            ecli = citation_data.get('ecli')
            fragment_citing = citation_data.get('fragment_citing', '')
            fragment_cited = citation_data.get('fragment_cited', '')
            
            logger.info(f"Processing citation: {source_celex} -> {target_celex} (ECLI: {ecli})")
            
            if not all([source_celex, target_celex, ecli]):
                logger.warning(f"Incomplete citation data: {citation_data}")
                return False
            
            # Ensure records exist
            if not self._ensure_regulation_exists(source_celex):
                logger.warning(f"Failed to ensure regulation exists: {source_celex}")
                return False
            if not self._ensure_caselaw_exists(ecli, target_celex):
                logger.warning(f"Failed to ensure caselaw exists: {ecli} ({target_celex})")
                return False
            
            # Find target provision
            target_provision_id = self._find_target_provision(source_celex, fragment_cited)
            if not target_provision_id:
                logger.warning(f"Could not find target provision for {source_celex} fragment {fragment_cited}")
                return False
            
            logger.info(f"Found target provision: {target_provision_id}")
            
            # Create citation ID
            citation_id = self._create_citation_id(ecli, target_provision_id, fragment_citing)
            
            # Check for duplicates
            if citation_id in self._citation_cache:
                logger.debug(f"Citation already exists: {citation_id}")
                return True
            
            # Determine provision type and set appropriate field
            citation = Citation(
                ecli=ecli,
                quote_text=f"Cited in {fragment_citing}",
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
            logger.error(f"Failed to ingest citation: {e}")
            self.session.rollback()
            return False
    
    def _ingest_citation_relationship_with_metadata(self, citation_data: Dict[str, Any], xml_metadata: Dict[str, Dict[str, Any]]) -> bool:
        """Ingest a single citation relationship using XML metadata when available."""
        try:
            # Extract basic information
            source_celex = citation_data.get('source_celex')
            target_celex = citation_data.get('target_celex')
            ecli = citation_data.get('ecli')
            fragment_citing = citation_data.get('fragment_citing', '')
            fragment_cited = citation_data.get('fragment_cited', '')
            
            logger.info(f"Processing citation: {source_celex} -> {target_celex} (ECLI: {ecli})")
            
            if not all([source_celex, target_celex, ecli]):
                logger.warning(f"Incomplete citation data: {citation_data}")
                return False
            
            # Ensure regulation exists
            if not self._ensure_regulation_exists(source_celex):
                logger.warning(f"Failed to ensure regulation exists: {source_celex}")
                return False
            
            # Ensure caselaw exists using XML metadata if available
            if target_celex in xml_metadata:
                meta = xml_metadata[target_celex]
                if not self._ensure_caselaw_exists_with_metadata(ecli, target_celex, meta):
                    logger.warning(f"Failed to ensure caselaw exists with XML metadata: {ecli} ({target_celex})")
                    return False
            else:
                if not self._ensure_caselaw_exists(ecli, target_celex):
                    logger.warning(f"Failed to ensure caselaw exists via API: {ecli} ({target_celex})")
                    return False
            
            # Find target provision
            target_provision_id = self._find_target_provision(source_celex, fragment_cited)
            if not target_provision_id:
                logger.warning(f"Could not find target provision for {source_celex} fragment {fragment_cited}")
                return False
            
            logger.info(f"Found target provision: {target_provision_id}")
            
            # Create citation ID
            citation_id = self._create_citation_id(ecli, target_provision_id, fragment_citing)
            
            # Check for duplicates
            if citation_id in self._citation_cache:
                logger.debug(f"Citation already exists: {citation_id}")
                return True
            
            # Determine provision type and set appropriate field
            citation = Citation(
                ecli=ecli,
                quote_text=f"Cited in {fragment_citing}",
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
            logger.error(f"Failed to ingest citation with metadata: {e}")
            self.session.rollback()
            return False
    
    def parse_cellar_xml(self, xml_content: str) -> List[Dict[str, Any]]:
        """Parse CELLAR XML content and extract citation relationships."""
        try:
            root = ET.fromstring(xml_content)
            citations = []
            
            # Find all citation relationships
            for description in root.findall('.//rdf:Description', self.namespaces):
                node_id = description.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}nodeID')
                
                if node_id:
                    # This is a citation relationship
                    citation_data = self._parse_citation_description(description)
                    if citation_data:
                        citations.append(citation_data)
                        logger.info(f"Found citation: {citation_data}")
                    else:
                        logger.debug(f"Failed to parse citation from nodeID {node_id}")
            
            logger.info(f"Total citations found: {len(citations)}")
            return citations
            
        except Exception as e:
            logger.error(f"Failed to parse CELLAR XML: {e}")
            return []
    
    def _parse_citation_description(self, description: ET.Element) -> Optional[Dict[str, Any]]:
        """Parse a citation description element."""
        try:
            # Extract source and target
            annotated_source = description.find('owl:annotatedSource', self.namespaces)
            annotated_target = description.find('owl:annotatedTarget', self.namespaces)
            
            if annotated_source is None or annotated_target is None:
                logger.debug(f"Missing elements: annotatedSource={annotated_source is not None}, annotatedTarget={annotated_target is not None}")
                return None
            
            source_uri = annotated_source.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
            target_uri = annotated_target.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}resource')
            
            if not source_uri or not target_uri:
                logger.debug(f"Missing URI resources: source={source_uri}, target={target_uri}")
                return None
            
            # Extract CELEX IDs and ECLI
            source_celex = self._extract_celex_id(source_uri)
            target_celex = self._extract_celex_id(target_uri)
            
            # For ECLI, try target URI first, then generate from target_celex
            ecli = self._extract_ecli(target_uri)
            if not ecli and target_celex and target_celex.startswith('6'):
                # Generate ECLI from case CELEX ID
                year = target_celex[1:5] if len(target_celex) >= 5 else "2000"
                case_num = target_celex[7:] if len(target_celex) > 7 else "000"
                ecli = f"ECLI:EU:C:{year}:{case_num}"
                logger.debug(f"Generated ECLI {ecli} from CELEX {target_celex}")
            
            if not all([source_celex, target_celex]):
                logger.debug(f"Missing CELEX IDs: source={source_celex}, target={target_celex}")
                return None
            
            # Extract fragment information
            fragment_citing = ""
            fragment_cited = ""
            
            citing_elem = description.find('j.2:fragment_citing_source', self.namespaces)
            if citing_elem is not None:
                fragment_citing = citing_elem.text or ""
            
            cited_elem = description.find('j.2:fragment_cited_target', self.namespaces)
            if cited_elem is not None:
                fragment_cited = cited_elem.text or ""
            
            # Ensure we have an ECLI
            if not ecli and target_celex and target_celex.startswith('6'):
                year = target_celex[1:5] if len(target_celex) >= 5 else "2000"
                case_num = target_celex[7:] if len(target_celex) > 7 else "000"
                ecli = f"ECLI:EU:C:{year}:{case_num}"
            
            citation_data = {
                'source_celex': source_celex,
                'target_celex': target_celex,
                'ecli': ecli,
                'fragment_citing': fragment_citing,
                'fragment_cited': fragment_cited
            }
            
            logger.debug(f"Parsed citation data: {citation_data}")
            return citation_data
            
        except Exception as e:
            logger.error(f"Failed to parse citation description: {e}")
            return None
    
    def _extract_caselaw_celex_ids(self, xml_content: str) -> List[str]:
        """Extract case-law CELEX identifiers referenced in regulation RDF."""
        ids: Set[str] = set()
        pattern = re.compile(r"celex/(6\d{4}[A-Z]{2}\d{4})")
        for match in pattern.finditer(xml_content):
            ids.add(match.group(1))
        return list(ids)

    def _extract_caselaw_metadata_from_xml(self, xml_content: str) -> Dict[str, Dict[str, Any]]:
        """Extract caselaw metadata directly from CELLAR XML content."""
        metadata_by_celex = {}
        
        try:
            root = ET.fromstring(xml_content)
            
            # Find all case law descriptions
            for description in root.findall('.//rdf:Description', self.namespaces):
                about_attr = description.get('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about')
                
                if about_attr and 'celex/' in about_attr:
                    celex_id = self._extract_celex_id(about_attr)
                    
                    # Only process case law CELEX IDs (start with 6)
                    if celex_id and celex_id.startswith('6'):
                        # Extract metadata elements
                        title_elem = description.find('j.0:resource_legal_title', self.namespaces)
                        date_elem = description.find('j.0:work_date_document', self.namespaces)
                        ecli_elem = description.find('j.0:case-law_ecli', self.namespaces)
                        court_elem = description.find('j.0:resource_legal_in-force_publisher-agent', self.namespaces)
                        
                        if title_elem is not None and title_elem.text:
                            metadata = {
                                "title": title_elem.text.strip(),
                                "decision_date": None,
                                "court": "European Court of Justice",  # Default court
                                "ecli": None
                            }
                            
                            # Parse date
                            if date_elem is not None and date_elem.text:
                                try:
                                    metadata["decision_date"] = datetime.fromisoformat(date_elem.text.strip())
                                except ValueError:
                                    pass
                            
                            # Extract ECLI
                            if ecli_elem is not None and ecli_elem.text:
                                metadata["ecli"] = ecli_elem.text.strip()
                            
                            # Extract court info if available
                            if court_elem is not None and court_elem.text:
                                metadata["court"] = court_elem.text.strip()
                            
                            metadata_by_celex[celex_id] = metadata
                            logger.info(f"Extracted metadata for case {celex_id}: {metadata['title']}")
                        
        except Exception as e:
            logger.error(f"Failed to extract caselaw metadata from XML: {e}")
            
        return metadata_by_celex


    def ingest_cellar_data(self, xml_content: str) -> Dict[str, int]:
        """Ingest CELLAR XML data into the database."""
        logger.info("Starting CELLAR data ingestion...")
        
        # Parse XML
        citations = self.parse_cellar_xml(xml_content)
        logger.info(f"Found {len(citations)} citation relationships")
        
        # Extract caselaw metadata from XML first
        xml_metadata = self._extract_caselaw_metadata_from_xml(xml_content)
        logger.info(f"Extracted metadata for {len(xml_metadata)} cases from XML")
        
        # Ingest citations
        successful = 0
        failed = 0

        # ------------------------------------------------------------------
        # STEP 2: Extract caselaw references and ensure caselaw records exist
        # ------------------------------------------------------------------
        celex_ids = self._extract_caselaw_celex_ids(xml_content)
        caselaw_created = 0
        for celex_id in celex_ids:
            # First try to use XML metadata, then fallback to API
            if celex_id in xml_metadata:
                meta = xml_metadata[celex_id]
                logger.info(f"Using XML metadata for case {celex_id}")
            else:
                meta = self._download_caselaw_metadata(celex_id)
                logger.info(f"Using API metadata for case {celex_id}")
            
            if not meta or not meta.get("title"):
                logger.warning(f"No metadata available for caselaw CELEX {celex_id}; skipping")
                continue
                
            # Use ECLI from metadata or generate default
            ecli = meta.get("ecli")
            if not ecli:
                # Generate default ECLI pattern
                year = celex_id[1:5] if len(celex_id) >= 5 else "2000"
                case_num = celex_id[7:] if len(celex_id) > 7 else "000"
                ecli = f"ECLI:EU:C:{year}:{case_num}"
                logger.info(f"Generated default ECLI for {celex_id}: {ecli}")
            
            if self._ensure_caselaw_exists_with_metadata(ecli, celex_id, meta):
                caselaw_created += 1
        
        for citation in citations:
            if self._ingest_citation_relationship_with_metadata(citation, xml_metadata):
                successful += 1
            else:
                failed += 1
        
        logger.info(f"CELLAR ingestion complete: {successful} citations ingested, {caselaw_created} caselaw records created, {failed} citation failures")
        
        return {
            'total_citations': len(citations),
            'caselaw_created': caselaw_created,
            'successful': successful,
            'failed': failed
        }
    
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