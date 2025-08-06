"""Parser for staged implementation schedules from EU regulation XML files."""

import xml.etree.ElementTree as ET
import logging
import re
from typing import Dict, List, Optional, Any
from datetime import datetime

from sqlmodel import Session, select
from .models_hierarchical import Regulation, StagedImplementation

logger = logging.getLogger(__name__)


class StagedImplementationParser:
    """Parse staged implementation schedules from EU regulation XML files."""
    
    def __init__(self, session: Session):
        self.session = session

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string to datetime."""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            logger.warning(f"Failed to parse date: {date_str}")
            return None

    def _parse_legal_basis_article(self, comment: str) -> Optional[str]:
        """Extract legal basis article (施行日の根拠条項) from XML comment."""
        if not comment:
            return None
        
        # Look for Article 113, 99, 97 patterns in various formats
        patterns = [
            r'\{ART[^}]*\}[^}]*?(\d{2,3}(?:\.\d+)?(?:\([a-z]\))?)',  # {ART|...} 113(a)
            r'Article\s+(\d{2,3}(?:\.\d+)?(?:\([a-z]\))?)',          # Article 113(a)
            r'art\.\s*(\d{2,3}(?:\.\d+)?(?:\([a-z]\))?)',           # art. 113(a)
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, comment, re.IGNORECASE)
            for match in matches:
                article_num = match.strip()
                # Focus on implementation date articles (113, 99, 97, etc.)
                if article_num.startswith(('113', '99', '97', '111', '112')):
                    return f"Article {article_num}"
        
        return None

    def _parse_affected_articles(self, scope_description: str, comment: str) -> Optional[str]:
        """Extract affected articles from XML data only. Returns None as this info is not in XML."""
        # XMLには影響を受ける条項の情報は含まれていないため、空欄とする
        return None

    def _determine_scope_description(self, comment: str, date: str, celex_id: str) -> str:
        """Determine scope description based on XML comment and context."""
        if not comment:
            return f"Provisions effective from {date}"
        
        # AI Act specific descriptions
        if celex_id == "32024R1689":  # AI Act
            if "113(a)" in comment:
                return "Prohibited AI practices and obligations for high-risk AI systems"
            elif "113(b)" in comment:
                return "General-purpose AI model obligations and AI Office establishment"
            elif "113(c)" in comment:
                return "Remaining implementation provisions and market surveillance"
            elif "113" in comment and "(" not in comment:
                return "Main regulation provisions (general obligations, conformity assessment, market surveillance)"
            elif "DATPUB" in comment:
                return "Basic legal framework entry into force"
        
        # GDPR specific descriptions
        elif celex_id == "32016R0679":  # GDPR
            if "99" in comment:
                return "Full application of GDPR (main enforcement date)"
            elif "DATPUB" in comment:
                return "GDPR entry into force (preparatory period)"
        
        # Generic description
        article_refs = self._parse_legal_basis_article(comment)
        if article_refs:
            return f"Implementation based on {article_refs}"
        
        return f"Regulatory provisions effective from {date}"

    def _determine_implementation_type(self, xml_tag: str, comment: str) -> str:
        """Determine implementation type from XML tag and comment."""
        if "ENTRY-INTO-FORCE" in xml_tag:
            if "DATPUB" in comment:
                return "legal_entry_into_force"
            else:
                return "application_start"
        elif "APPLICATION" in xml_tag:
            return "application_start"
        elif "DEADLINE" in xml_tag:
            return "compliance_deadline"
        else:
            return "other"

    def _is_main_application_date(self, date: str, celex_id: str, comment: str) -> bool:
        """Determine if this is the main application date."""
        if celex_id == "32024R1689":  # AI Act
            return date == "2026-08-02"  # Main application date
        elif celex_id == "32016R0679":  # GDPR
            return date == "2018-05-25"  # Main application date
        else:
            # For other regulations, consider the latest application date as main
            return False

    def extract_staged_implementation(self, xml_content: str, celex_id: str) -> Dict[str, Any]:
        """
        Extract staged implementation schedule from XML content.
        
        Args:
            xml_content: XML content string
            celex_id: CELEX ID of the regulation
            
        Returns:
            Dictionary with extraction results
        """
        try:
            root = ET.fromstring(xml_content)
            implementations = []
            
            # Define XML tags to look for implementation dates
            implementation_tags = [
                ('RESOURCE_LEGAL_DATE_ENTRY-INTO-FORCE', 'entry_into_force'),
                ('RESOURCE_LEGAL_DATE_APPLICATION', 'application'),
                ('RESOURCE_LEGAL_DATE_DEADLINE', 'deadline')
            ]
            
            for xml_tag, base_type in implementation_tags:
                elements = root.findall(f'.//{xml_tag}')
                
                for element in elements:
                    try:
                        value_elem = element.find('.//VALUE')
                        if value_elem is None or not value_elem.text:
                            continue
                        
                        date_str = value_elem.text
                        effective_date = self._parse_date(date_str)
                        if not effective_date:
                            continue
                        
                        # Extract additional information
                        comment_elem = element.find('.//COMMENT_ON_DATE')
                        type_elem = element.find('.//TYPE_OF_DATE')
                        
                        comment = comment_elem.text if comment_elem is not None else ""
                        xml_type_code = type_elem.text if type_elem is not None else ""
                        
                        # Determine implementation details
                        implementation_type = self._determine_implementation_type(xml_tag, comment)
                        scope_description = self._determine_scope_description(comment, date_str, celex_id)
                        
                        # Separate legal basis and affected articles
                        article_references = self._parse_legal_basis_article(comment)  # 施行日の根拠条項
                        affected_articles = self._parse_affected_articles(scope_description, comment)  # 影響を受ける条項
                        
                        is_main = self._is_main_application_date(date_str, celex_id, comment)
                        
                        implementation = {
                            'celex_id': celex_id,
                            'effective_date': effective_date,
                            'implementation_type': implementation_type,
                            'scope_description': scope_description,
                            'article_references': article_references,
                            'affected_articles': affected_articles,
                            'comment': comment[:1000] if comment else None,  # Truncate to fit DB
                            'xml_type_code': xml_type_code,
                            'is_main_application': is_main
                        }
                        
                        implementations.append(implementation)
                        
                    except Exception as e:
                        logger.error(f"Failed to process implementation element: {e}")
                        continue
            
            # Remove duplicates (same date and type)
            unique_implementations = []
            seen = set()
            
            for impl in implementations:
                key = (impl['effective_date'], impl['implementation_type'], impl['scope_description'])
                if key not in seen:
                    seen.add(key)
                    unique_implementations.append(impl)
            
            # Sort by effective date
            unique_implementations.sort(key=lambda x: x['effective_date'])
            
            return {
                'implementations': unique_implementations,
                'total_found': len(unique_implementations)
            }
            
        except Exception as e:
            logger.error(f"Failed to extract staged implementation: {e}")
            return {
                'implementations': [],
                'total_found': 0,
                'error': str(e)
            }

    def save_staged_implementation(self, xml_content: str, celex_id: str) -> Dict[str, Any]:
        """
        Extract and save staged implementation to database.
        
        Args:
            xml_content: XML content string
            celex_id: CELEX ID of the regulation
            
        Returns:
            Dictionary with save results
        """
        try:
            # Check if regulation exists
            regulation = self.session.exec(
                select(Regulation).where(Regulation.celex_id == celex_id)
            ).first()
            
            if not regulation:
                return {
                    'success': False,
                    'error': f'Regulation {celex_id} not found in database',
                    'saved': 0
                }
            
            # Extract implementation data
            extraction_result = self.extract_staged_implementation(xml_content, celex_id)
            
            if 'error' in extraction_result:
                return {
                    'success': False,
                    'error': extraction_result['error'],
                    'saved': 0
                }
            
            # Clear existing implementations for this regulation
            existing = self.session.exec(
                select(StagedImplementation).where(StagedImplementation.celex_id == celex_id)
            ).all()
            
            for impl in existing:
                self.session.delete(impl)
            
            # Save new implementations
            saved_count = 0
            for impl_data in extraction_result['implementations']:
                implementation = StagedImplementation(**impl_data)
                self.session.add(implementation)
                saved_count += 1
            
            self.session.commit()
            
            logger.info(f"Saved {saved_count} staged implementations for {celex_id}")
            
            return {
                'success': True,
                'saved': saved_count,
                'total_found': extraction_result['total_found']
            }
            
        except Exception as e:
            logger.error(f"Failed to save staged implementation: {e}")
            self.session.rollback()
            return {
                'success': False,
                'error': str(e),
                'saved': 0
            }

    def get_implementation_schedule(self, celex_id: str) -> List[Dict[str, Any]]:
        """
        Get staged implementation schedule for a regulation.
        
        Args:
            celex_id: CELEX ID of the regulation
            
        Returns:
            List of implementation stages sorted by effective date
        """
        implementations = self.session.exec(
            select(StagedImplementation)
            .where(StagedImplementation.celex_id == celex_id)
            .order_by(StagedImplementation.effective_date)
        ).all()
        
        schedule = []
        for impl in implementations:
            schedule.append({
                'effective_date': impl.effective_date.isoformat(),
                'implementation_type': impl.implementation_type,
                'scope_description': impl.scope_description,
                'article_references': impl.article_references,
                'affected_articles': impl.affected_articles,
                'is_main_application': impl.is_main_application,
                'comment': impl.comment
            })
        
        return schedule

    def get_current_and_upcoming_implementations(self, reference_date: Optional[datetime] = None) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get current and upcoming implementations across all regulations.
        
        Args:
            reference_date: Reference date (default: today)
            
        Returns:
            Dictionary with 'current' and 'upcoming' implementation lists
        """
        if reference_date is None:
            reference_date = datetime.now()
        
        # Get all implementations
        all_implementations = self.session.exec(
            select(StagedImplementation)
            .order_by(StagedImplementation.effective_date)
        ).all()
        
        current = []
        upcoming = []
        
        for impl in all_implementations:
            impl_dict = {
                'celex_id': impl.celex_id,
                'effective_date': impl.effective_date.isoformat(),
                'implementation_type': impl.implementation_type,
                'scope_description': impl.scope_description,
                'article_references': impl.article_references,
                'affected_articles': impl.affected_articles,
                'is_main_application': impl.is_main_application
            }
            
            if impl.effective_date <= reference_date:
                current.append(impl_dict)
            else:
                upcoming.append(impl_dict)
        
        return {
            'current': current,
            'upcoming': upcoming
        }