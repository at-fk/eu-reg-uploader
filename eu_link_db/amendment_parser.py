"""Amendment history parser for EUR-Lex NOTICE XML files."""

import xml.etree.ElementTree as ET
import logging
import re
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime

from sqlmodel import Session, select
from .models_hierarchical import Regulation, AmendmentHistory, ConsolidatedVersion

logger = logging.getLogger(__name__)


class AmendmentParser:
    """Parse amendment history from EUR-Lex XML files."""
    
    def __init__(self, session: Session):
        self.session = session

    def _extract_celex_from_uri(self, uri: str) -> Optional[str]:
        """Extract CELEX ID from URI."""
        if 'celex/' in uri:
            return uri.split('celex/')[-1].split('?')[0]
        return None

    def _extract_eli_from_uri(self, uri: str) -> Optional[str]:
        """Extract ELI from URI."""
        if 'eli/' in uri:
            return uri.split('eli/')[-1].split('?')[0]
        return None

    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse date string to datetime."""
        if not date_str:
            return None
        try:
            return datetime.strptime(date_str, "%Y-%m-%d")
        except ValueError:
            logger.warning(f"Failed to parse date: {date_str}")
            return None

    def _extract_consolidated_version_id(self, uri: str) -> Optional[str]:
        """Extract consolidated version ID from URI like '01995L0046-20180525'."""
        pattern = r'(\d{5}[LR]\d{4}-\d{8})'
        match = re.search(pattern, uri)
        return match.group(1) if match else None

    def extract_amendment_history(self, xml_content: str, celex_id: str) -> Dict[str, int]:
        """Extract amendment history from XML content."""
        try:
            root = ET.fromstring(xml_content)
            amendments_created = 0
            versions_created = 0
            
            # Find amendment relationships
            amendment_types = [
                ('RESOURCE_LEGAL_AMENDED_BY_ACT', 'amended'),
                ('RESOURCE_LEGAL_CORRIGED_BY_ACT', 'corrected'),
                ('RESOURCE_LEGAL_REPEALED_BY_ACT', 'repealed'),
                ('REPEALS_ACT', 'repeals')
            ]
            
            for xml_tag, amendment_type in amendment_types:
                elements = root.findall(f'.//{xml_tag}')
                for element in elements:
                    try:
                        # Extract amending act CELEX
                        amending_celex = None
                        amending_eli = None
                        
                        # Look for CELEX in VALUE or IDENTIFIER
                        value_elem = element.find('.//VALUE')
                        if value_elem is not None:
                            amending_celex = self._extract_celex_from_uri(value_elem.text or '')
                        
                        # Look for ELI
                        for sameas in element.findall('.//SAMEAS'):
                            if 'eli/' in (sameas.get('rdf:resource') or ''):
                                amending_eli = self._extract_eli_from_uri(sameas.get('rdf:resource') or '')
                                break
                        
                        # Extract date
                        date_elem = element.find('.//DATE')
                        amendment_date = self._parse_date(date_elem.text) if date_elem is not None else None
                        
                        # Extract article reference from annotations
                        article_ref = None
                        ref_elem = element.find('.//REFERENCE_TO_MODIFIED_LOCATION')
                        if ref_elem is not None:
                            article_ref = ref_elem.text
                        
                        # Extract OJ reference
                        oj_ref = None
                        for sameas in element.findall('.//SAMEAS'):
                            if 'oj/' in (sameas.get('rdf:resource') or ''):
                                oj_ref = sameas.get('rdf:resource', '').split('/')[-1]
                                break
                        
                        # Create amendment record
                        if amending_celex or amending_eli:
                            amendment = AmendmentHistory(
                                celex_id=celex_id,
                                amending_act_celex=amending_celex,
                                amending_act_eli=amending_eli,
                                amendment_type=amendment_type,
                                amendment_date=amendment_date,
                                article_reference=article_ref,
                                oj_reference=oj_ref
                            )
                            
                            self.session.add(amendment)
                            amendments_created += 1
                            logger.info(f"Created amendment record: {celex_id} {amendment_type} by {amending_celex or amending_eli}")
                    
                    except Exception as e:
                        logger.error(f"Failed to process amendment element: {e}")
            
            # Find consolidated versions
            consolidated_elements = root.findall('.//RESOURCE_LEGAL_CONSOLIDATED_BY_ACT_CONSOLIDATED')
            for element in consolidated_elements:
                try:
                    value_elem = element.find('.//VALUE')
                    if value_elem is not None:
                        version_id = self._extract_consolidated_version_id(value_elem.text or '')
                        if version_id:
                            # Extract date from version ID (last 8 digits)
                            date_str = version_id[-8:]
                            consolidated_date = datetime.strptime(date_str, "%Y%m%d")
                            
                            version = ConsolidatedVersion(
                                version_id=version_id,
                                base_celex_id=celex_id,
                                consolidated_date=consolidated_date,
                                version_uri=value_elem.text,
                                is_current=True  # We'll update this logic later
                            )
                            
                            self.session.add(version)
                            versions_created += 1
                            logger.info(f"Created consolidated version: {version_id}")
                
                except Exception as e:
                    logger.error(f"Failed to process consolidated version: {e}")
            
            # Extract time series metadata
            self._extract_time_series_metadata(root, celex_id)
            
            self.session.commit()
            
            return {
                'amendments_created': amendments_created,
                'versions_created': versions_created
            }
            
        except Exception as e:
            logger.error(f"Failed to extract amendment history: {e}")
            self.session.rollback()
            return {'amendments_created': 0, 'versions_created': 0}

    def _extract_time_series_metadata(self, root: ET.Element, celex_id: str) -> None:
        """Extract and update time series metadata in regulation table."""
        try:
            regulation = self.session.exec(
                select(Regulation).where(Regulation.celex_id == celex_id)
            ).first()
            
            if not regulation:
                logger.warning(f"Regulation {celex_id} not found for time series update")
                return
            
            # Extract entry into force date
            entry_force_elem = root.find('.//RESOURCE_LEGAL_DATE_ENTRY_INTO_FORCE/VALUE')
            if entry_force_elem is not None:
                regulation.entry_into_force_date = self._parse_date(entry_force_elem.text)
            
            # Extract application date
            application_elem = root.find('.//RESOURCE_LEGAL_DATE_APPLICATION/VALUE')
            if application_elem is not None:
                regulation.application_date = self._parse_date(application_elem.text)
            
            # Extract end of validity date
            end_validity_elem = root.find('.//RESOURCE_LEGAL_DATE_END-OF-VALIDITY/VALUE')
            if end_validity_elem is not None:
                regulation.end_of_validity_date = self._parse_date(end_validity_elem.text)
            
            # Find current consolidated version
            consolidated_elem = root.find('.//RESOURCE_LEGAL_CONSOLIDATED_BY_ACT_CONSOLIDATED/VALUE')
            if consolidated_elem is not None:
                version_id = self._extract_consolidated_version_id(consolidated_elem.text or '')
                if version_id:
                    regulation.consolidated_version_id = version_id
                    regulation.consolidated_as_of_date = datetime.strptime(version_id[-8:], "%Y%m%d")
            
            logger.info(f"Updated time series metadata for {celex_id}")
            
        except Exception as e:
            logger.error(f"Failed to extract time series metadata: {e}")

    def get_amendment_stats(self, celex_id: str) -> Dict[str, Any]:
        """Get amendment statistics for a regulation."""
        amendments = self.session.exec(
            select(AmendmentHistory).where(AmendmentHistory.celex_id == celex_id)
        ).all()
        
        versions = self.session.exec(
            select(ConsolidatedVersion).where(ConsolidatedVersion.base_celex_id == celex_id)
        ).all()
        
        regulation = self.session.exec(
            select(Regulation).where(Regulation.celex_id == celex_id)
        ).first()
        
        return {
            'total_amendments': len(amendments),
            'amendments_by_type': {
                amendment_type: len([a for a in amendments if a.amendment_type == amendment_type])
                for amendment_type in ['amended', 'corrected', 'repealed', 'repeals']
            },
            'total_versions': len(versions),
            'current_version': regulation.consolidated_version_id if regulation else None,
            'time_series': {
                'adoption_date': regulation.adoption_date.isoformat() if regulation and regulation.adoption_date else None,
                'entry_into_force_date': regulation.entry_into_force_date.isoformat() if regulation and regulation.entry_into_force_date else None,
                'application_date': regulation.application_date.isoformat() if regulation and regulation.application_date else None,
                'end_of_validity_date': regulation.end_of_validity_date.isoformat() if regulation and regulation.end_of_validity_date else None
            } if regulation else {}
        }