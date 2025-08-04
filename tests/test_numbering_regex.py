import pytest
import re
import sys
import os

# Add the parent directory to the path to import the main module
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from eu_reg_html_analyzer import SectionBuilder


class TestSectionBuilder:
    """Test cases for SectionBuilder regex patterns"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.builder = SectionBuilder()
    
    def test_arabic_number_pattern(self):
        """Test that Arabic numbers (1., 2., etc.) are correctly identified"""
        test_cases = [
            ("1. First item", True),
            ("2. Second item", True),
            ("10. Tenth item", True),
            ("1) Alternative format", True),
            ("2) Another alternative", True),
            ("A. Letter format", False),
            ("(a) Letter format", False),
            ("(iv) Roman format", False),
            ("— Dash format", False),
        ]
        
        for text, expected in test_cases:
            match = self.builder.NUMBER.match(text)
            assert (match is not None) == expected, f"Failed for: {text}"
            if match:
                assert match.group(1).isdigit(), f"Number not extracted from: {text}"
    
    def test_lettered_pattern(self):
        """Test that lettered items (a), (b), etc. are correctly identified"""
        test_cases = [
            ("(a) First lettered item", True),
            ("(b) Second lettered item", True),
            ("(z) Last lettered item", True),
            ("1. Numbered item", False),
            ("2) Alternative number", False),
            ("(iv) Roman numeral", False),
            ("— Dash item", False),
            ("A. Capital letter", False),
        ]
        
        for text, expected in test_cases:
            match = self.builder.LETTER.match(text)
            assert (match is not None) == expected, f"Failed for: {text}"
            if match:
                assert match.group(1).isalpha(), f"Letter not extracted from: {text}"
    
    def test_roman_pattern(self):
        """Test that Roman numerals (i), (ii), (iv), etc. are correctly identified"""
        test_cases = [
            ("(i) First roman item", True),
            ("(ii) Second roman item", True),
            ("(iv) Fourth roman item", True),
            ("(x) Tenth roman item", True),
            ("(xiv) Fourteenth roman item", True),
            ("1. Numbered item", False),
            ("(a) Lettered item", False),
            ("— Dash item", False),
            ("I. Capital roman", False),
        ]
        
        for text, expected in test_cases:
            match = self.builder.ROMAN.match(text)
            assert (match is not None) == expected, f"Failed for: {text}"
            if match:
                roman_text = match.group(1)
                # Verify it contains only valid Roman numeral characters
                assert all(c in 'ivxlcdm' for c in roman_text.lower()), f"Invalid Roman numeral: {roman_text}"
    
    def test_dash_pattern(self):
        """Test that dash/bullet items are correctly identified"""
        test_cases = [
            ("— Dash item", True),
            ("- Hyphen item", True),
            ("• Bullet item", True),
            ("1. Numbered item", False),
            ("(a) Lettered item", False),
            ("(iv) Roman item", False),
        ]
        
        for text, expected in test_cases:
            match = self.builder.DASH.match(text)
            assert (match is not None) == expected, f"Failed for: {text}"
            if match:
                assert match.group(1).strip(), f"Content not extracted from: {text}"
    
    def test_hierarchical_pattern(self):
        """Test that hierarchical numbering (1. 1. content) is correctly identified"""
        test_cases = [
            ("1. 1. Hierarchical content", True),
            ("2. 3. Another hierarchical", True),
            ("10. 5. Complex hierarchical", True),
            ("1. Simple numbered", False),
            ("(a) Lettered item", False),
            ("— Dash item", False),
        ]
        
        for text, expected in test_cases:
            match = self.builder.HIER.match(text)
            assert (match is not None) == expected, f"Failed for: {text}"
            if match:
                parent_id = match.group(1)
                child_id = match.group(2)
                content = match.group(3)
                assert parent_id.isdigit(), f"Parent ID not extracted: {parent_id}"
                assert child_id.isdigit(), f"Child ID not extracted: {child_id}"
                assert content.strip(), f"Content not extracted: {content}"


if __name__ == "__main__":
    pytest.main([__file__]) 