"""CLI tool for managing staged implementation schedules."""

import sys
from pathlib import Path
from datetime import datetime
from typing import Optional

from eu_link_db.models_hierarchical import get_session
from eu_link_db.staged_implementation_parser import StagedImplementationParser


def show_regulation_schedule(celex_id: str):
    """Show implementation schedule for a regulation."""
    with get_session() as session:
        parser = StagedImplementationParser(session)
        schedule = parser.get_implementation_schedule(celex_id)
        
        if not schedule:
            print(f"No staged implementation found for {celex_id}")
            return
        
        regulation_names = {
            "32016R0679": "GDPR (General Data Protection Regulation)",
            "32024R1689": "AI Act (Artificial Intelligence Act)"
        }
        
        reg_name = regulation_names.get(celex_id, f"Regulation {celex_id}")
        print(f"=== {reg_name} - 段階的適用スケジュール ===\n")
        
        for i, stage in enumerate(schedule, 1):
            main_flag = " [メイン適用日]" if stage['is_main_application'] else ""
            date_obj = datetime.fromisoformat(stage['effective_date'])
            formatted_date = date_obj.strftime("%Y年%m月%d日")
            
            print(f"{i}. {formatted_date}{main_flag}")
            print(f"   {stage['scope_description']}")
            
            if stage['article_references']:
                print(f"   根拠条項: {stage['article_references']}")
            
            if stage['affected_articles']:
                print(f"   影響条項: {stage['affected_articles']}")
            
            # Show status
            now = datetime.now()
            if date_obj <= now:
                print("   ✅ 適用済み")
            else:
                days_until = (date_obj - now).days
                print(f"   ⏳ あと{days_until}日で適用")
            
            print()


def show_current_upcoming():
    """Show current and upcoming implementations across all regulations."""
    with get_session() as session:
        parser = StagedImplementationParser(session)
        implementations = parser.get_current_and_upcoming_implementations()
        
        print("=== 現在有効な適用段階 ===\n")
        for impl in implementations['current']:
            date_obj = datetime.fromisoformat(impl['effective_date'])
            formatted_date = date_obj.strftime("%Y年%m月%d日")
            
            print(f"• {formatted_date} - {impl['celex_id']}")
            print(f"  {impl['scope_description']}")
            if impl['article_references']:
                print(f"  根拠条項: {impl['article_references']}")
            if impl['affected_articles']:
                print(f"  影響条項: {impl['affected_articles']}")
            print()
        
        print("\n=== 今後の適用予定 ===\n")
        for impl in implementations['upcoming']:
            date_obj = datetime.fromisoformat(impl['effective_date'])
            formatted_date = date_obj.strftime("%Y年%m月%d日")
            days_until = (date_obj - datetime.now()).days
            
            print(f"• {formatted_date} ({days_until}日後) - {impl['celex_id']}")
            print(f"  {impl['scope_description']}")
            if impl['article_references']:
                print(f"  根拠条項: {impl['article_references']}")
            if impl['affected_articles']:
                print(f"  影響条項: {impl['affected_articles']}")
            print()


def load_implementation_from_xml(xml_path: Path, celex_id: str):
    """Load staged implementation from XML file."""
    if not xml_path.exists():
        print(f"XMLファイルが見つかりません: {xml_path}")
        return
    
    with get_session() as session:
        parser = StagedImplementationParser(session)
        
        with open(xml_path, 'r', encoding='utf-8') as f:
            xml_content = f.read()
        
        print(f"XMLファイルから段階的適用を抽出中: {xml_path}")
        result = parser.save_staged_implementation(xml_content, celex_id)
        
        if result['success']:
            print(f"✅ 成功: {result['saved']}件の段階的適用を保存")
            print("\n抽出されたスケジュール:")
            show_regulation_schedule(celex_id)
        else:
            print(f"❌ 失敗: {result['error']}")


def main():
    """Main CLI function."""
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python staged_implementation_cli.py show <celex_id>   # Show schedule for regulation")
        print("  python staged_implementation_cli.py overview          # Show current/upcoming")
        print("  python staged_implementation_cli.py load <xml_path> <celex_id>  # Load from XML")
        print("")
        print("Examples:")
        print("  python staged_implementation_cli.py show 32024R1689")
        print("  python staged_implementation_cli.py overview")
        print("  python staged_implementation_cli.py load eu_link_db/ai_act.xml 32024R1689")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "show":
        if len(sys.argv) != 3:
            print("Usage: python staged_implementation_cli.py show <celex_id>")
            sys.exit(1)
        
        celex_id = sys.argv[2]
        show_regulation_schedule(celex_id)
    
    elif command == "overview":
        show_current_upcoming()
    
    elif command == "load":
        if len(sys.argv) != 4:
            print("Usage: python staged_implementation_cli.py load <xml_path> <celex_id>")
            sys.exit(1)
        
        xml_path = Path(sys.argv[2])
        celex_id = sys.argv[3]
        load_implementation_from_xml(xml_path, celex_id)
    
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()