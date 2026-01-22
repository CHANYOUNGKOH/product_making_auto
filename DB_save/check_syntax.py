"""문법 오류 체크 스크립트"""
import ast
import sys
import os

def check_syntax(file_path):
    """파일의 문법 오류를 체크"""
    try:
        full_path = os.path.join(os.path.dirname(__file__), file_path)
        with open(full_path, 'r', encoding='utf-8') as f:
            source = f.read()
        
        try:
            ast.parse(source)
            print(f"✅ {file_path}: 문법 오류 없음")
            return True
        except SyntaxError as e:
            print(f"❌ {file_path}: 문법 오류 발견")
            print(f"   줄 {e.lineno}: {e.text}")
            print(f"   오류: {e.msg}")
            if e.offset:
                print(f"   위치: {e.offset}번째 문자")
            return False
    except Exception as e:
        print(f"⚠️ {file_path}: 파일 읽기 실패 - {e}")
        return False

if __name__ == "__main__":
    file_path = "season_filter_manager_gui.py"
    success = check_syntax(file_path)
    sys.exit(0 if success else 1)
