"""
백업 스크립트: 순환식 조합 변경 전 백업

이 스크립트는 상품조합을 순환식으로 변경하기 전에 
데이터베이스와 스크립트 파일을 백업합니다.
"""

import os
import shutil
from datetime import datetime
from pathlib import Path

def backup_database(db_path: str, backup_dir: str = None) -> str:
    """
    데이터베이스 백업
    
    Args:
        db_path: 백업할 데이터베이스 파일 경로
        backup_dir: 백업 디렉토리 (None이면 db_path와 같은 디렉토리의 backups 폴더)
    
    Returns:
        백업 파일 경로
    """
    if not os.path.exists(db_path):
        print(f"❌ 데이터베이스 파일이 없습니다: {db_path}")
        return None
    
    # 백업 디렉토리 설정
    if backup_dir is None:
        db_dir = os.path.dirname(os.path.abspath(db_path))
        backup_dir = os.path.join(db_dir, "backups")
    
    os.makedirs(backup_dir, exist_ok=True)
    
    # 백업 파일명 생성
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    db_name = os.path.basename(db_path)
    backup_name = f"{db_name}_backup_before_circular_{timestamp}"
    
    # .db 확장자 유지
    if db_name.endswith('.db'):
        backup_name = f"{backup_name}.db"
    
    backup_path = os.path.join(backup_dir, backup_name)
    
    # 백업 실행
    try:
        shutil.copy2(db_path, backup_path)
        
        # 파일 크기 확인
        original_size = os.path.getsize(db_path)
        backup_size = os.path.getsize(backup_path)
        
        if original_size == backup_size:
            print(f"✅ 데이터베이스 백업 완료: {backup_path}")
            print(f"   원본 크기: {original_size:,} bytes")
            print(f"   백업 크기: {backup_size:,} bytes")
            return backup_path
        else:
            print(f"❌ 백업 파일 크기가 일치하지 않습니다!")
            print(f"   원본 크기: {original_size:,} bytes")
            print(f"   백업 크기: {backup_size:,} bytes")
            return None
            
    except Exception as e:
        print(f"❌ 데이터베이스 백업 실패: {e}")
        return None


def backup_script_files(base_dir: str, backup_dir: str = None) -> list:
    """
    스크립트 파일 백업
    
    Args:
        base_dir: 백업할 스크립트 파일이 있는 디렉토리
        backup_dir: 백업 디렉토리 (None이면 base_dir의 backups 폴더)
    
    Returns:
        백업된 파일 경로 리스트
    """
    if not os.path.exists(base_dir):
        print(f"❌ 디렉토리가 없습니다: {base_dir}")
        return []
    
    # 백업 디렉토리 설정
    if backup_dir is None:
        backup_dir = os.path.join(base_dir, "backups", "scripts")
    
    os.makedirs(backup_dir, exist_ok=True)
    
    # 백업할 파일 목록
    files_to_backup = [
        "database/db_handler.py",
        "ui/main_window.py",
        "data_export.py",
    ]
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backed_up_files = []
    
    for file_path in files_to_backup:
        source_path = os.path.join(base_dir, file_path)
        
        if not os.path.exists(source_path):
            print(f"⚠️ 파일이 없습니다: {source_path}")
            continue
        
        # 백업 파일명 생성
        file_name = os.path.basename(source_path)
        file_dir = os.path.dirname(file_path)
        
        # 디렉토리 구조 유지
        backup_file_dir = os.path.join(backup_dir, file_dir)
        os.makedirs(backup_file_dir, exist_ok=True)
        
        # 백업 파일명
        name_without_ext, ext = os.path.splitext(file_name)
        backup_name = f"{name_without_ext}_backup_before_circular_{timestamp}{ext}"
        backup_path = os.path.join(backup_file_dir, backup_name)
        
        try:
            shutil.copy2(source_path, backup_path)
            original_size = os.path.getsize(source_path)
            backup_size = os.path.getsize(backup_path)
            
            if original_size == backup_size:
                print(f"✅ 스크립트 백업 완료: {backup_path}")
                backed_up_files.append(backup_path)
            else:
                print(f"❌ 백업 파일 크기가 일치하지 않습니다: {backup_path}")
                
        except Exception as e:
            print(f"❌ 스크립트 백업 실패: {source_path} → {e}")
    
    return backed_up_files


def main():
    """메인 실행 함수"""
    print("=" * 60)
    print("순환식 조합 변경 전 백업 스크립트")
    print("=" * 60)
    print()
    
    # 현재 스크립트 위치 기준으로 경로 설정
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = script_dir
    
    print(f"📁 작업 디렉토리: {base_dir}")
    print()
    
    # 데이터베이스 파일 찾기
    db_files = []
    for root, dirs, files in os.walk(base_dir):
        for file in files:
            if file.endswith('.db') and 'backup' not in file.lower():
                db_path = os.path.join(root, file)
                db_files.append(db_path)
    
    if not db_files:
        print("⚠️ 데이터베이스 파일을 찾을 수 없습니다.")
        print(f"   검색 디렉토리: {base_dir}")
        db_path = input("데이터베이스 파일 경로를 직접 입력하세요 (Enter로 건너뛰기): ").strip()
        if db_path:
            db_files = [db_path]
    
    # 데이터베이스 백업
    backup_dir = os.path.join(base_dir, "backups")
    os.makedirs(backup_dir, exist_ok=True)
    
    backed_up_dbs = []
    for db_path in db_files:
        backup_path = backup_database(db_path, backup_dir)
        if backup_path:
            backed_up_dbs.append(backup_path)
    
    print()
    
    # 스크립트 파일 백업
    print("스크립트 파일 백업 중...")
    backed_up_scripts = backup_script_files(base_dir, backup_dir)
    
    print()
    print("=" * 60)
    print("백업 완료 요약")
    print("=" * 60)
    print(f"✅ 데이터베이스 백업: {len(backed_up_dbs)}개")
    for db in backed_up_dbs:
        print(f"   - {os.path.basename(db)}")
    
    print(f"✅ 스크립트 백업: {len(backed_up_scripts)}개")
    for script in backed_up_scripts:
        print(f"   - {os.path.relpath(script, base_dir)}")
    
    print()
    print("⚠️ 백업 파일은 다음 디렉토리에 저장되었습니다:")
    print(f"   {backup_dir}")
    print()
    print("이제 순환식 조합 변경을 진행할 수 있습니다.")


if __name__ == "__main__":
    main()
