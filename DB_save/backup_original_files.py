# -*- coding: utf-8 -*-
"""
원본 파일 백업 스크립트
안전한 최적화 작업 전에 원본 파일을 백업합니다.
"""

import os
import shutil
from datetime import datetime

# 현재 스크립트의 디렉토리
script_dir = os.path.dirname(os.path.abspath(__file__))

# 백업할 파일 목록
backup_files = [
    {
        "source": os.path.join(script_dir, "database", "db_handler.py"),
        "backup": os.path.join(script_dir, "database", "db_handler_원본_백업.py")
    },
    {
        "source": os.path.join(script_dir, "ui", "main_window.py"),
        "backup": os.path.join(script_dir, "ui", "main_window_원본_백업.py")
    }
]

def backup_files():
    """원본 파일을 백업합니다."""
    backed_up = []
    failed = []
    
    for file_info in backup_files:
        source = file_info["source"]
        backup = file_info["backup"]
        
        try:
            # 소스 파일이 존재하는지 확인
            if not os.path.exists(source):
                print(f"⚠️ 소스 파일이 존재하지 않습니다: {source}")
                failed.append(source)
                continue
            
            # 백업 파일이 이미 존재하는지 확인
            if os.path.exists(backup):
                # 타임스탬프 추가하여 중복 방지
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                base_name = os.path.splitext(backup)[0]
                ext = os.path.splitext(backup)[1]
                backup = f"{base_name}_{timestamp}{ext}"
                file_info["backup"] = backup
            
            # 파일 복사
            shutil.copy2(source, backup)
            print(f"✅ 백업 완료: {os.path.basename(source)} → {os.path.basename(backup)}")
            backed_up.append((source, backup))
            
        except Exception as e:
            print(f"❌ 백업 실패: {source}")
            print(f"   오류: {e}")
            failed.append(source)
    
    print("\n" + "="*60)
    print("백업 결과 요약")
    print("="*60)
    print(f"✅ 성공: {len(backed_up)}개")
    for source, backup in backed_up:
        print(f"   {os.path.basename(source)} → {os.path.basename(backup)}")
    
    if failed:
        print(f"\n❌ 실패: {len(failed)}개")
        for source in failed:
            print(f"   {source}")
    
    return len(backed_up), len(failed)

if __name__ == "__main__":
    print("="*60)
    print("원본 파일 백업 시작")
    print("="*60)
    print()
    
    success_count, fail_count = backup_files()
    
    print()
    if fail_count == 0:
        print("✅ 모든 파일 백업이 완료되었습니다!")
    else:
        print(f"⚠️ {fail_count}개 파일 백업에 실패했습니다.")
