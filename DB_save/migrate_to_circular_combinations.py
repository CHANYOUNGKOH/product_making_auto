"""
마이그레이션 스크립트: 순환식 조합 구조로 변경

이 스크립트는 상품조합을 순환식으로 사용할 수 있도록
데이터베이스 구조를 변경합니다.

주의: 실행 전 반드시 백업을 생성하세요!
"""

import os
import sys
from datetime import datetime

# DB 핸들러 import
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from database.db_handler import DBHandler


def create_store_combination_state_table(db_handler):
    """
    store_combination_state 테이블 생성
    
    이 테이블은 각 스토어별로 마지막 사용한 조합 인덱스를 추적합니다.
    """
    cursor = db_handler.conn.cursor()
    
    try:
        # 테이블 생성
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS store_combination_state (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sheet_name TEXT NOT NULL,
                business_number TEXT NOT NULL,
                product_code TEXT NOT NULL,
                last_used_combination_index INTEGER DEFAULT 0,
                last_used_url_type TEXT DEFAULT 'mix',
                last_used_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(sheet_name, business_number, product_code)
            )
        """)
        
        # 인덱스 생성 (조회 성능 최적화)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_store_combination_state_lookup
            ON store_combination_state(sheet_name, business_number, product_code)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_store_combination_state_product
            ON store_combination_state(product_code)
        """)
        
        db_handler.conn.commit()
        print("✅ store_combination_state 테이블 생성 완료")
        return True
        
    except Exception as e:
        print(f"❌ 테이블 생성 실패: {e}")
        db_handler.conn.rollback()
        return False


def initialize_state_from_assignments(db_handler, reset_mode: bool = False):
    """
    기존 combination_assignments에서 초기 상태 설정
    
    Args:
        db_handler: DB 핸들러
        reset_mode: True면 모두 0부터 시작, False면 기존 내역 기반으로 초기화
    """
    cursor = db_handler.conn.cursor()
    
    try:
        if reset_mode:
            # 모두 0부터 시작 (초기화)
            print("⚠️ 리셋 모드: 모든 스토어가 0부터 시작합니다.")
            # 상태 테이블은 비어있으므로 자동으로 0부터 시작됨
            print("✅ 리셋 모드 설정 완료 (상태 테이블 비어있음)")
            return True
        
        # 기존 내역 기반으로 초기화
        print("기존 combination_assignments에서 초기 상태 설정 중...")
        
        # 조합 정보와 함께 최신 상태 조회
        # 서브쿼리에서 외부 집계 함수를 참조할 수 없으므로, 
        # 서브쿼리 내부에서 최대값을 별도로 계산
        cursor.execute("""
            INSERT OR IGNORE INTO store_combination_state
            (sheet_name, business_number, product_code, 
             last_used_combination_index, last_used_url_type,
             last_used_at, updated_at)
            SELECT 
                grouped.sheet_name,
                grouped.business_number,
                grouped.product_code,
                grouped.last_index,
                COALESCE(
                    (SELECT pc.url_type 
                     FROM product_combinations pc 
                     WHERE pc.product_code = grouped.product_code 
                     AND pc.combination_index = grouped.last_index
                     LIMIT 1),
                    'mix'
                ) as last_url_type,
                grouped.last_assigned_at,
                CURRENT_TIMESTAMP
            FROM (
                SELECT 
                    ca.sheet_name,
                    ca.business_number,
                    ca.product_code,
                    MAX(ca.combination_index) as last_index,
                    MAX(ca.assigned_at) as last_assigned_at
                FROM combination_assignments ca
                GROUP BY ca.sheet_name, ca.business_number, ca.product_code
            ) grouped
        """)
        
        rows_inserted = cursor.rowcount
        db_handler.conn.commit()
        
        print(f"✅ {rows_inserted}개 스토어의 초기 상태 설정 완료")
        return True
        
    except Exception as e:
        print(f"❌ 초기 상태 설정 실패: {e}")
        import traceback
        traceback.print_exc()
        db_handler.conn.rollback()
        return False


def verify_migration(db_handler):
    """마이그레이션 검증"""
    cursor = db_handler.conn.cursor()
    
    try:
        # 테이블 존재 확인
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='store_combination_state'
        """)
        if not cursor.fetchone():
            print("❌ store_combination_state 테이블이 없습니다!")
            return False
        
        # 상태 데이터 확인
        cursor.execute("SELECT COUNT(*) FROM store_combination_state")
        count = cursor.fetchone()[0]
        print(f"✅ 검증 완료: store_combination_state 테이블에 {count}개 레코드")
        
        # combination_assignments 테이블 확인
        cursor.execute("SELECT COUNT(*) FROM combination_assignments")
        assignments_count = cursor.fetchone()[0]
        print(f"✅ combination_assignments 테이블: {assignments_count}개 레코드 (참조용 유지)")
        
        return True
        
    except Exception as e:
        print(f"❌ 검증 실패: {e}")
        return False


def main():
    """메인 실행 함수"""
    print("=" * 60)
    print("순환식 조합 구조 마이그레이션")
    print("=" * 60)
    print()
    print("⚠️ 주의: 이 스크립트를 실행하기 전에 반드시 백업을 생성하세요!")
    print()
    
    # 데이터베이스 파일 경로 입력
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # 기본 DB 파일 찾기
    default_db = None
    for root, dirs, files in os.walk(script_dir):
        for file in files:
            if file.endswith('.db') and 'backup' not in file.lower() and 'test' not in file.lower():
                default_db = os.path.join(root, file)
                break
        if default_db:
            break
    
    if default_db:
        print(f"📁 기본 데이터베이스: {os.path.basename(default_db)}")
        db_path = input(f"데이터베이스 파일 경로 (Enter로 기본값 사용): ").strip()
        if not db_path:
            db_path = default_db
    else:
        db_path = input("데이터베이스 파일 경로: ").strip()
    
    if not os.path.exists(db_path):
        print(f"❌ 데이터베이스 파일이 없습니다: {db_path}")
        return
    
    # 리셋 모드 선택
    print()
    print("초기화 모드 선택:")
    print("  1. 기존 내역 기반 초기화 (권장)")
    print("     - combination_assignments의 최신 조합부터 시작")
    print("     - 기존 출고 이력 유지")
    print()
    print("  2. 모두 0부터 시작 (리셋)")
    print("     - 모든 스토어가 처음부터 다시 시작")
    print("     - 기존 출고 이력은 유지되지만 사용 안 함")
    print()
    
    mode = input("모드를 선택하세요 (1 또는 2, 기본값: 1): ").strip()
    reset_mode = (mode == "2")
    
    if reset_mode:
        confirm = input("⚠️ 리셋 모드입니다. 정말 진행하시겠습니까? (yes/no): ").strip().lower()
        if confirm != "yes":
            print("취소되었습니다.")
            return
    
    print()
    print("데이터베이스 연결 중...")
    
    # DB 핸들러 생성 및 연결
    db_handler = DBHandler(db_path)
    try:
        db_handler.connect()
    except Exception as e:
        print(f"❌ 데이터베이스 연결 실패: {e}")
        return
    
    try:
        # 1. 테이블 생성
        print()
        print("1단계: store_combination_state 테이블 생성...")
        if not create_store_combination_state_table(db_handler):
            print("❌ 마이그레이션 실패")
            return
        
        # 2. 초기 상태 설정
        print()
        print("2단계: 초기 상태 설정...")
        if not initialize_state_from_assignments(db_handler, reset_mode):
            print("❌ 마이그레이션 실패")
            return
        
        # 3. 검증
        print()
        print("3단계: 마이그레이션 검증...")
        if not verify_migration(db_handler):
            print("❌ 마이그레이션 검증 실패")
            return
        
        print()
        print("=" * 60)
        print("✅ 마이그레이션 완료!")
        print("=" * 60)
        print()
        print("다음 단계:")
        print("  1. db_handler.py에 순환 로직 메서드 추가")
        print("  2. main_window.py의 조합 선택 로직 수정")
        print("  3. 테스트 실행")
        
    finally:
        db_handler.close()


if __name__ == "__main__":
    main()
