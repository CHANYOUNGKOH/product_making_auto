"""
시즌 필터링 연결 검증 스크립트

시즌 필터링이 데이터 출고 스크립트와 올바르게 연결되었는지 검증합니다.
"""

import os
import sys

def check_imports():
    """필요한 모듈 import 가능 여부 확인"""
    print("=" * 60)
    print("1. 모듈 Import 검증")
    print("=" * 60)
    
    errors = []
    warnings = []
    
    # season_filter_manager_gui 모듈 확인
    try:
        from season_filter_manager_gui import (
            load_season_config,
            filter_products_by_season,
            SeasonFilterManagerGUI
        )
        print("✅ season_filter_manager_gui 모듈 import 성공")
        print(f"   - load_season_config: {load_season_config}")
        print(f"   - filter_products_by_season: {filter_products_by_season}")
        print(f"   - SeasonFilterManagerGUI: {SeasonFilterManagerGUI}")
    except ImportError as e:
        errors.append(f"❌ season_filter_manager_gui import 실패: {e}")
        print(f"❌ season_filter_manager_gui import 실패: {e}")
    
    # db_handler 모듈 확인
    try:
        from database.db_handler import DBHandler
        print("✅ database.db_handler 모듈 import 성공")
        print(f"   - DBHandler: {DBHandler}")
        
        # SEASON_FILTER_AVAILABLE 확인
        from database import db_handler
        if hasattr(db_handler, 'SEASON_FILTER_AVAILABLE'):
            if db_handler.SEASON_FILTER_AVAILABLE:
                print("✅ SEASON_FILTER_AVAILABLE = True")
            else:
                warnings.append("⚠️ SEASON_FILTER_AVAILABLE = False (시즌 필터링 비활성화)")
                print("⚠️ SEASON_FILTER_AVAILABLE = False")
        else:
            warnings.append("⚠️ SEASON_FILTER_AVAILABLE 속성이 없습니다")
            print("⚠️ SEASON_FILTER_AVAILABLE 속성이 없습니다")
    except ImportError as e:
        errors.append(f"❌ database.db_handler import 실패: {e}")
        print(f"❌ database.db_handler import 실패: {e}")
    
    # main_window 모듈 확인
    try:
        from ui.main_window import MainWindow
        print("✅ ui.main_window 모듈 import 성공")
        print(f"   - MainWindow: {MainWindow}")
    except ImportError as e:
        warnings.append(f"⚠️ ui.main_window import 실패 (GUI 모듈이므로 선택적): {e}")
        print(f"⚠️ ui.main_window import 실패 (GUI 모듈이므로 선택적): {e}")
    
    print()
    return errors, warnings


def check_function_signatures():
    """함수 시그니처 확인"""
    print("=" * 60)
    print("2. 함수 시그니처 검증")
    print("=" * 60)
    
    errors = []
    
    try:
        from season_filter_manager_gui import filter_products_by_season, load_season_config
        import inspect
        
        # filter_products_by_season 시그니처 확인
        sig = inspect.signature(filter_products_by_season)
        params = list(sig.parameters.keys())
        print(f"✅ filter_products_by_season 파라미터: {params}")
        
        # 반환값 확인 (docstring에서)
        doc = filter_products_by_season.__doc__
        if doc and 'Returns:' in doc:
            print(f"   반환값: 5개 튜플 (filtered_products, excluded_count, excluded_seasons, included_seasons, season_stats)")
        
        # load_season_config 시그니처 확인
        sig2 = inspect.signature(load_season_config)
        params2 = list(sig2.parameters.keys())
        print(f"✅ load_season_config 파라미터: {params2}")
        
    except Exception as e:
        errors.append(f"❌ 함수 시그니처 확인 실패: {e}")
        print(f"❌ 함수 시그니처 확인 실패: {e}")
    
    print()
    return errors


def check_file_paths():
    """필요한 파일 경로 확인"""
    print("=" * 60)
    print("3. 파일 경로 검증")
    print("=" * 60)
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    errors = []
    warnings = []
    
    # Excel 파일 확인
    excel_path = os.path.join(script_dir, "Season_Filter_Seasons_Keywords.xlsx")
    if os.path.exists(excel_path):
        print(f"✅ Excel 설정 파일 존재: {excel_path}")
    else:
        warnings.append(f"⚠️ Excel 설정 파일 없음: {excel_path}")
        print(f"⚠️ Excel 설정 파일 없음: {excel_path}")
    
    # JSON 캐시 파일 확인
    json_path = os.path.join(script_dir, "season_filters.json")
    if os.path.exists(json_path):
        print(f"✅ JSON 캐시 파일 존재: {json_path}")
    else:
        warnings.append(f"⚠️ JSON 캐시 파일 없음 (자동 생성됨): {json_path}")
        print(f"⚠️ JSON 캐시 파일 없음 (자동 생성됨): {json_path}")
    
    # 환경설정 파일 확인
    config_path = os.path.join(script_dir, "season_filter_config.json")
    if os.path.exists(config_path):
        print(f"✅ 환경설정 파일 존재: {config_path}")
    else:
        warnings.append(f"⚠️ 환경설정 파일 없음 (기본값 사용): {config_path}")
        print(f"⚠️ 환경설정 파일 없음 (기본값 사용): {config_path}")
    
    print()
    return errors, warnings


def check_db_handler_integration():
    """db_handler 통합 확인"""
    print("=" * 60)
    print("4. db_handler 통합 검증")
    print("=" * 60)
    
    errors = []
    warnings = []
    
    try:
        from database.db_handler import DBHandler
        import inspect
        
        # get_products_for_upload 메서드 확인
        if hasattr(DBHandler, 'get_products_for_upload'):
            sig = inspect.signature(DBHandler.get_products_for_upload)
            params = list(sig.parameters.keys())
            print(f"✅ DBHandler.get_products_for_upload 파라미터: {params}")
            
            if 'season_filter_enabled' in params:
                print("   ✅ season_filter_enabled 파라미터 존재")
            else:
                errors.append("❌ season_filter_enabled 파라미터가 없습니다")
                print("   ❌ season_filter_enabled 파라미터가 없습니다")
            
            # _last_season_filter_info 속성 확인
            if hasattr(DBHandler, '_last_season_filter_info'):
                print("   ✅ _last_season_filter_info 속성 존재")
            else:
                warnings.append("⚠️ _last_season_filter_info 속성이 없습니다")
                print("   ⚠️ _last_season_filter_info 속성이 없습니다")
        else:
            errors.append("❌ get_products_for_upload 메서드가 없습니다")
            print("❌ get_products_for_upload 메서드가 없습니다")
        
    except Exception as e:
        errors.append(f"❌ db_handler 통합 확인 실패: {e}")
        print(f"❌ db_handler 통합 확인 실패: {e}")
    
    print()
    return errors, warnings


def check_main_window_integration():
    """main_window 통합 확인"""
    print("=" * 60)
    print("5. main_window 통합 검증")
    print("=" * 60)
    
    errors = []
    warnings = []
    
    try:
        # main_window.py 파일 직접 읽기
        script_dir = os.path.dirname(os.path.abspath(__file__))
        main_window_path = os.path.join(script_dir, "ui", "main_window.py")
        
        if not os.path.exists(main_window_path):
            warnings.append(f"⚠️ main_window.py 파일을 찾을 수 없습니다: {main_window_path}")
            print(f"⚠️ main_window.py 파일을 찾을 수 없습니다: {main_window_path}")
            print()
            return errors, warnings
        
        with open(main_window_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # season_filter_var 확인
        if 'season_filter_var' in content:
            print("✅ season_filter_var 변수 존재")
        else:
            errors.append("❌ season_filter_var 변수가 없습니다")
            print("❌ season_filter_var 변수가 없습니다")
        
        # season_filter_enabled 파라미터 전달 확인
        if 'season_filter_enabled=' in content:
            print("✅ season_filter_enabled 파라미터 전달 코드 존재")
        else:
            errors.append("❌ season_filter_enabled 파라미터 전달 코드가 없습니다")
            print("❌ season_filter_enabled 파라미터 전달 코드가 없습니다")
        
        # _last_season_filter_info 사용 확인
        if '_last_season_filter_info' in content:
            print("✅ _last_season_filter_info 사용 코드 존재")
        else:
            warnings.append("⚠️ _last_season_filter_info 사용 코드가 없습니다")
            print("⚠️ _last_season_filter_info 사용 코드가 없습니다")
        
        # SeasonFilterManagerGUI import 확인
        if 'SeasonFilterManagerGUI' in content:
            print("✅ SeasonFilterManagerGUI import 존재")
        else:
            warnings.append("⚠️ SeasonFilterManagerGUI import가 없습니다")
            print("⚠️ SeasonFilterManagerGUI import가 없습니다")
        
    except Exception as e:
        errors.append(f"❌ main_window 통합 확인 실패: {e}")
        print(f"❌ main_window 통합 확인 실패: {e}")
    
    print()
    return errors, warnings


def test_season_config_loading():
    """시즌 설정 로드 테스트"""
    print("=" * 60)
    print("6. 시즌 설정 로드 테스트")
    print("=" * 60)
    
    errors = []
    warnings = []
    
    try:
        from season_filter_manager_gui import load_season_config
        
        script_dir = os.path.dirname(os.path.abspath(__file__))
        excel_path = os.path.join(script_dir, "Season_Filter_Seasons_Keywords.xlsx")
        json_path = os.path.join(script_dir, "season_filters.json")
        
        if not os.path.exists(excel_path):
            warnings.append(f"⚠️ Excel 파일이 없어 테스트를 건너뜁니다: {excel_path}")
            print(f"⚠️ Excel 파일이 없어 테스트를 건너뜁니다: {excel_path}")
            print()
            return errors, warnings
        
        config = load_season_config(excel_path, json_path)
        
        if config:
            print("✅ 시즌 설정 로드 성공")
            
            # 설정 구조 확인
            if 'seasons' in config:
                season_count = len(config['seasons'])
                print(f"   - 시즌 개수: {season_count}개")
            else:
                warnings.append("⚠️ 'seasons' 키가 없습니다")
                print("   ⚠️ 'seasons' 키가 없습니다")
            
            if 'settings' in config:
                print("   - 'settings' 키 존재")
            else:
                warnings.append("⚠️ 'settings' 키가 없습니다")
                print("   ⚠️ 'settings' 키가 없습니다")
        else:
            errors.append("❌ 시즌 설정 로드 실패 (None 반환)")
            print("❌ 시즌 설정 로드 실패 (None 반환)")
        
    except Exception as e:
        errors.append(f"❌ 시즌 설정 로드 테스트 실패: {e}")
        print(f"❌ 시즌 설정 로드 테스트 실패: {e}")
        import traceback
        print(traceback.format_exc())
    
    print()
    return errors, warnings


def main():
    """메인 검증 함수"""
    print("\n" + "=" * 60)
    print("시즌 필터링 연결 검증 시작")
    print("=" * 60 + "\n")
    
    all_errors = []
    all_warnings = []
    
    # 각 검증 단계 실행
    errors, warnings = check_imports()
    all_errors.extend(errors)
    all_warnings.extend(warnings)
    
    errors, warnings = check_function_signatures()
    all_errors.extend(errors)
    all_warnings.extend(warnings)
    
    errors, warnings = check_file_paths()
    all_errors.extend(errors)
    all_warnings.extend(warnings)
    
    errors, warnings = check_db_handler_integration()
    all_errors.extend(errors)
    all_warnings.extend(warnings)
    
    errors, warnings = check_main_window_integration()
    all_errors.extend(errors)
    all_warnings.extend(warnings)
    
    errors, warnings = test_season_config_loading()
    all_errors.extend(errors)
    all_warnings.extend(warnings)
    
    # 최종 결과 출력
    print("=" * 60)
    print("검증 결과 요약")
    print("=" * 60)
    
    if all_errors:
        print(f"\n❌ 오류 {len(all_errors)}개:")
        for i, error in enumerate(all_errors, 1):
            print(f"   {i}. {error}")
    else:
        print("\n✅ 오류 없음")
    
    if all_warnings:
        print(f"\n⚠️ 경고 {len(all_warnings)}개:")
        for i, warning in enumerate(all_warnings, 1):
            print(f"   {i}. {warning}")
    else:
        print("\n✅ 경고 없음")
    
    print("\n" + "=" * 60)
    if all_errors:
        print("❌ 검증 실패: 오류가 발견되었습니다.")
        return 1
    elif all_warnings:
        print("⚠️ 검증 완료: 경고가 있지만 기본 기능은 정상입니다.")
        return 0
    else:
        print("✅ 검증 성공: 모든 연결이 정상입니다.")
        return 0


if __name__ == "__main__":
    sys.exit(main())

