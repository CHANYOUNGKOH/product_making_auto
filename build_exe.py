"""
PyInstaller를 사용한 실행 파일 빌드 스크립트 (자동 감지 버전)

✨ 특징: main_launcher_v7.py의 SCRIPTS 딕셔너리를 읽어서
         자동으로 필요한 폴더를 감지하고 포함합니다.
         스크립트를 추가/수정해도 이 파일을 수정할 필요가 없습니다!

사용법:
1. pip install pyinstaller
2. python build_exe.py
"""

import PyInstaller.__main__
import os
import sys
import ast
import re
from pathlib import Path

# 현재 스크립트의 디렉토리
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def extract_scripts_folders():
    """
    main_launcher_v7.py에서 SCRIPTS 딕셔너리를 읽어
    사용되는 모든 폴더를 자동으로 추출합니다.
    """
    launcher_path = os.path.join(BASE_DIR, 'main_launcher_v7.py')
    
    if not os.path.exists(launcher_path):
        print(f"⚠️ 경고: {launcher_path}를 찾을 수 없습니다.")
        return []
    
    try:
        with open(launcher_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # SCRIPTS 딕셔너리 부분 추출 (간단한 정규식 사용)
        # "folder": "폴더명" 패턴 찾기
        folder_pattern = r'"folder":\s*"([^"]+)"'
        matches = re.findall(folder_pattern, content)
        
        # 중복 제거 및 정리
        folders = set()
        for match in matches:
            # 중첩된 경로 처리 (예: "stage1_product_name/stage1_batch_API")
            parts = match.split('/')
            if parts:
                folders.add(parts[0])  # 최상위 폴더만 추가
        
        # 실제 존재하는 폴더만 필터링
        existing_folders = []
        for folder in sorted(folders):
            folder_path = os.path.join(BASE_DIR, folder)
            if os.path.isdir(folder_path):
                existing_folders.append(folder)
            else:
                print(f"⚠️ 경고: 폴더가 존재하지 않습니다: {folder}")
        
        return existing_folders
    
    except Exception as e:
        print(f"⚠️ 경고: 폴더 자동 감지 실패: {e}")
        print("기본 폴더 목록을 사용합니다.")
        return []

def get_all_subdirectories():
    """
    BASE_DIR의 모든 하위 디렉토리를 찾습니다.
    (자동 감지 실패 시 백업용)
    """
    folders = []
    for item in os.listdir(BASE_DIR):
        item_path = os.path.join(BASE_DIR, item)
        if os.path.isdir(item_path) and not item.startswith('.'):
            # 빌드/배포 관련 폴더 제외
            if item not in ['dist', 'build', '__pycache__', '.git']:
                folders.append(item)
    return sorted(folders)

# 자동으로 폴더 감지
print("📁 폴더 자동 감지 중...")
detected_folders = extract_scripts_folders()

if not detected_folders:
    print("⚠️ 자동 감지 실패, 모든 하위 폴더 포함...")
    detected_folders = get_all_subdirectories()

print(f"✅ 감지된 폴더: {', '.join(detected_folders)}")

# PyInstaller 옵션 설정
options = [
    'main_launcher_v7.py',  # 메인 스크립트
    '--name=상품가공프로그램',  # 실행 파일 이름
    '--onedir',  # 폴더 형태로 생성 (하위 스크립트 .py 파일 포함 가능)
    # '--onefile',  # 단일 실행 파일로 생성 (주석 처리: 하위 스크립트 실행 시 문제 발생 가능)
    '--windowed',  # 콘솔 창 숨김 (GUI 앱)
    '--icon=NONE',  # 아이콘 파일이 있으면 경로 지정
]

# 자동 감지된 폴더들을 --add-data에 추가
for folder in detected_folders:
    if sys.platform == 'win32':
        # Windows: 세미콜론 구분자
        options.append(f'--add-data={folder};{folder}')
    else:
        # Linux/Mac: 콜론 구분자
        options.append(f'--add-data={folder}:{folder}')

# Tcl/Tk 데이터 수집 (Tkinter GUI 앱 필수)
# Python 설치 경로에서 Tcl/Tk 데이터를 직접 찾아서 포함
def find_tcl_tk_data():
    """Python 설치 경로에서 Tcl/Tk 데이터 파일 찾기"""
    tcl_tk_data = []
    
    # Python 설치 경로 찾기
    python_lib = os.path.dirname(os.__file__)
    python_base = os.path.dirname(python_lib)
    
    # Tcl 데이터 경로
    tcl_paths = [
        os.path.join(python_base, 'tcl', 'tcl8.6'),
        os.path.join(python_lib, 'tcl8.6'),
        os.path.join(python_base, 'lib', 'tcl8.6'),
    ]
    
    # Tk 데이터 경로
    tk_paths = [
        os.path.join(python_base, 'tcl', 'tk8.6'),
        os.path.join(python_lib, 'tk8.6'),
        os.path.join(python_base, 'lib', 'tk8.6'),
    ]
    
    # Tcl 데이터 찾기
    for tcl_path in tcl_paths:
        if os.path.exists(tcl_path):
            tcl_tk_data.append((tcl_path, '_tcl_data'))
            print(f"✅ Tcl 데이터 발견: {tcl_path}")
            break
    
    # Tk 데이터 찾기
    for tk_path in tk_paths:
        if os.path.exists(tk_path):
            tcl_tk_data.append((tk_path, '_tk_data'))
            print(f"✅ Tk 데이터 발견: {tk_path}")
            break
    
    return tcl_tk_data

# Tcl/Tk 데이터 찾기 및 추가
tcl_tk_data = find_tcl_tk_data()
if tcl_tk_data:
    for src, dst in tcl_tk_data:
        if sys.platform == 'win32':
            options.append(f'--add-data={src};{dst}')
        else:
            options.append(f'--add-data={src}:{dst}')
else:
    # 찾지 못한 경우 collect-all 사용
    print("⚠️ Tcl/Tk 데이터를 직접 찾지 못했습니다. collect-all 옵션을 사용합니다.")
    options.append('--collect-all=tcl')
    options.append('--collect-all=tkinter')

# 숨겨진 import 포함 (자동 감지 실패 시)
hidden_imports = [
    'tkinter',
    'tkinter.ttk',
    'pandas',
    'openpyxl',
    'PIL',
    'PIL.Image',
    'PIL.ImageTk',
    'boto3',
    'websocket',
    'json',
    'pathlib',
]

for imp in hidden_imports:
    options.append(f'--hidden-import={imp}')

# 제외할 모듈 (필요시)
# options.append('--exclude-module=matplotlib')

# 빌드 결과물 저장 위치
options.extend([
    '--distpath=dist',  # dist 폴더에 결과 저장
    '--workpath=build',  # build 폴더에 임시 파일 저장
    '--specpath=.',  # .spec 파일 저장 위치
    '--clean',  # 빌드 전 임시 파일 정리
    '--noconfirm',  # 덮어쓰기 확인 없이 진행
])

# Windows에서 실행 시
if sys.platform == 'win32':
    # 아이콘 파일이 있으면 추가
    icon_path = os.path.join(BASE_DIR, 'icon.ico')
    if os.path.exists(icon_path):
        options.append(f'--icon={icon_path}')

print("\n" + "=" * 60)
print("상품 가공 프로그램 빌드 시작")
print("=" * 60)
print(f"작업 디렉토리: {BASE_DIR}")
print(f"포함된 폴더: {len(detected_folders)}개")
print("=" * 60)

# PyInstaller 실행
PyInstaller.__main__.run(options)

print("\n" + "=" * 60)
print("✅ 빌드 완료!")
print(f"실행 파일 위치: {os.path.join(BASE_DIR, 'dist', '상품가공프로그램', '상품가공프로그램.exe')}")
print(f"배포 폴더: {os.path.join(BASE_DIR, 'dist', '상품가공프로그램')}")

# 빌드 결과 검증
dist_folder = os.path.join(BASE_DIR, 'dist', '상품가공프로그램')
internal_folder = os.path.join(dist_folder, '_internal')

print("\n🔍 빌드 결과 검증 중...")
tcl_data_exists = os.path.exists(os.path.join(internal_folder, '_tcl_data'))
tk_data_exists = os.path.exists(os.path.join(internal_folder, '_tk_data'))

if tcl_data_exists and tk_data_exists:
    print("✅ Tcl/Tk 데이터가 올바르게 포함되었습니다.")
else:
    print("⚠️ 경고: Tcl/Tk 데이터가 누락되었을 수 있습니다.")
    if not tcl_data_exists:
        print("   - _tcl_data 폴더가 없습니다.")
    if not tk_data_exists:
        print("   - _tk_data 폴더가 없습니다.")
    print("\n💡 해결 방법:")
    print("   1. PyInstaller를 최신 버전으로 업그레이드: pip install --upgrade pyinstaller")
    print("   2. 빌드 폴더 삭제 후 재빌드: rmdir /s /q dist build")
    print("   3. Python 버전 확인 (Python 3.11 이상 권장)")

print("\n📦 배포 방법:")
print("   'dist/상품가공프로그램' 폴더 전체를 다른 PC로 복사하세요.")
print("   폴더 안의 '상품가공프로그램.exe'를 실행하세요.")
print("\n💡 팁: 스크립트를 수정해도 이 빌드 스크립트는 수정할 필요가 없습니다!")
print("   main_launcher_v7.py의 SCRIPTS 딕셔너리만 업데이트하면 됩니다.")
print("=" * 60)

