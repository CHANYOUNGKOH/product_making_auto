"""
Upload_Mapper EXE 빌드 스크립트
PyInstaller를 사용하여 실행 파일을 생성합니다.
"""

import os
import sys
import shutil
from pathlib import Path

def build_exe():
    """EXE 파일 빌드"""
    
    # 현재 디렉토리 확인
    current_dir = Path(__file__).parent
    os.chdir(current_dir)
    
    print("=" * 60)
    print("Upload_Mapper EXE 빌드 시작")
    print("=" * 60)
    
    # PyInstaller 설치 확인
    try:
        import PyInstaller
        print(f"✓ PyInstaller 설치됨: {PyInstaller.__version__}")
    except ImportError:
        print("✗ PyInstaller가 설치되지 않았습니다.")
        print("다음 명령어로 설치하세요: pip install pyinstaller")
        sys.exit(1)
    
    # 이전 빌드 파일 정리
    print("\n이전 빌드 파일 정리 중...")
    if (current_dir / "build").exists():
        shutil.rmtree(current_dir / "build")
        print("✓ build 폴더 삭제됨")
    
    if (current_dir / "dist").exists():
        shutil.rmtree(current_dir / "dist")
        print("✓ dist 폴더 삭제됨")
    
    if (current_dir / "main.spec").exists():
        os.remove(current_dir / "main.spec")
        print("✓ main.spec 파일 삭제됨")
    
    # 데이터 파일 경로 설정
    templates_dir = current_dir / "templates"
    config_file = current_dir / "upload_mapper_config.json"
    
    # Windows와 다른 OS 구분
    if sys.platform == 'win32':
        sep = ';'
    else:
        sep = ':'
    
    # PyInstaller 명령어 구성 (python -m PyInstaller 사용)
    cmd = [
        sys.executable,  # Python 실행 파일 경로 사용
        "-m", "PyInstaller",
        "--onefile",  # 단일 EXE 파일로 생성
        "--windowed",  # 콘솔 창 숨김 (GUI 프로그램)
        "--name=Upload_Mapper",  # 실행 파일 이름
        "--clean",  # 임시 파일 정리
        f"--add-data={templates_dir}{sep}templates",  # templates 폴더 포함
        f"--add-data={config_file}{sep}.",  # config 파일 포함
        "--hidden-import=pandas",
        "--hidden-import=openpyxl",
        "--hidden-import=tkinter",
        "--hidden-import=config_manager",
        "--hidden-import=solutions",
        "--hidden-import=solutions.base_solution",
        "--hidden-import=solutions.dafalza",
        "--hidden-import=rules",
        "--hidden-import=rules.price_calculation",
        "--hidden-import=rules.shipping_fee",
        "--hidden-import=rules.option_price_correction",
        "main.py"
    ]
    
    print("\nPyInstaller 실행 중...")
    print(f"명령어: {' '.join(cmd)}\n")
    
    # PyInstaller 실행
    import subprocess
    result = subprocess.run(cmd, check=False, cwd=str(current_dir))
    
    if result.returncode == 0:
        print("\n" + "=" * 60)
        print("✓ 빌드 완료!")
        print("=" * 60)
        
        dist_dir = current_dir / "dist"
        exe_file = dist_dir / "Upload_Mapper.exe"
        
        # 필요한 파일들을 dist 폴더에 복사
        print("\n필요한 파일 복사 중...")
        if templates_dir.exists():
            import shutil
            dist_templates = dist_dir / "templates"
            if dist_templates.exists():
                shutil.rmtree(dist_templates)
            shutil.copytree(templates_dir, dist_templates)
            print(f"✓ templates 폴더 복사됨")
        
        if config_file.exists():
            import shutil
            dist_config = dist_dir / "upload_mapper_config.json"
            shutil.copy2(config_file, dist_config)
            print(f"✓ upload_mapper_config.json 복사됨")
        
        print(f"\n실행 파일 위치: {exe_file}")
        print(f"배포 폴더: {dist_dir}")
        print("\n배포 방법:")
        print("1. dist 폴더 전체를 다른 PC로 복사하세요.")
        print("2. Upload_Mapper.exe 파일을 실행하세요.")
        print("\n주의사항:")
        print("- 첫 실행 시 Windows SmartScreen 경고가 나타날 수 있습니다.")
        print("  '추가 정보' → '실행'을 클릭하여 실행할 수 있습니다.")
        print("- 백신 프로그램에서 차단될 수 있습니다. 이는 정상적인 현상입니다.")
    else:
        print("\n" + "=" * 60)
        print("✗ 빌드 실패")
        print("=" * 60)
        sys.exit(1)

if __name__ == "__main__":
    build_exe()

