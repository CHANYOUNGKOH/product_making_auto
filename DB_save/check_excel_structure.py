"""엑셀 파일 구조 확인"""
import pandas as pd
import os
import sys

# 현재 스크립트의 디렉토리로 경로 설정
script_dir = os.path.dirname(os.path.abspath(__file__))
excel_path = os.path.join(script_dir, "Season_Filter_Seasons_Keywords.xlsx")

if os.path.exists(excel_path):
    xl = pd.ExcelFile(excel_path)
    print(f"시트 목록: {xl.sheet_names}\n")
    
    for sheet_name in xl.sheet_names:
        print(f"=== {sheet_name} 시트 ===")
        df = pd.read_excel(excel_path, sheet_name=sheet_name, nrows=10)
        print(f"컬럼 ({len(df.columns)}개): {list(df.columns)}")
        print(f"행 수: {len(df)}")
        print(f"\n데이터 샘플 (처음 5행):")
        print(df.head().to_string())
        print("\n" + "="*60 + "\n")
else:
    print(f"파일 없음: {excel_path}")
    print(f"현재 디렉토리: {script_dir}")
    print(f"파일 목록: {[f for f in os.listdir(script_dir) if f.endswith('.xlsx')]}")
