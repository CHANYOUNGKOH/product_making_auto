"""Excel 파일 구조 확인"""
import pandas as pd
import os

excel_path = os.path.join(os.path.dirname(__file__), "Season_Filter_Seasons_Keywords.xlsx")

if os.path.exists(excel_path):
    xl = pd.ExcelFile(excel_path)
    print(f"시트 목록: {xl.sheet_names}\n")
    
    for sheet_name in xl.sheet_names:
        print(f"{'='*60}")
        print(f"시트: {sheet_name}")
        print(f"{'='*60}")
        df = pd.read_excel(excel_path, sheet_name=sheet_name)
        print(f"행 수: {len(df)}, 컬럼 수: {len(df.columns)}")
        print(f"컬럼: {list(df.columns)}")
        print("\n데이터 샘플:")
        print(df.head(20).to_string())
        print("\n")
else:
    print(f"파일을 찾을 수 없습니다: {excel_path}")

