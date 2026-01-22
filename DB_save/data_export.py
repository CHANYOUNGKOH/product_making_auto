"""
data_export.py

데이터 입고 및 출고 도구
- 기능: SQLite DB에서 상품을 읽어 마켓에 업로드할 DB를 내보냄
- 역할: 
  1. 마켓에 업로드할 때 DB를 빼낼 때 사용
  2. 입고된 DB 중 중복된 DB를 재가공하지 않기 위해 중복을 확인하는 용도
  3. DB 출고 이력을 기록
"""

import sys
import os

# 현재 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ui.main_window import MainWindow

if __name__ == "__main__":
    app = MainWindow()
    app.mainloop()

