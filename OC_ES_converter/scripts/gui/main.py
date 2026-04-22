#!/usr/bin/env python3
"""
오너클랜 → 이셀러스 변환기 GUI

Usage:
    python main.py
"""

import sys
from pathlib import Path

# 스크립트 경로를 Python 경로에 추가
SCRIPT_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(SCRIPT_DIR.parent))

from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QIcon, QPixmap, QPainter, QColor, QFont

from converter_gui import ConverterWindow


def create_app_icon():
    """애플리케이션 아이콘 생성 (프로그래밍 방식)"""
    # 64x64 아이콘 생성
    pixmap = QPixmap(64, 64)
    pixmap.fill(Qt.transparent)

    painter = QPainter(pixmap)
    painter.setRenderHint(QPainter.Antialiasing)

    # 배경 원 (파란색 그라데이션 느낌)
    painter.setBrush(QColor(52, 120, 246))  # 파란색
    painter.setPen(Qt.NoPen)
    painter.drawRoundedRect(4, 4, 56, 56, 12, 12)

    # "ES" 텍스트 (eSellers 약어)
    painter.setPen(QColor(255, 255, 255))
    font = QFont("Arial", 22, QFont.Bold)
    painter.setFont(font)
    painter.drawText(pixmap.rect(), Qt.AlignCenter, "ES")

    painter.end()
    return QIcon(pixmap)


def main():
    # High DPI 지원 (PyQt5)
    QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
    QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)

    app = QApplication(sys.argv)
    app.setStyle('Fusion')

    # 애플리케이션 정보 설정
    app.setApplicationName("OC2ES Converter")
    app.setApplicationDisplayName("OC→ES 변환기")
    app.setOrganizationName("eSellers")

    # 아이콘 설정
    app_icon = create_app_icon()
    app.setWindowIcon(app_icon)

    window = ConverterWindow()
    window.setWindowIcon(app_icon)  # 윈도우에도 아이콘 설정
    window.show()

    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
