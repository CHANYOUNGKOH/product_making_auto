#!/usr/bin/env python3
"""
설정 관리 모듈

JSON 파일을 사용하여 설정 저장/로드:
- OpenMarketCategory.xlsx 경로
- 선택된 마켓 목록
"""

import json
import os
from pathlib import Path
from typing import Optional, List


class Settings:
    """설정 관리 클래스"""

    # 기본 설정 파일 경로 (스크립트 위치 기준)
    DEFAULT_CONFIG_PATH = Path(__file__).parent.parent.parent.parent.parent / 'cc-system'
    CONFIG_FILE = Path(__file__).parent / 'config.json'

    # 기본값
    DEFAULTS = {
        'category_file': '',
        'last_input_dir': '',
        'last_output_dir': '',
        'last_input_file': '',
        'last_output_file': '',
        'selected_markets': None,  # None이면 기본값 사용, 리스트면 저장된 값 사용
        'seo_alt_enabled': True,
    }

    def __init__(self):
        self._settings = self.DEFAULTS.copy()
        self.load()

        # 카테고리 파일 기본값 설정
        if not self._settings['category_file']:
            default_category = self.DEFAULT_CONFIG_PATH / 'OpenMarketCategory.xlsx'
            if default_category.exists():
                self._settings['category_file'] = str(default_category)

    def load(self) -> bool:
        """설정 파일 로드"""
        try:
            if self.CONFIG_FILE.exists():
                with open(self.CONFIG_FILE, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                    self._settings.update(loaded)
                return True
        except Exception as e:
            print(f"설정 로드 실패: {e}")
        return False

    def save(self) -> bool:
        """설정 파일 저장"""
        try:
            self.CONFIG_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(self.CONFIG_FILE, 'w', encoding='utf-8') as f:
                json.dump(self._settings, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            print(f"설정 저장 실패: {e}")
            return False

    @property
    def category_file(self) -> str:
        """카테고리 파일 경로"""
        return self._settings.get('category_file', '')

    @category_file.setter
    def category_file(self, value: str):
        self._settings['category_file'] = value
        self.save()

    @property
    def last_input_dir(self) -> str:
        """마지막 입력 디렉토리"""
        return self._settings.get('last_input_dir', '')

    @last_input_dir.setter
    def last_input_dir(self, value: str):
        self._settings['last_input_dir'] = value
        self.save()

    @property
    def last_output_dir(self) -> str:
        """마지막 출력 디렉토리"""
        return self._settings.get('last_output_dir', '')

    @last_output_dir.setter
    def last_output_dir(self, value: str):
        self._settings['last_output_dir'] = value
        self.save()

    @property
    def last_input_file(self) -> str:
        """마지막 입력 파일"""
        return self._settings.get('last_input_file', '')

    @last_input_file.setter
    def last_input_file(self, value: str):
        self._settings['last_input_file'] = value
        # 디렉토리도 함께 저장
        if value:
            self._settings['last_input_dir'] = str(Path(value).parent)
        self.save()

    @property
    def last_output_file(self) -> str:
        """마지막 출력 파일"""
        return self._settings.get('last_output_file', '')

    @last_output_file.setter
    def last_output_file(self, value: str):
        self._settings['last_output_file'] = value
        # 디렉토리도 함께 저장
        if value:
            self._settings['last_output_dir'] = str(Path(value).parent)
        self.save()

    def get_initial_input_dir(self) -> str:
        """입력 파일 대화상자용 초기 디렉토리"""
        if self.last_input_dir and os.path.exists(self.last_input_dir):
            return self.last_input_dir
        return str(Path.home())

    def get_initial_output_dir(self) -> str:
        """출력 파일 대화상자용 초기 디렉토리"""
        if self.last_output_dir and os.path.exists(self.last_output_dir):
            return self.last_output_dir
        if self.last_input_dir and os.path.exists(self.last_input_dir):
            return self.last_input_dir
        return str(Path.home())

    @property
    def selected_markets(self) -> Optional[List[str]]:
        """선택된 마켓 목록 (None이면 기본값 사용)"""
        return self._settings.get('selected_markets', None)

    @selected_markets.setter
    def selected_markets(self, value: Optional[List[str]]):
        self._settings['selected_markets'] = value
        self.save()

    @property
    def seo_alt_enabled(self) -> bool:
        """SEO alt 텍스트 자동 삽입 활성화 여부"""
        return self._settings.get('seo_alt_enabled', False)

    @seo_alt_enabled.setter
    def seo_alt_enabled(self, value: bool):
        self._settings['seo_alt_enabled'] = value
        self.save()
