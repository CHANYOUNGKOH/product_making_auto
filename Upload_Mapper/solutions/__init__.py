"""
등록 솔루션 모듈
"""

import sys
from pathlib import Path

# 상대 import를 위한 경로 설정
current_dir = Path(__file__).parent
if str(current_dir) not in sys.path:
    sys.path.insert(0, str(current_dir))

from solutions.base_solution import BaseSolution
from solutions.dafalza import DafalzaSolution
from solutions.esellers import EsellersSolution

# 솔루션 팩토리
SOLUTION_CLASSES = {
    "다팔자": DafalzaSolution,
    "이셀러스": EsellersSolution,
    # 추후 추가: "스피드고": SpeedgoSolution,
    # 추후 추가: "플레이오토": PlayautoSolution,
}

def get_solution(solution_name: str) -> BaseSolution:
    """솔루션 인스턴스 가져오기"""
    solution_class = SOLUTION_CLASSES.get(solution_name)
    if solution_class:
        return solution_class()
    raise ValueError(f"지원하지 않는 솔루션: {solution_name}")

def list_solutions() -> list:
    """지원하는 솔루션 목록"""
    return list(SOLUTION_CLASSES.keys())

