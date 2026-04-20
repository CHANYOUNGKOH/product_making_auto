# 이셀러스(ESellers) — 변환/업로드 경로

이셀러스는 단독 마켓이 아니라 **ESM+ (옥션/지마켓) 및 기타 마켓의 일괄등록 파일 포맷**. OC → 이셀러스 엑셀 78컬럼 → ESM+ 업로드.

## 문서 지도

| 파일 | 상태 |
|---|---|
| [pricing.md](pricing.md) | ✅ 상속 (ESM 정책 그대로) |
| [columns.md](columns.md) | 🟡 초안 (78컬럼 목록만, 결정 규칙 미정) |
| stores.md | — (스토어 개념 없음, [../esm/stores.md](../esm/stores.md) 참조) |
| [api.md](api.md) | ✅ 업로드 경로 요약 |
| bulk-ops.md | 🔴 미작성 |

## 스코프

대상 마켓:
- ✅ 옥션 (ESM+)
- ✅ 지마켓 (ESM+)
- ⏳ 11번가 (확인 필요 — 별도 포맷 가능성)
- ⏳ 쿠팡 (예정)
- ⏳ 멸치쇼핑 (예정)

## 관련 코드
- [Upload_Mapper/solutions/esellers.py](../../../Upload_Mapper/solutions/esellers.py) — 변환기 1,117줄
- [Upload_Mapper/solutions/base_solution.py](../../../Upload_Mapper/solutions/base_solution.py) — 추상 클래스
- [Upload_Mapper/rules/option_price_correction.py](../../../Upload_Mapper/rules/option_price_correction.py) — 옵션 추가금 상한 보정

## 관련 메모리
- project_esellers_integration_scope — ES 통합 범위
- project_alt_text_injection — alt 주입 공통 적용
