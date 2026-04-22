# ESM (옥션/지마켓) — 마켓 허브

옥션 + 지마켓 공용 ESM+ 경로. 이셀러스([../esellers/](../esellers/))를 통해 업로드.

## 운영 스토어
- 옥션: 19 계정
- 지마켓: 17 계정

## 타입 용도 분기
- **A타입 (유배, J=2%)**: 네이버 가격비교탭 노출용 — 할인후가가 실 경쟁가
- **B타입 (무배, J=61%)**: 옥션/지마켓 내부 검색순위 상승용 — 큰 할인율이 검색 알고리즘에서 가점

## 문서 지도

| 파일 | 상태 |
|---|---|
| [pricing.md](pricing.md) | ✅ 옥션/지마켓 공통 (A/B 타입) |
| [columns.md](columns.md) | 🔴 TBD — 이셀러스 엑셀 78컬럼 정책 (`../esellers/columns.md` 참조) |
| [stores.md](stores.md) | 🟡 초안 |
| [api.md](api.md) | 🟡 초안 — API 경로 미확정 |
| bulk-ops.md | 🔴 미작성 |

## 관련 코드

- [DB_save/sim_esm.py](../../../DB_save/sim_esm.py) — 시뮬
- [Upload_Mapper/solutions/esellers.py](../../../Upload_Mapper/solutions/esellers.py) — ESM+ 업로드 변환
