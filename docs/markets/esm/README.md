# ESM (옥션/지마켓) — 마켓 허브

옥션 + 지마켓 공용 ESM+ 경로. 이셀러스([../esellers/](../esellers/))를 통해 업로드.

## 운영 스토어
- 옥션: 19 계정
- 지마켓: 17 계정

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
