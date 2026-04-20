# ESM API · 업로드 경로

## 현황

**공식 API 없음** (옥션/지마켓 단독). 유일한 대량 업로드 경로는:
- **ESM+ 어드민** (이셀러스 변환 엑셀 업로드) — 상품 등록/수정
- **판매자 센터 Playwright** — 마감/주문 처리

## 경로 상세

### ESM+ 업로드
1. OC 원본 → `Upload_Mapper/solutions/esellers.py` 로 변환
2. 78컬럼 엑셀 출력
3. ESM+ 판매자센터 "대량등록" 메뉴 업로드
4. 수동 업로드 (자동화 Playwright 가능)

### 주문/마감
- [파이썬자동화파일/마켓마감기](../../../../..) Playwright 자동화

## 🔴 TBD
- ESM+ 업로드 API 유무 확인 (현재 미확인)
- Playwright 자동 업로드 구현 여부
- rate limit / 대량 업로드 제한

## 관련 코드
- [Upload_Mapper/solutions/esellers.py](../../../Upload_Mapper/solutions/esellers.py) — 변환기 (1,117줄)
- [Upload_Mapper/rules/option_price_correction.py](../../../Upload_Mapper/rules/option_price_correction.py) — 옵션 추가금 보정
