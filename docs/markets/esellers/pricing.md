# 이셀러스 가격 전략

이셀러스는 자체 가격 정책 없음 — **ESM/11번가/쿠팡 등 타겟 마켓의 정책을 상속**.

## 상속 경로

| 타겟 마켓 | 적용 pricing |
|---|---|
| 옥션 | [../esm/pricing.md](../esm/pricing.md) |
| 지마켓 | [../esm/pricing.md](../esm/pricing.md) |
| 11번가 | [../11st/pricing.md](../11st/pricing.md) |
| 쿠팡(예정) | — |

## 변환 시 역할

`Upload_Mapper/solutions/esellers.py` 는:
1. 각 상품의 **대상 마켓 판정** (시트 또는 카테고리로)
2. 대상 마켓의 pricing_strategies 적용 → 등록가 계산
3. 이셀러스 엑셀의 "판매가*" 필드에 기록

즉 이 파일은 가격을 **설정**하지 않고 **적용**만.
