# 마켓별 가격 전략 정책

> 3-전략 체계: 최저가전략 / 일반판매전략 / 광고전략  
> 엔진: `DB_save/price_engine.py` | 설정: `DB_save/pricing_strategies.json`

---

## 마켓별 문서

| 마켓 | 수수료 | 수수료기준 | J | 문서 |
|------|--------|---------|---|------|
| [11번가](./11st.md) | 17.5% | pre_discount | 50% 고정 | [11st.md](./11st.md) |
| [스마트스토어](./smartstore.md) | 6.5% | post_discount | 50% 고정 | [smartstore.md](./smartstore.md) |
| [고도몰](./godomall.md) | 4.0% | post_discount | 가격대별 4단계 | [godomall.md](./godomall.md) |
| [ESM (옥션/지마켓)](./esm.md) | 16.5% | pre_discount | A=2% / B=61% | [esm.md](./esm.md) |

---

## 3-전략 개요

| 전략 | 지표 | 목적 |
|------|------|------|
| 최저가전략 | absolute_s | 등급/판매건수 확보 — 절대값 손실 한도 |
| 일반판매전략 | v_percent | 마진 확보 — 매출대비 V% |
| 광고전략 | v_percent | 광고 ROI / 원가만큼 회수 (높은 V% 목표) |

---

## 주요 파라미터

- **C (마진배수)**: G = 원가 × C + E. H 헤드룸 결정. 마켓 수수료가 높을수록 C를 높여야 함.
- **H (판매가율)**: I = G × H. 엔진이 역산. 최저가<일반<광고 순으로 높아짐.
- **round_mode**: `nearest`(기본) 또는 `ceil`(올림). 최저가전략에서 S≥0 보장 시 `ceil` 사용.
- **min_h**: H 하한. 수수료 낮은 마켓(고도몰 4%)은 H_raw가 낮아 1.0이면 clamped → 0.8로 설정.

---

## 시뮬레이션 파일

| 마켓 | 스크립트 | 최근 시뮬 파일 |
|------|---------|-------------|
| 스마트스토어 | `DB_save/sim_smartstore.py` | `DB_save/스마트스토어_3전략_시뮬_v7.xlsx` |
| 고도몰 | `DB_save/sim_godomall.py` | `DB_save/고도몰_3전략_시뮬_v2.xlsx` |
| 옥션 | `DB_save/sim_esm.py` | `DB_save/옥션_3전략_시뮬_v1.xlsx` |
| 지마켓 | `DB_save/sim_esm.py` | `DB_save/지마켓_3전략_시뮬_v1.xlsx` |
