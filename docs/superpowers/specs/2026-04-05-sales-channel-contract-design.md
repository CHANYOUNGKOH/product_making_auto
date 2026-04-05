# Design: 판매처 계약 정규화 (`sales_channel_policy.py`)

**Date**: 2026-04-05
**Status**: Approved (brainstorming 완료)
**Scope**: 판매채널 3계층 정책 데이터 중앙 관리 — 가격계산 로직은 별도
**Depends on**: 없음 (독립 모듈)

---

## 목적

현재 DB_save/config.py, store_memos.json, Upload_Mapper, workflow_master.md 등에
흩어진 판매채널 정책(수수료, 배송전략, 할인율 등)을 **하나의 JSON + 읽기 함수**로 중앙 관리.

다른 모듈은 `sales_channel_policy.py`의 함수를 통해서만 정책을 읽는다.

---

## 왜 필요한가

1. **정책 데이터 파편화** — 6개 이상 파일에 흩어져 있어 변경 시 누락 발생
2. **마켓 추가/정지 대응** — 중앙 구조 없이는 새 마켓/스토어 추가 시 여러 파일 수정 필요
3. **가격계산 엑셀 분해의 전제조건** — 수수료율, 할인율, 배송전략이 정규화되어야 엑셀을 코드로 대체 가능
4. **시스템 간 연결 기반** — 다른 로컬/사용자가 같은 구조로 운영할 수 있는 계약

---

## 이 모듈이 하는 것 / 하지 않는 것

### 하는 것
- 오픈마켓별 수수료율/수수료기준/기본할인율 관리
- 명의자별 기본 배송전략/배송비 관리
- 스토어별 정책 오버라이드 관리
- 스토어 상태 관리 (active/suspended/closed)
- 새 스토어 등록용 템플릿 제공
- 정책 병합 (마켓 + 명의자 기본값 + 스토어 오버라이드)

### 하지 않는 것
- 가격계산 (마진율, 판매가율 룩업, 쿠폰 — 미래 가격계산 모듈)
- 도매가/배송비 동기화 (ownerclan_sync.py가 담당)
- API 키/인증 관리 (godomall_register/config.py 등 기존 유지)
- 카테고리 매핑 (별도 모듈)
- 상품별 실제 적용값 결정 (가격계산 모듈이 계약 + 도매데이터 조합)

---

## 3계층 구조

```
오픈마켓 (market)
  수수료율, 수수료기준, 기본할인율, 상태
    │
    ├─ 명의자 (owner) — 배송 기본 정책
    │    기본 배송전략, 기본 배송비
    │      │
    │      └─ 스토어 (store) — 오버라이드
    │           배송전략, 배송비, 할인율, 상태
    │
    └─ 템플릿 (template)
         새 스토어 추가 시 복제하는 기본값 세트
```

**상속 규칙**:
- 할인율: 스토어 `null` → 마켓 기본할인율 사용. 스토어에 값이 있으면 오버라이드.
- 배송전략/배송비: 스토어 `null` → 명의자 기본값 사용. 스토어에 값이 있으면 오버라이드.
- 수수료율/수수료기준: 항상 마켓 레벨 (스토어 오버라이드 없음).

---

## 비즈니스 로직 (정책 결정의 이유)

### 할인율 전략

마켓별로 할인율을 다르게 설정하는 이유:

- **스마트스토어 50%**: 수수료가 실결제금액(할인가) 기준이므로, 높은 할인율로 판매가를 높게
  설정 → 옵션가격이 판매가에 연동되므로 옵션가격으로 마진 확보. 70% 이상은 노출 제한 위험.
- **옥션/지마켓 2% 또는 61%**: 같은 ESM 플랫폼이지만 스토어별로 두 가지 전략 선택 가능.
- **나머지 마켓 2%**: 수수료가 할인 전 판매가 기준이므로 할인율을 낮게 유지.
- **고도몰**: 할인율 개념 없음. 자체 쿠폰정책으로 운영 (가격대별 4단계, 위탁주문가 이상만).

### 배송전략

명의자별 기본 전략이 다른 이유:

- **명의자A (유료배송 3000원)**: 배송비를 구매자에게 직접 받음.
  도매 배송비 5000원 → 3000원으로 판매, 차액 2000원은 제품가에 흡수.
  도매 무배 상품 → 3000원 배송비가 추가 마진이 됨.
- **명의자B (무료배송)**: 배송비를 제품가격에 흡수.

### 배송비 옵션

모든 마켓 공통으로 사용 가능한 배송비 값:

| 배송비 | 유형 | 비고 |
|--------|------|------|
| 0 | 무료배송 | |
| 3000 | 유료배송 기본 | 가장 빈번 |
| 착불 10000 | 착불배송 | 대형/무거운 상품, 빈도 낮음 |
| 3500~7000 | 유료배송 확장 | 추후 도입 예정 (3500, 4000, 4500, 5000, 6000, 7000) |

스토어의 `shipping_fee`는 해당 스토어의 **기본 배송비**이며, 실제 상품별 적용은
가격계산 모듈이 도매 배송비와 조합하여 결정한다.

### 수수료 적용 방식 차이

같은 수수료율이라도 적용 기준에 따라 마진이 달라짐:

- **할인전 기준 (`pre_discount`)**: 대부분의 마켓. 할인 전 판매가에 수수료 부과.
- **할인후 기준 (`post_discount`)**: 스마트스토어, 고도몰. 실결제금액에 수수료 부과.
  → 할인율을 높여도 수수료 부담이 줄어들어 옵션가격 마진 전략이 가능.

---

## 계약 (Contract)

### 1. 데이터 계약 — `DB_save/sales_channels.json`

```json
{
  "markets": {
    "쿠팡": {
      "commission_rate": 11.5,
      "commission_base": "pre_discount",
      "default_discount_rate": 2,
      "status": "pending"
    },
    "11번가": {
      "commission_rate": 17.5,
      "commission_base": "pre_discount",
      "default_discount_rate": 2,
      "status": "registering"
    },
    "옥션": {
      "commission_rate": 16.5,
      "commission_base": "pre_discount",
      "default_discount_rate": 2,
      "status": "registering",
      "note": "스토어별 2% 또는 61% 선택 가능"
    },
    "지마켓": {
      "commission_rate": 16.5,
      "commission_base": "pre_discount",
      "default_discount_rate": 2,
      "status": "registering",
      "note": "스토어별 2% 또는 61% 선택 가능"
    },
    "스마트스토어": {
      "commission_rate": 6.5,
      "commission_base": "post_discount",
      "default_discount_rate": 50,
      "status": "registering",
      "note": "할인율 70% 미만 유지 필요 (노출 제한 위험). 옵션가격이 판매가 연동 → 옵션으로 마진 확보 전략"
    },
    "롯데온": {
      "commission_rate": 16.5,
      "commission_base": "pre_discount",
      "default_discount_rate": 2,
      "status": "planned"
    },
    "카카오톡스토어": {
      "commission_rate": 10.0,
      "commission_base": "pre_discount",
      "default_discount_rate": 2,
      "status": "planned"
    },
    "올웨이즈": {
      "commission_rate": 12.0,
      "commission_base": "pre_discount",
      "default_discount_rate": 2,
      "status": "planned"
    },
    "에이블리": {
      "commission_rate": 18.0,
      "commission_base": "pre_discount",
      "default_discount_rate": 2,
      "status": "planned"
    },
    "토스쇼핑": {
      "commission_rate": 15.0,
      "commission_base": "pre_discount",
      "default_discount_rate": 2,
      "status": "planned"
    },
    "멸치쇼핑": {
      "commission_rate": 16.5,
      "commission_base": "pre_discount",
      "default_discount_rate": 2,
      "status": "planned"
    },
    "오늘의집": {
      "commission_rate": 20.0,
      "commission_base": "pre_discount",
      "default_discount_rate": 2,
      "status": "planned"
    },
    "고도몰": {
      "commission_rate": 4.0,
      "commission_base": "post_discount",
      "default_discount_rate": null,
      "status": "pending",
      "note": "할인율 개념 없음. 실결제금액 기준 수수료. 자체쿠폰정책: 가격대별 4단계, 위탁주문가 이상만 쿠폰 발행"
    }
  },

  "owners": {
    "A": {
      "name": "고찬영",
      "default_shipping_strategy": "paid",
      "default_shipping_fee": 3000
    },
    "B": {
      "name": "최빛나",
      "default_shipping_strategy": "free",
      "default_shipping_fee": 0
    }
  },

  "stores": {
    "쿠팡A2-1": {
      "market": "쿠팡",
      "owner": "A",
      "business_code": "A2",
      "business_name": "마이유통",
      "store_num": "1",
      "status": "active",
      "shipping_strategy": null,
      "shipping_fee": null,
      "discount_rate": null
    }
  },

  "templates": {
    "owner_A_default": {
      "shipping_strategy": "paid",
      "shipping_fee": 3000
    },
    "owner_B_default": {
      "shipping_strategy": "free",
      "shipping_fee": 0
    }
  }
}
```

#### 필드 정의

**markets**

| 필드 | 타입 | 설명 |
|------|------|------|
| `commission_rate` | float | 마켓 수수료율 (%) |
| `commission_base` | string | 수수료 적용 기준: `pre_discount` (할인전 판매가), `post_discount` (할인후 실결제금액) |
| `default_discount_rate` | float? | 마켓 기본 할인율 (%). null이면 할인 개념 없음 (고도몰) |
| `status` | string | `registering` (등록중), `pending` (등록예정), `planned` (예정), `active` (활성), `suspended` (정지) |
| `note` | string? | 비즈니스 로직 설명, 특이사항 |

**owners**

| 필드 | 타입 | 설명 |
|------|------|------|
| `name` | string | 명의자 이름 |
| `default_shipping_strategy` | string | 기본 배송전략: `paid` (유료), `free` (무료), `collect` (착불) |
| `default_shipping_fee` | int | 기본 배송비 (원). 유료=3000, 무료=0, 착불=10000 |

**stores**

| 필드 | 타입 | 설명 |
|------|------|------|
| `market` | string | 소속 오픈마켓 (markets 키 참조) |
| `owner` | string | 소속 명의자 (owners 키 참조) |
| `business_code` | string | 사업자코드 (A1, B3 등) |
| `business_name` | string | 상호명 (굿투굿, 샤인몰 등) |
| `store_num` | string | 스토어 번호 |
| `status` | string | `active`, `suspended`, `closed` |
| `shipping_strategy` | string? | null이면 명의자 기본값 상속 |
| `shipping_fee` | int? | null이면 명의자 기본값 상속 |
| `discount_rate` | float? | null이면 마켓 기본할인율 상속 |

**templates**

새 스토어 등록 시 복제하는 배송 정책 기본값 세트. 명의자별 기본 템플릿 제공.

---

### 2. 함수 계약 — `DB_save/sales_channel_policy.py`

```python
from DB_save.sales_channel_policy import (
    get_market_policy,
    get_store_policy,
    list_active_stores,
    get_template,
)
```

#### `get_market_policy(market_name: str) -> dict`

```python
get_market_policy("쿠팡")
# 반환:
# {
#     "commission_rate": 11.5,
#     "commission_base": "pre_discount",
#     "status": "pending"
# }
```

#### `get_store_policy(store_alias: str) -> dict`

마켓 정책 + 명의자 기본값 + 스토어 오버라이드를 **병합**하여 반환.

```python
get_store_policy("쿠팡A2-1")
# 반환:
# {
#     "store_alias": "쿠팡A2-1",
#     "market": "쿠팡",
#     "owner": "A",
#     "business_code": "A2",
#     "business_name": "마이유통",
#     "store_num": "1",
#     "status": "active",
#     "commission_rate": 11.5,        ← 마켓에서
#     "commission_base": "pre_discount", ← 마켓에서
#     "discount_rate": 2,              ← 마켓 기본할인율 (스토어 null)
#     "shipping_strategy": "paid",     ← 명의자 기본값 (스토어 null)
#     "shipping_fee": 3000,            ← 명의자 기본값 (스토어 null)
# }
```

#### `list_active_stores(market: str | None = None) -> list[dict]`

```python
list_active_stores()           # 전체 active 스토어
list_active_stores("쿠팡")     # 쿠팡 active 스토어만
```

#### `get_template(template_name: str) -> dict`

```python
get_template("owner_A_default")
# 반환: {"shipping_strategy": "paid", "shipping_fee": 3000, "discount_rate": 2}
```

---

### 계약의 소비자

| 소비자 | 호출 시점 | 용도 |
|--------|----------|------|
| 가격계산 모듈 (미래) | 가격 산출 시 | 수수료율, 배송전략, 할인율 읽기 |
| 데이터출고 | 출고 대상 결정 | `list_active_stores()` → 활성 스토어 |
| Upload_Mapper | 출고 파일 생성 시 | 배송비, 할인율 적용 |
| 미래 UI | 스토어 추가/수정 | 템플릿 복제, 정책 수정 |
| 미래 주문관리 | 마켓별 입금액 계산 | 수수료율, 수수료기준 |

---

## 파일 구조

```
DB_save/
├── sales_channels.json          ← 신규: 3계층 정책 데이터
├── sales_channel_policy.py      ← 신규: 읽기 함수 (계약 인터페이스)
├── config.py                    ← 기존 유지 (OWNER_NAMES, BUSINESS_NAMES 등)
├── store_memos.json             ← 기존 유지 (카테고리 등 비정책 데이터 남음)
└── products.db                  ← 기존 유지
```

### 기존 파일과의 관계

- `config.py`의 `OWNER_NAMES`, `BUSINESS_NAMES` → `sales_channels.json`의 owners에 통합되지만, config.py는 삭제하지 않음 (다른 모듈 의존)
- `store_memos.json`의 "할N_배N" 정책 부분 → `sales_channels.json` stores로 이전. 카테고리 등 비정책 데이터는 store_memos.json에 유지
- `Upload_Mapper/config_manager.py`의 `price_calculation` 빈 필드 → 미래에 `get_store_policy()`에서 읽도록 전환

---

## 에러 처리

| 상황 | 처리 |
|------|------|
| JSON 파일 없음 | FileNotFoundError 예외 (호출자 처리) |
| 존재하지 않는 마켓명 | KeyError → 명확한 에러 메시지 |
| 존재하지 않는 스토어별칭 | KeyError → 명확한 에러 메시지 |
| 스토어의 마켓/명의자가 markets/owners에 없음 | 로드 시 검증 → 경고 로그 |
| JSON 포맷 오류 | JSONDecodeError 예외 |

---

## 초기 데이터

현재 확인된 스토어 목록은 구현 시 `store_memos.json`과 `config.py`에서 자동 추출하여 `sales_channels.json`에 채운다.

마켓별 수수료율은 이 스펙에 확정된 값을 사용:

| 마켓 | 수수료율 | 수수료기준 | 기본할인율 | 상태 |
|------|---------|-----------|----------|------|
| 쿠팡 | 11.5% | 할인전 | 2% | 등록예정 |
| 11번가 | 17.5% | 할인전 | 2% | 등록중 |
| 옥션 | 16.5% | 할인전 | 2% (61% 가능) | 등록중 |
| 지마켓 | 16.5% | 할인전 | 2% (61% 가능) | 등록중 |
| 스마트스토어 | 6.5% | 할인후 | 50% | 등록중 |
| 롯데온 | 16.5% | 할인전 | 2% | 예정 |
| 카카오톡스토어 | 10.0% | 할인전 | 2% | 예정 |
| 올웨이즈 | 12.0% | 할인전 | 2% | 예정 |
| 에이블리 | 18.0% | 할인전 | 2% | 예정 |
| 토스쇼핑 | 15.0% | 할인전 | 2% | 예정 |
| 멸치쇼핑 | 16.5% | 할인전 | 2% | 예정 |
| 오늘의집 | 20.0% | 할인전 | 2% | 예정 |
| 고도몰 | 4.0% | 할인후 | 해당없음 | 등록예정 |

---

## 테스트 전략

`if __name__ == "__main__"` 블록으로 검증:

1. JSON 로드 + 스키마 검증
2. `get_market_policy("쿠팡")` → 수수료율 11.5 확인
3. `get_store_policy("쿠팡A2-1")` → 명의자 기본값 병합 확인
4. `list_active_stores()` → active 스토어만 반환 확인
5. `get_template("owner_A_default")` → 템플릿 값 확인
6. 존재하지 않는 키 → 적절한 에러 확인

---

## 의존성

- `json` (표준 라이브러리)
- `logging` (표준 라이브러리)
- `pathlib` (표준 라이브러리)
- 추가 설치 없음
