# 금지어 운영자 설명 템플릿

운영자가 금지어를 등록하거나 설명할 때 아래 템플릿을 그대로 복사해서 사용합니다.

## 복사용 템플릿

```md
### 금지어 등록 요청
- term:
- severity: HIGH | MED | LOW
- scope: all | naver | esm | st11 | coupang | interpark | tmon | wmp
- reason:
- source:
- added_at: YYYY-MM-DD
- notes:
```

## 작성 가이드

1. `term`에는 실제로 막아야 하는 단어 또는 문구를 정확히 적습니다.
2. `severity`는 운영 리스크 기준으로 고릅니다.
3. `scope`는 마켓 표시 이름 대신 Hub canonical key를 사용합니다.
4. `reason`에는 차단 이유를 한 줄로 적습니다.
5. `source`에는 정책 문서, 운영 메모, 검수 결과 등 근거를 적습니다.

## scope 예시

- `naver`: 고도몰, 스마트스토어 계열
- `esm`: 옥션, 지마켓 계열
- `st11`: 11번가
- `coupang`: 쿠팡
- `interpark`: 인터파크
- `tmon`: 티몬
- `wmp`: 위메프

## 예시

```md
### 금지어 등록 요청
- term: <금지어-예시-1>
- severity: HIGH
- scope: all
- reason: 마켓 정책 또는 내부 검수에서 차단이 필요한 표현입니다.
- source: operator_manual_seed
- added_at: 2026-04-22
- notes: 실제 금지어가 정해지면 placeholder를 교체합니다.
```
