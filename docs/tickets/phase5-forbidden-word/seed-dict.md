# Phase 5 금지어 Seed 사전

Phase 5에서 바로 이어서 쓸 수 있도록 금지어 사전의 최소 구조를 먼저 고정합니다. 실제 금지어 데이터는 운영자가 나중에 채웁니다.

## 컬럼 정의

| 컬럼 | 필수 | 설명 |
| --- | --- | --- |
| `term` | 예 | 차단할 실제 금지어입니다. 아직 미정이면 placeholder를 유지합니다. |
| `severity` | 예 | 위험도입니다. `HIGH`, `MED`, `LOW` 중 하나를 사용합니다. |
| `scope` | 예 | Hub canonical key 기준 적용 범위입니다. `all`, `naver`, `esm`, `st11`, `coupang`, `interpark`, `tmon`, `wmp` 중 하나를 사용합니다. |
| `reason` | 예 | 왜 차단해야 하는지 운영 기준을 짧게 적습니다. |
| `source` | 예 | 운영 메모, 마켓 정책, 내부 검수 등 규칙의 출처를 적습니다. |
| `added_at` | 예 | `YYYY-MM-DD` 형식의 등록일입니다. |

## scope 작성 원칙

- `scope`는 마켓 표시 이름이 아니라 Hub 내부 canonical key를 사용합니다.
- 예를 들어 고도몰/스마트스토어 계열은 `naver`, 옥션/지마켓 계열은 `esm`으로 통일합니다.
- 모든 마켓에 공통 적용할 때만 `all`을 사용합니다.

## Seed 예시

| term | severity | scope | reason | source | added_at |
| --- | --- | --- | --- | --- | --- |
| `<금지어-예시-1>` | HIGH | all | 운영자가 확정한 공통 금지어를 나중에 대체 입력합니다. | operator_manual_seed | 2026-04-22 |
| `<금지어-예시-2>` | MED | st11 | 11번가 계열에만 적용할 금지어 예시 placeholder입니다. | operator_manual_seed | 2026-04-22 |

## 운영 메모

- 금지어 1개당 1행을 유지합니다.
- 실제 금지어가 준비되지 않았다면 placeholder를 지우지 말고 그대로 둡니다.
- 사람 친화적 표시는 별도 UI/문서에서 다루고, 저장값은 canonical key로 통일합니다.
