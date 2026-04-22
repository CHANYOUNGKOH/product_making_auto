# 이셀러스 업로드 경로

## 엑셀 업로드 방식

78컬럼 이셀러스 엑셀 → ESM+ 판매자센터 "대량등록" 수동 업로드.

## 자동화 가능 여부

- **ESM+**: Playwright 자동 업로드 가능 (미구현)
- **API**: ESM+ 공식 API 없음

## 변환 단계

```
OC 원본 (ownerclan API or 엑셀)
  ↓
OC_ES_converter 또는 Upload_Mapper/solutions/esellers.py
  ↓
이셀러스 엑셀 (78컬럼)
  ↓
ESM+ 어드민 업로드
```

## 관련 코드
- [Upload_Mapper/solutions/esellers.py](../../../Upload_Mapper/solutions/esellers.py)
- [Upload_Mapper/main.py](../../../Upload_Mapper/main.py) — UI 런처
