# 고도몰 벌크 업데이트 — 플레이북 & 성능 최적화

> 등록 상품 대량 수정(가격/brandCd/속성 일괄 변경) 시 표준 절차.
> 연관 스킬: `godomall-bulk-update` (`~/.claude/skills/`).

## 언제 사용

- 가격 정책 변경으로 79,200건(8 스토어 × 9,900) 재계산
- brandCd 일괄 정규화
- goods_state / goodsDisplayFl 토글
- 잘못 등록된 필드 정정

## 3단계 안전 플레이북

### Phase A — read-only 샘플 10건
- `scripts/phase2a_sample_analysis.py`
- 1 스토어 × page=1 × size=10 fetch
- 원가(costPrice) 기반 신 정책 계산 → diff 표 + 어드민 직링크 출력
- **write API 금지**. 사용자 어드민 확인 후 승인 → B.

### Phase B — 샘플 write + 검증 (동일 10건)
- `scripts/phase2b_sample_update.py`
- SIMPLE_FIELDS 전체 복사 + goodsPrice/brandCd 덮어쓰기 ([api.md](api.md#🔒-중요-제약--partial-update-미지원))
- `build_product_xml(mode="update")` + `XmlHost.upload()` + `api.update_product(data_url)`
- 변경 후 즉시 재조회 → 예상 vs 실제 일치 검증
- 에러 0 + 100% 일치 → C.

### Phase C — 전체 확장 (스토어별 9,900건 × 활성 8 스토어)
- `scripts/phase2c_full_store_update.py`
- `get_all_products(max_workers=4)` 병렬 fetch
- **Snapshot JSONL 저장** (rollback용, `godomall_register/logs/phase2c/`)
- `ThreadPoolExecutor(max_workers=8)` 업데이트
- 체크포인트 200건마다 진행률/rate/ETA 출력
- R2 cleanup 500건 배치

### Phase C 병렬 실행 (8 스토어 동시)
```bash
for s in "고도몰A1-1" "고도몰A1-2" "고도몰A1-3" "고도몰A4-1" \
         "고도몰B1-1" "고도몰B1-2" "고도몰B1-3" "고도몰B2-1"; do
  (python scripts/phase2c_full_store_update.py "$s" > logs/${s}.log 2>&1 &)
done
```
활성 8 스토어 (A4-2/A4-3 제외 — [stores.md](stores.md)).

## 관찰된 병목과 최적화

### 2026-04-20 Phase 2C 실측
- 설계: 스토어당 8 워커 × 4 스토어 = 32 concurrent
- 실측: 스토어당 **2.7/s**, 총 11/s (스펙 66/s의 **4%**)
- 글로벌 병목 (A 끝난 후 B만 남아도 속도 동일)

### 원인
1. **boto3 S3 client 연결 풀 기본 10** → 32 concurrent 업로드 시 22 대기
2. **data_url 방식 왕복** → 상품당 R2 업로드 ~100ms + godomall fetch+process ~300ms
3. **Cloudflare 측 IP rate limit 가능성**

### Fix 1 (적용됨) — boto3 pool 64
[godomall_register/xml_host.py](../../../godomall_register/xml_host.py) `_get_client()`:
```python
config=BotoConfig(
    signature_version="s3v4",
    retries={"max_attempts": 3, "mode": "adaptive"},
    max_pool_connections=64,  # 기본 10 → 64
)
```

### Fix 2 (검증됨) — Multi-goods XML 배치
`build_product_xml([p1, ..., p50], mode="update")` 는 이미 list 지원. API도 multi-goods_data 응답 반환 확인 (phase2d_batch_test.py).

- **1 XML에 50 goods_data** → 1 API 호출로 50건 업데이트 = 0.67초
- 예상 성능: 9,900건/스토어 **5~8분** (기존 60분 대비 **10배**)
- 구현 대기: `api_client.update_products_batch(products, batch_size=50)` 신규 메서드 + list 응답 per-item 파싱

### Fix 3 (장기 backlog) — 로컬 HTTP 터널로 R2 제거
cloudflared tunnel / ngrok 로 로컬 XML 서버 공개 → R2 왕복 제거.

## Rollback

- Phase 2C 실행 전 스냅샷 JSONL이 자동 저장됨 (`logs/phase2c/<store>_<ts>_snapshot.jsonl`)
- 스냅샷 포맷: goodsNo, goodsCd, goodsPrice, brandCd, costPrice
- rollback 스크립트 미작성 (필요 시 snapshot→update 역방향 재실행)

## 안전 체크리스트

- [ ] Phase A 승인받았나?
- [ ] Phase B 100% 일치 확인했나?
- [ ] 비활성 스토어(A4-2, A4-3) 제외했나?
- [ ] 스냅샷 JSONL 저장됐나?
- [ ] 체크포인트 로그에서 err 증가 없는지 모니터링
- [ ] rate limit EXHAUSTED/429 로그 주기 확인

## 주의

- **옵션 있는 상품**: optionData 재전송 필요. 현 phase2c 스크립트는 가격/brandCd만 변경하고 옵션은 SIMPLE_FIELDS 패스스루 — 옵션 자체 바꾸려면 별도 설계
- **B 스토어 오등록 가능성**: 과거 잘못 등록된 상품이 brandCd는 001이지만 가격이 다른 전략 값일 수 있음. Phase 2A에서 price/cost 비율로 감지 가능 (1.8 이상 의심)
- **partner_key 분리**: 본사/공급사 키 다르면 조회 결과도 다름 ([api.md](api.md#주의))

## 실행 이력

- 2026-04-20: Phase 2C 완료. 67,039 업데이트 / 12,161 skip_same / 0 err. 8 스토어 약 60분.
