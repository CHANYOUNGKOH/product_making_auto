#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Stage2 배치 API 결과에서 프롬프트 캐싱 분석
"""
import json
import sys

def analyze_caching(jsonl_path):
    cached_list = []
    input_list = []
    total_requests = 0
    success_requests = 0
    
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
            try:
                data = json.loads(line)
                total_requests += 1
                
                if 'response' not in data or 'body' not in data['response']:
                    continue
                
                body = data['response']['body']
                if 'usage' not in body:
                    continue
                
                usage = body['usage']
                input_tokens = usage.get('input_tokens', 0)
                input_details = usage.get('input_tokens_details', {})
                cached_tokens = input_details.get('cached_tokens', 0)
                
                cached_list.append(cached_tokens)
                input_list.append(input_tokens)
                success_requests += 1
                
            except Exception as e:
                print(f"파싱 오류: {e}", file=sys.stderr)
                continue
    
    if not cached_list:
        print("분석할 데이터가 없습니다.")
        return
    
    cached_sum = sum(cached_list)
    input_sum = sum(input_list)
    cached_count = sum(1 for x in cached_list if x > 0)
    zero_count = sum(1 for x in cached_list if x == 0)
    
    print("=" * 60)
    print("Stage2 배치 API 프롬프트 캐싱 분석 결과")
    print("=" * 60)
    print(f"총 요청 수: {total_requests}")
    print(f"성공 요청 수: {success_requests}")
    print()
    print("cached_tokens 통계:")
    print(f"  - 최소: {min(cached_list)}")
    print(f"  - 최대: {max(cached_list)}")
    print(f"  - 중앙값: {sorted(cached_list)[len(cached_list)//2]}")
    print(f"  - 평균: {cached_sum / len(cached_list):.2f}")
    print(f"  - 합계: {cached_sum}")
    print(f"  - 0인 요청: {zero_count}/{len(cached_list)} ({zero_count*100/len(cached_list):.1f}%)")
    print(f"  - 캐싱된 요청: {cached_count}/{len(cached_list)} ({cached_count*100/len(cached_list):.1f}%)")
    print()
    print("input_tokens 통계:")
    print(f"  - 최소: {min(input_list)}")
    print(f"  - 최대: {max(input_list)}")
    print(f"  - 중앙값: {sorted(input_list)[len(input_list)//2]}")
    print(f"  - 평균: {input_sum / len(input_list):.2f}")
    print()
    print("캐싱 비율:")
    if input_sum > 0:
        cache_ratio = (cached_sum / input_sum) * 100
        print(f"  - 전체 input_tokens 대비 cached_tokens 비율: {cache_ratio:.2f}%")
    print()
    
    # 캐싱이 발생한 요청들의 input_tokens 분포
    cached_inputs = [input_list[i] for i in range(len(input_list)) if cached_list[i] > 0]
    if cached_inputs:
        print("캐싱이 발생한 요청의 input_tokens:")
        print(f"  - 최소: {min(cached_inputs)}")
        print(f"  - 최대: {max(cached_inputs)}")
        print(f"  - 중앙값: {sorted(cached_inputs)[len(cached_inputs)//2]}")
        print(f"  - 평균: {sum(cached_inputs) / len(cached_inputs):.2f}")
    print()
    
    # 캐싱이 발생하지 않은 요청들의 input_tokens 분포
    non_cached_inputs = [input_list[i] for i in range(len(input_list)) if cached_list[i] == 0]
    if non_cached_inputs:
        print("캐싱이 발생하지 않은 요청의 input_tokens:")
        print(f"  - 최소: {min(non_cached_inputs)}")
        print(f"  - 최대: {max(non_cached_inputs)}")
        print(f"  - 중앙값: {sorted(non_cached_inputs)[len(non_cached_inputs)//2]}")
        print(f"  - 평균: {sum(non_cached_inputs) / len(non_cached_inputs):.2f}")
    print("=" * 60)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python analyze_stage2_caching.py <jsonl_file_path>")
        sys.exit(1)
    
    analyze_caching(sys.argv[1])
