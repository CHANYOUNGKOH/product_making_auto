"""
배치 JSONL 파일 분석 스크립트
- custom_id 중복 체크
- 청크 파일별 요청 수 및 크기 분석
- 전체 통계 제공
"""

import json
import os
import glob
from collections import Counter

def analyze_jsonl_file(jsonl_path):
    """단일 JSONL 파일 분석"""
    if not os.path.exists(jsonl_path):
        print(f"❌ 파일을 찾을 수 없습니다: {jsonl_path}")
        return None
    
    custom_ids = []
    total_size = os.path.getsize(jsonl_path)
    line_count = 0
    
    with open(jsonl_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            line_count += 1
            try:
                obj = json.loads(line)
                custom_id = obj.get('custom_id', '')
                if custom_id:
                    custom_ids.append(custom_id)
            except Exception as e:
                print(f"⚠️ 라인 파싱 오류: {e}")
    
    # 중복 체크
    id_counter = Counter(custom_ids)
    duplicates = {k: v for k, v in id_counter.items() if v > 1}
    
    return {
        'file': os.path.basename(jsonl_path),
        'path': jsonl_path,
        'total_lines': line_count,
        'unique_custom_ids': len(set(custom_ids)),
        'total_custom_ids': len(custom_ids),
        'duplicates': duplicates,
        'duplicate_count': len(duplicates),
        'file_size_mb': total_size / (1024 * 1024),
        'avg_size_per_request_mb': (total_size / line_count) / (1024 * 1024) if line_count > 0 else 0
    }

def analyze_chunk_files(base_dir, pattern):
    """청크 파일들 분석"""
    os.chdir(base_dir)
    files = sorted(glob.glob(pattern))
    
    if not files:
        print(f"❌ 패턴에 맞는 파일을 찾을 수 없습니다: {pattern}")
        return
    
    print(f"=" * 80)
    print(f"📊 배치 JSONL 파일 분석")
    print(f"=" * 80)
    print(f"총 청크 파일 수: {len(files)}\n")
    
    all_custom_ids = []
    all_duplicates = {}
    total_requests = 0
    total_size = 0
    
    # 각 청크 파일 분석
    chunk_results = []
    for i, file_path in enumerate(files, 1):
        result = analyze_jsonl_file(file_path)
        if result:
            chunk_results.append(result)
            all_custom_ids.extend([cid for _ in range(result['total_custom_ids'])])
            all_duplicates.update(result['duplicates'])
            total_requests += result['total_lines']
            total_size += result['file_size_mb']
            
            # 청크별 상세 정보
            status = "⚠️ 중복 있음" if result['duplicate_count'] > 0 else "✅ 정상"
            print(f"청크 {i:2d}: {result['file']}")
            print(f"  - 요청 수: {result['total_lines']}개")
            print(f"  - 고유 custom_id: {result['unique_custom_ids']}개")
            print(f"  - 파일 크기: {result['file_size_mb']:.2f} MB")
            print(f"  - 요청당 평균 크기: {result['avg_size_per_request_mb']:.4f} MB")
            if result['duplicate_count'] > 0:
                print(f"  - ⚠️ 중복 custom_id: {result['duplicate_count']}개")
                for dup_id, count in list(result['duplicates'].items())[:5]:
                    print(f"      • {dup_id}: {count}회")
                if result['duplicate_count'] > 5:
                    print(f"      ... 외 {result['duplicate_count'] - 5}개")
            print(f"  - 상태: {status}\n")
    
    # 전체 통계
    print(f"=" * 80)
    print(f"📈 전체 통계")
    print(f"=" * 80)
    print(f"총 청크 파일 수: {len(files)}")
    print(f"총 요청 수: {total_requests}개")
    print(f"고유 custom_id 수: {len(set(all_custom_ids))}개")
    print(f"총 파일 크기: {total_size:.2f} MB")
    print(f"평균 청크 크기: {total_size / len(files):.2f} MB")
    print(f"요청당 평균 크기: {(total_size * 1024 * 1024) / total_requests / (1024 * 1024):.4f} MB")
    
    # 전체 중복 체크
    all_id_counter = Counter(all_custom_ids)
    all_duplicates_final = {k: v for k, v in all_id_counter.items() if v > 1}
    
    if all_duplicates_final:
        print(f"\n⚠️ 전체 중복 custom_id: {len(all_duplicates_final)}개")
        print(f"중복된 요청 총 수: {sum(v - 1 for v in all_duplicates_final.values())}개")
        print(f"\n중복된 custom_id 목록 (처음 10개):")
        for dup_id, count in list(all_duplicates_final.items())[:10]:
            print(f"  • {dup_id}: {count}회 (중복 {count - 1}회)")
        if len(all_duplicates_final) > 10:
            print(f"  ... 외 {len(all_duplicates_final) - 10}개")
        
        # 중복이 발생한 청크 파일 찾기
        print(f"\n중복이 발생한 청크 파일:")
        for result in chunk_results:
            if result['duplicate_count'] > 0:
                print(f"  - {result['file']}: {result['duplicate_count']}개 중복")
    else:
        print(f"\n✅ 중복 없음: 모든 custom_id가 고유합니다.")
    
    # 예상 토큰 사용량 추정 (대략적)
    # 각 요청당 평균 크기를 기반으로 추정
    avg_size_per_request = (total_size * 1024 * 1024) / total_requests if total_requests > 0 else 0
    print(f"\n💡 참고 정보:")
    print(f"  - 요청당 평균 크기: {avg_size_per_request / (1024 * 1024):.4f} MB")
    print(f"  - 180MB 기준 예상 청크 수: {total_size / 180:.1f}개")
    print(f"  - 실제 청크 수: {len(files)}개")
    if total_size / 180 < len(files):
        print(f"  - ⚠️ 예상보다 {len(files) - int(total_size / 180)}개 더 많은 청크가 생성되었습니다.")
    
    print(f"\n" + "=" * 80)

if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("사용법:")
        print("  python analyze_batch_jsonl.py <JSONL_파일_경로_또는_디렉토리>")
        print("\n예시:")
        print("  python analyze_batch_jsonl.py 중복제거_신년DB오너클랜_1_T1_I3_stage2_batch_input.jsonl")
        print("  python analyze_batch_jsonl.py . 중복제거_신년DB오너클랜_1_T1_I3_stage2_batch_input_chunk*.jsonl")
        sys.exit(1)
    
    target = sys.argv[1]
    
    if os.path.isfile(target):
        # 단일 파일 분석
        result = analyze_jsonl_file(target)
        if result:
            print(f"📊 파일 분석: {result['file']}")
            print(f"요청 수: {result['total_lines']}개")
            print(f"고유 custom_id: {result['unique_custom_ids']}개")
            print(f"파일 크기: {result['file_size_mb']:.2f} MB")
            if result['duplicate_count'] > 0:
                print(f"⚠️ 중복 custom_id: {result['duplicate_count']}개")
                for dup_id, count in result['duplicates'].items():
                    print(f"  • {dup_id}: {count}회")
            else:
                print(f"✅ 중복 없음")
    elif os.path.isdir(target):
        # 디렉토리에서 패턴 검색
        if len(sys.argv) >= 3:
            pattern = sys.argv[2]
        else:
            pattern = "*_stage2_batch_input*.jsonl"
        analyze_chunk_files(target, pattern)
    else:
        print(f"❌ 파일 또는 디렉토리를 찾을 수 없습니다: {target}")
