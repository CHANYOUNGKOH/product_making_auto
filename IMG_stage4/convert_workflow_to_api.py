"""
워크플로우 JSON을 ComfyUI API 형식으로 변환하는 스크립트

사용법:
    python convert_workflow_to_api.py <입력_JSON_파일> [출력_JSON_파일]

예시:
    python convert_workflow_to_api.py "배경생성 _경량ver.1.json" "배경생성 _경량ver.1_API.json"
"""

import json
import sys
import os
from typing import Dict, Any


def convert_workflow_to_api_format(workflow: Dict[str, Any]) -> Dict[str, Any]:
    """
    ComfyUI 워크플로우 JSON을 API 제출 형식으로 변환합니다.
    nodes 배열을 딕셔너리 형태로 변환하고, class_type 필드를 추가합니다.
    links 정보를 inputs에 반영합니다.
    """
    if "nodes" in workflow:
        # nodes 배열이 있는 경우 (JSON 파일 형식)
        api_workflow = {}
        nodes_list = workflow["nodes"]
        links_list = workflow.get("links", [])
        
        if not isinstance(nodes_list, list):
            raise Exception(f"워크플로우 'nodes'가 배열이 아닙니다 (타입: {type(nodes_list)})")
        
        # 원본 노드 정보 저장 (links 처리 시 outputs 정보 필요)
        original_nodes = {}
        
        # 먼저 모든 노드를 변환
        for idx, node in enumerate(nodes_list):
            if not isinstance(node, dict):
                print(f"경고: 노드[{idx}]가 딕셔너리가 아닙니다 (타입: {type(node)})")
                continue
                
            node_id = str(node.get("id", ""))
            if node_id:
                # 원본 노드 정보 저장 (outputs 정보 포함)
                original_nodes[node_id] = node
                
                # ComfyUI API 형식: class_type과 inputs만 필요 (UI 필드 제거)
                api_node = {}
                
                # type을 class_type으로 변환
                if "type" in node:
                    api_node["class_type"] = node["type"]
                elif "class_type" in node:
                    api_node["class_type"] = node["class_type"]
                else:
                    print(f"경고: 노드 {node_id}에 class_type이 없습니다.")
                    continue
                
                # inputs 초기화
                api_node["inputs"] = {}
                
                # widgets_values가 있으면 inputs에 반영 (LoadImage 등)
                if "widgets_values" in node and isinstance(node["widgets_values"], list):
                    widgets = node["widgets_values"]
                    # LoadImage의 경우: [filename, image_type]
                    if api_node.get("class_type") == "LoadImage" and len(widgets) >= 1:
                        api_node["inputs"]["image"] = widgets[0]
                        print(f"노드 {node_id}: widgets_values에서 image 설정: {widgets[0]}")
                
                api_workflow[node_id] = api_node
        
        # links 정보를 inputs에 반영
        # links 형식: [link_id, source_node_id, source_slot, dest_node_id, dest_slot, type]
        for link in links_list:
            if not isinstance(link, list) or len(link) < 6:
                continue
            
            link_id, src_node_id, src_slot, dest_node_id, dest_slot, link_type = link[0], link[1], link[2], link[3], link[4], link[5]
            dest_node_id_str = str(dest_node_id)
            src_node_id_str = str(src_node_id)
            
            if dest_node_id_str in api_workflow:
                dest_node = api_workflow[dest_node_id_str]
                if "inputs" not in dest_node:
                    dest_node["inputs"] = {}
                
                # 목적지 노드의 입력 이름 찾기 (dest_slot에 해당하는 입력)
                input_name = None
                if dest_node_id_str in original_nodes:
                    dest_node_original = original_nodes[dest_node_id_str]
                    inputs_original = dest_node_original.get("inputs", [])
                    if isinstance(inputs_original, list) and dest_slot < len(inputs_original):
                        input_info = inputs_original[dest_slot]
                        if isinstance(input_info, dict):
                            input_name = input_info.get("name")
                        elif isinstance(input_info, str):
                            input_name = input_info
                    
                    # inputs가 리스트가 아니거나 dest_slot을 찾을 수 없으면, class_type에 따라 기본 입력 이름 사용
                    if not input_name:
                        class_type = dest_node.get("class_type", "")
                        # SaveImage는 "images" 입력을 사용
                        if class_type == "SaveImage":
                            input_name = "images"
                        # 다른 노드들은 일반적으로 출력 타입 이름을 사용 (예: IMAGE, LATENT, CONDITIONING 등)
                        else:
                            # 소스 노드의 출력 타입을 확인
                            if src_node_id_str in original_nodes:
                                src_node_original = original_nodes[src_node_id_str]
                                outputs = src_node_original.get("outputs", [])
                                if isinstance(outputs, list) and src_slot < len(outputs):
                                    output_info = outputs[src_slot]
                                    if isinstance(output_info, dict):
                                        output_type = output_info.get("type", "")
                                        # 타입을 입력 이름으로 변환 (예: IMAGE -> IMAGE, LATENT -> samples 등)
                                        if output_type == "IMAGE":
                                            input_name = "IMAGE"
                                        elif output_type == "LATENT":
                                            input_name = "samples"
                                        elif output_type == "CONDITIONING":
                                            input_name = "conditioning"
                                        elif output_type == "MODEL":
                                            input_name = "model"
                                        elif output_type == "VAE":
                                            input_name = "vae"
                                        elif output_type == "CLIP":
                                            input_name = "clip"
                                        elif output_type == "IPADAPTER":
                                            input_name = "ipadapter"
                                        elif output_type == "CLIP_VISION":
                                            input_name = "clip_vision"
                                        else:
                                            # 기본값: 타입 이름을 소문자로 변환
                                            input_name = output_type.lower() if output_type else "input"
                
                if input_name:
                    # inputs에 연결 정보 추가: [source_node_id, source_slot]
                    dest_node["inputs"][input_name] = [src_node_id_str, src_slot]
                    print(f"링크 추가: 노드 {dest_node_id_str}.inputs['{input_name}'] = [{src_node_id_str}, {src_slot}]")
        
        print(f"워크플로우 변환 완료: {len(api_workflow)}개 노드 (nodes 배열 -> 딕셔너리)")
        return api_workflow
    else:
        # 이미 딕셔너리 형태인 경우 (이미 변환됨)
        print("워크플로우가 이미 API 형식입니다 (변환 불필요)")
        return workflow


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    input_file = sys.argv[1]
    
    if not os.path.exists(input_file):
        print(f"오류: 파일을 찾을 수 없습니다: {input_file}")
        sys.exit(1)
    
    # 출력 파일명 결정
    if len(sys.argv) >= 3:
        output_file = sys.argv[2]
    else:
        # 입력 파일명에 "_API" 추가
        base, ext = os.path.splitext(input_file)
        output_file = f"{base}_API{ext}"
    
    print(f"입력 파일: {input_file}")
    print(f"출력 파일: {output_file}")
    print("-" * 50)
    
    # JSON 로드
    try:
        with open(input_file, "r", encoding="utf-8") as f:
            workflow = json.load(f)
    except Exception as e:
        print(f"오류: JSON 파일 로드 실패: {e}")
        sys.exit(1)
    
    # 변환
    try:
        api_workflow = convert_workflow_to_api_format(workflow)
    except Exception as e:
        print(f"오류: 워크플로우 변환 실패: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    # 저장
    try:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(api_workflow, f, ensure_ascii=False, indent=2)
        print("-" * 50)
        print(f"✅ 변환 완료: {output_file}")
        print(f"   노드 개수: {len(api_workflow)}개")
    except Exception as e:
        print(f"오류: 파일 저장 실패: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

