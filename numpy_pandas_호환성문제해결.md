# 🔧 numpy/pandas 호환성 문제 해결 가이드

## 문제 증상

```
ValueError: numpy.dtype size changed, may indicate binary incompatibility. 
Expected 96 from C header, got 88 from PyObject
```

## 원인

numpy와 pandas가 서로 호환되지 않는 버전으로 설치되었을 때 발생합니다.
- numpy가 업그레이드되었지만 pandas가 이전 버전
- 또는 그 반대의 경우
- 설치 순서 문제

## 해결 방법

### 방법 1: 자동 해결 스크립트 (권장) ⭐

```bash
패키지_재설치_호환성수정.bat 더블클릭
```

이 스크립트가 자동으로:
1. 기존 numpy, pandas 제거
2. numpy 재설치 (호환 버전)
3. pandas 재설치 (numpy 이후)

---

### 방법 2: 수동 해결 (VSCode 터미널)

```bash
# 1. 기존 패키지 제거
pip uninstall -y numpy pandas

# 2. numpy 먼저 설치 (중요!)
pip install "numpy>=1.24.0,<2.0.0"

# 3. pandas 설치
pip install "pandas>=2.0.0,<3.0.0"

# 4. 나머지 패키지 설치
pip install -r requirements.txt
```

---

### 방법 3: 전체 재설치

```bash
# 모든 패키지 제거 후 재설치
pip uninstall -y numpy pandas openpyxl Pillow opencv-python

# requirements.txt로 재설치
pip install -r requirements.txt
```

---

## 호환되는 버전

### Python 3.11 기준

- **numpy**: 1.24.0 ~ 1.26.x (권장: 1.24.0 이상)
- **pandas**: 2.0.0 ~ 2.2.x (권장: 2.0.0 이상)

### 설치 순서 중요!

1. **numpy 먼저** 설치
2. **pandas 나중에** 설치

이 순서를 지켜야 합니다!

---

## 확인 방법

설치 후 테스트:

```python
python -c "import numpy; import pandas; print('numpy:', numpy.__version__); print('pandas:', pandas.__version__)"
```

정상 출력 예시:
```
numpy: 1.24.3
pandas: 2.0.3
```

---

## 예방 방법

### requirements.txt 사용

`requirements.txt`에 명시적인 버전을 지정하면 문제를 예방할 수 있습니다:

```txt
numpy>=1.24.0,<2.0.0
pandas>=2.0.0,<3.0.0
```

이미 `requirements.txt`에 반영되어 있습니다!

---

## 요약

1. **문제**: numpy와 pandas 버전 불일치
2. **해결**: `패키지_재설치_호환성수정.bat` 실행
3. **예방**: `requirements.txt` 사용하여 설치

끝! 🎯

