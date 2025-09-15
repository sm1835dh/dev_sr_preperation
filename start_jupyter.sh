#!/bin/bash

# Jupyter Notebook 시작 스크립트
# uv 환경에서 Jupyter를 실행합니다

echo "=================================="
echo "Rubicon Jupyter Notebook 시작"
echo "=================================="

# PATH에 uv 추가
export PATH="$HOME/.local/bin:$PATH"

# notebooks 폴더가 없으면 생성
if [ ! -d "notebooks" ]; then
    echo "notebooks 폴더 생성 중..."
    mkdir notebooks
fi

# Jupyter 설정 파일로 실행
echo "Jupyter Notebook을 시작합니다..."
echo "브라우저가 자동으로 열립니다."
echo ""
echo "종료하려면 Ctrl+C를 두 번 누르세요."
echo ""

# Jupyter 실행
uv run jupyter notebook --config=jupyter_config.py