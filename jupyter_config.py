"""
Jupyter Notebook 설정 파일
notebooks 폴더를 기본 작업 디렉토리로 설정
"""

c = get_config()  # noqa

# 기본 노트북 디렉토리 설정
c.NotebookApp.notebook_dir = './notebooks'
c.ServerApp.root_dir = './notebooks'

# 브라우저 자동 열기
c.NotebookApp.open_browser = True
c.ServerApp.open_browser = True

# 토큰 기반 인증 (로컬 개발용)
c.NotebookApp.token = ''
c.ServerApp.token = ''
c.NotebookApp.password = ''
c.ServerApp.password = ''

# 포트 설정
c.NotebookApp.port = 8888
c.ServerApp.port = 8888

# IP 설정 (로컬 개발용)
c.NotebookApp.ip = 'localhost'
c.ServerApp.ip = 'localhost'