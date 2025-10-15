# 🚀 빠른 시작 가이드 - Rubicon Data Ingestion

## 📋 시스템 요구사항

- Docker 20.10+ 설치
- Docker Compose 2.0+ 설치 (선택사항)
- 메모리: 최소 4GB RAM 권장

## 🎯 3단계로 시작하기

### 1️⃣ 초기 설정 (최초 1회만)

```bash
# 환경 설정 스크립트 실행
./setup.sh

# 다음 정보를 준비하세요:
# - Azure OpenAI API Key
# - PostgreSQL Password
# - MongoDB/Cosmos DB Connection String
```

설정 옵션:
- **Quick Setup (권장)**: 필수 정보만 설정
- **Full Setup**: 모든 설정 값 커스터마이징
- **Test Connections**: 연결 테스트

### 2️⃣ Docker 이미지 빌드

```bash
# 기본 빌드
./build.sh

# 캐시 없이 새로 빌드 (문제 해결 시)
./build.sh --clean

# 커스텀 태그로 빌드
./build.sh --tag v1.0.0
```

### 3️⃣ 실행

```bash
# 기본 실행 (.env 설정값 사용)
./run.sh

# 테이블만 변경하여 실행
./run.sh --table kt_merged_product_20251015

# 여러 파라미터 변경
./run.sh \
  --table kt_merged_product_20251015 \
  --direct-collection product_data_v2 \
  --mode metadata

# 백그라운드 실행 + 로그 확인
./run.sh -d -f
```

## 📊 실행 모드

| 모드 | 설명 | 명령어 |
|------|------|--------|
| `both` | PostgreSQL → MongoDB 전송 + 메타데이터 생성 | `./run.sh --mode both` |
| `direct` | PostgreSQL → MongoDB 전송만 | `./run.sh --mode direct` |
| `metadata` | 메타데이터 생성만 | `./run.sh --mode metadata` |

## 🔧 자주 사용하는 명령어

### 일일 배치 작업

```bash
# 오늘 날짜로 테이블 처리
./run.sh --table kt_merged_product_$(date +%Y%m%d)

# 특정 날짜 처리
./run.sh --table kt_merged_product_20251015
```

### 테스트 실행

```bash
# 소량 데이터로 테스트 (100개 샘플, 배치 10)
./run.sh --sample-size 100 --batch-size 10 --mode direct

# 메타데이터만 테스트
./run.sh --sample-size 10 --mode metadata
```

### 컨테이너 관리

```bash
# 실행 중인 컨테이너 확인
docker ps --filter name=rubicon-data-ingestion

# 로그 확인 (실시간)
docker logs rubicon-data-ingestion -f

# 최근 로그 100줄
docker logs rubicon-data-ingestion --tail 100

# 컨테이너 중지
docker stop rubicon-data-ingestion

# 컨테이너 제거
docker rm rubicon-data-ingestion

# 중지 + 제거 + 재시작
./run.sh --rm
```

## 🐛 문제 해결

### 1. 이미지를 찾을 수 없음

```bash
# 이미지 다시 빌드
./build.sh
```

### 2. 컨테이너가 이미 실행 중

```bash
# 기존 컨테이너 제거 후 실행
./run.sh --rm
```

### 3. 환경 변수 오류

```bash
# 환경 설정 재구성
./setup.sh

# .env 파일 직접 수정
vi .env
```

### 4. 연결 실패

```bash
# 연결 테스트
./setup.sh
# 옵션 3 선택 (Test Connections Only)
```

### 5. 빌드 캐시 문제

```bash
# 캐시 없이 클린 빌드
./build.sh --clean
```

## 📁 파일 구조

```
deploy/
├── .env.example     # 환경변수 템플릿
├── .env            # 실제 환경변수 (git 제외)
├── setup.sh        # 초기 설정 스크립트
├── build.sh        # 이미지 빌드 스크립트
├── run.sh          # 컨테이너 실행 스크립트
├── Dockerfile      # Docker 이미지 정의
├── requirements.txt # Python 패키지
└── *.py            # Python 소스 코드
```

## 💡 팁

### 환경별 설정 파일 사용

```bash
# 개발 환경
./run.sh --env-file .env.dev

# 테스트 환경
./run.sh --env-file .env.test
```

### 스크립트 권한 문제

```bash
# 실행 권한 부여
chmod +x setup.sh build.sh run.sh
```

### 도움말 확인

```bash
./setup.sh --help
./build.sh --help
./run.sh --help
```

## 📞 지원

문제가 지속되면 다음 정보와 함께 문의하세요:

1. 실행한 명령어
2. 에러 메시지 전문
3. `docker logs rubicon-data-ingestion --tail 50` 출력
4. `.env` 파일 (민감 정보 제외)

## 🔐 보안 주의사항

- `.env` 파일은 절대 Git에 커밋하지 마세요
- API 키와 패스워드는 안전하게 관리하세요
- 프로덕션 환경에서는 별도의 시크릿 관리 도구 사용을 권장합니다

---

**Quick Reference Card**

```bash
# 설정
./setup.sh

# 빌드
./build.sh

# 실행
./run.sh

# 커스텀 실행
./run.sh --table TABLE_NAME --mode MODE

# 로그
docker logs rubicon-data-ingestion -f

# 중지
docker stop rubicon-data-ingestion
```