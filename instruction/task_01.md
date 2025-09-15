## 클라이언트 사이트 파견시 확인사항 정리 

### 네트워크
* 외부 데이터를 내부망 PC로 옮기는 방법
* 외부망 사용 PC를 내부망 PC로 붙일 때, 보안 허가 필요한지 확인
* Airdrop 확인 

### Server
* 서버 접근제어 신청 

### DB 
* DB 계정 신청 (DDL, DML 어디까지 가능한지 확인)
* DB 접근제어 신청
* DB내 테이블 생성 문의
* DB -> 신규 DB로 데이터 dump 방법 문의
* DB -> 신규 DB로 CDC 가능한지 문의 (Read replica 만들어 줄 수 있는지, VPC 이슈가 없는지)
* Query log 확인

### 로그 
* 질의어 로그 

### 소프트웨어 확인 
* DB 접속 툴 - https://www.pgadmin.org/, https://dbeaver.io/
* VSCode - https://code.visualstudio.com/ 
* iterm2 - https://iterm2.com/
* VSCode extension 
* Python 버전 확인
* Python 라이브러리 (ex. jupyter, uv) 

## 준비 사항
### 데이터 Ingestion

```text
[Source PostgreSQL] → [Azure DMS] → [Target PostgreSQL (Replica)]
                                            ↓
                                    [Azure Data Factory]
                                            ↓
                                    [NoSQL (Cosmos DB)]
```

## 체크 리스트

| 대분류 | 소분류 | 항목 | 작업자 |
|---|---|---|---|
| Database profiling | 데이터 확인 | ☐ 원천 테이블 주요 테이블 리스트 정리 | 미정 |
|  |  | ☐ 원천 테이블 스키마 정리 | 미정 |
|  |  | ☐ 루비콘 테이블 스키마 정리 | 미정 |
|  |  | ☐ Synonym dictionary | 미정 |
|  | 데이터 통계 추출 | ☐ 원천 테이블 | 미정 |
|  |  | ☐ 루비콘 테이블 | 미정 |
| Data Standardization |  | ☐ Naming standard | 미정 |
|  |  | ☐ Definition standard | 미정 |
|  |  | ☐ Format standard | 미정 |
|  |  | ☐ Code standard | 미정 |
| User data analysis |  | ☐ 질의어 분석 | 미정 |
|  |  | ☐ TC 분석 | 미정 |
| Metadata Extension | Metadata | ☐ Short column description | 미정 |
|  |  | ☐ Long column  description | 미정 |
|  |  | ☐ Table profile description | 미정 |
|  | SQL-to-Text | ☐ 대상 쿼리 확보 | 미정 | 
|  |  | ☐ Summarize columns | 미정 | 
| Data Ingestion |  | ☐ Replication 작업 | 미정 |
|  |  | ☐ PostgreSQL 셋팅 | 미정 |
|  |  | ☐ Azure Data Factory 셋팅 | 미정 |
|  |  | ☐ Cosmos DB 셋팅 | 미정 |
|  |  | ☐ AI Search 셋팅 | 미정 |
| Data Loading |  | ☐ Mart 테이블 정형 저장소에 적재 | 미정 | 
|  |  | ☐ Metadata 정형 저장소에 적재 | 미정 | 
|  |  | ☐ Metadata 비정형 저장소에 적재 | 미정 | 
| Data Mart | 정형 | ☐ Design | 미정 |
|  |  | ☐ Build | 미정 |
|  |  | ☐ Validation | 미정 |
| Mock DB |  | ☐ Mock DB 생성 (마트 스키마 사용) | 미정 |
|  |  | ☐ 샘플 데이터 적재 | 미정 |
| 기타 | 데이터 확인 | ☐ 프로모션 테이블 확인 | 미정 |
|  |  | ☐ Query log 분석 | 미정 |
