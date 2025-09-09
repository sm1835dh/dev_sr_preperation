## 클라이언트 사이트 파견시 확인사항 정리 

### 네트워크
* 외부 데이터를 내부망 PC로 옮기는 방법
* 외부망 사용 PC를 내부망 PC로 붙일 때, 보안 허가 필요한지 확인

### DB 
* DB 계정 신청 (DDL, DML 어디까지 가능한지 확인)
* DB 접근제어 신청
* DB내 테이블 생성 문의
* DB -> 신규 DB로 데이터 dump 방법 문의
* DB -> 신규 DB로 CDC 가능한지 문의 (Read replica 만들어 줄 수 있는지, VPC 이슈가 없는지)

### 소프트웨어 설치 확인 
* DB 접속 툴 - https://www.pgadmin.org/, https://dbeaver.io/
* VSCode - https://code.visualstudio.com/ 
* iterm2 - https://iterm2.com/

## 준비 사항
### DB 예상 구조
```mermaid
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
| Database profiling | 데이터 확인 | ☐ 원천 테이블 주요 테이블 리스트 정리<br>☐ 테이블 스키마 정리 | 미정 |
|  |  | ☐ 루비콘 테이블 스키마 정리 | 미정 |
|  | 데이터 통계 추출 | ☐ 원천 테이블 | 미정 |
|  |  | ☐ 루비콘 마트 테이블 | 미정 |
|  |  | ☐ Synonym dictionary | 미정 |
| Data Standardization |  | ☐ Naming standard | 미정 |
|  |  | ☐ Definition standard | 미정 |
|  |  | ☐ Format standard | 미정 |
|  |  | ☐ Code standard | 미정 |
| Metadata | 생성 | ☐ Short column description | 미정 |
|  |  | ☐ Long column  description | 미정 |
|  |  | ☐ Table profile description | 미정 |
|  |  | ☐ SQL-to-Text | 미정 |
| Data Load |  | ☐ Replication 작업 | 미정 |
| Data Mart |  | ☐ Design | 미정 |
|  |  | ☐ Build | 미정 |
| 기타 | 데이터 확인 | ☐ 프로모션 테이블 확인 | 미정 |
|  |  | ☐ Query log 분석 | 미정 |
|  |  | ☐ 질의어 분석 | 미정 |
|  |  | ☐ TC 분석 | 미정 |