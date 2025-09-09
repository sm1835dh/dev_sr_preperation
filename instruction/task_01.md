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

## 준비 사항
### DB 
[Source PostgreSQL] → [Azure DMS] → [Target PostgreSQL (Replica)]
                                            ↓
                                    [Azure Data Factory]
                                            ↓
                                    [NoSQL (Cosmos DB)]

