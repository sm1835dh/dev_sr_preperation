# 수정 사항 요약

## 문제점
DB에서 `dimension_type='resolution_name'` 데이터의 `parsed_string_value`가 NULL로 저장되는 문제 발생

## 원인
1. 파서는 `parsed_string_value` 필드에 값을 저장
2. `transform_spec_size.py`는 여전히 `resolution_type`을 찾고 있어서 데이터 전달 실패
3. `parsed_string_value` 필드 처리 로직 누락

## 수정 내용

### 1. parsers/resolution_parser.py
- `parsed_value` → `parsed_string_value` 변경 (resolution_name의 경우)
- 숫자 타입(width, height): `parsed_value` 사용
- 문자열 타입(resolution_name): `parsed_string_value` 사용

### 2. transform_spec_size.py
변경된 부분:
- 라인 285-287: `resolution_type` → `parsed_string_value`
- 라인 291-315: `resolution_type` → `parsed_string_value`
- 라인 342-344: `resolution_type` → `parsed_string_value`
- 라인 386-389: `resolution_name` → `parsed_string_value` 처리 추가
- 라인 384: `goal` 필드 추가
- 라인 519: 파싱 결과에 `goal` 필드 추가

## 데이터 저장 형식

### 올바른 저장 예시
```sql
-- width (숫자)
INSERT INTO kt_spec_dimension_mod_table_v01
(mdl_code, dimension_type, parsed_value, parsed_string_value, goal)
VALUES ('TEST001', 'width', 3840.0, NULL, '해상도');

-- height (숫자)
INSERT INTO kt_spec_dimension_mod_table_v01
(mdl_code, dimension_type, parsed_value, parsed_string_value, goal)
VALUES ('TEST001', 'height', 2160.0, NULL, '해상도');

-- resolution_name (문자열)
INSERT INTO kt_spec_dimension_mod_table_v01
(mdl_code, dimension_type, parsed_value, parsed_string_value, goal)
VALUES ('TEST001', 'resolution_name', NULL, '4K', '해상도');
```

## DB 정리 작업

기존 잘못된 데이터를 정리하려면:

1. SQL 스크립트 실행:
```bash
psql -d your_database < cleanup_resolution.sql
```

2. 또는 Python 스크립트 실행:
```bash
python cleanup_resolution_data.py
```

3. 해상도 데이터 재처리:
```bash
python transform_spec_size.py --goal 해상도
```

## 테스트 스크립트

- `test_parsed_string_value.py` - parsed_string_value 필드 검증
- `test_final_integration.py` - 전체 통합 테스트

## 중요 포인트

✅ **데이터 타입별 저장 규칙**:
- Numeric 타입 (width, height, depth) → `parsed_value` (FLOAT)
- String 타입 (resolution_name) → `parsed_string_value` (VARCHAR)

✅ **NULL 처리**:
- `parsed_value` 사용 시 → `parsed_string_value`는 NULL
- `parsed_string_value` 사용 시 → `parsed_value`는 NULL