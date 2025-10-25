# 필드 매핑 문서

## 데이터 타입별 저장 규칙

### 1. 숫자 데이터 (Numeric)
- **필드**: `parsed_value` (FLOAT)
- **적용 dimension_type**: `width`, `height`, `depth`
- **예시**:
  ```sql
  dimension_type='width', parsed_value=3840.0
  dimension_type='height', parsed_value=2160.0
  ```

### 2. 문자열 데이터 (String)
- **필드**: `parsed_string_value` (VARCHAR)
- **적용 dimension_type**: `resolution_name`
- **예시**:
  ```sql
  dimension_type='resolution_name', parsed_string_value='4K'
  dimension_type='resolution_name', parsed_string_value='FHD'
  ```

## 데이터베이스 스키마

```sql
CREATE TABLE kt_spec_dimension_mod_table_v01 (
    mdl_code VARCHAR,
    dimension_type VARCHAR,
    parsed_value FLOAT,           -- 숫자 값 저장 (width, height, depth)
    parsed_string_value VARCHAR,   -- 문자열 값 저장 (resolution_name)
    goal VARCHAR,
    needs_check BOOLEAN,
    ...
);
```

## 파서별 출력 형식

### 크기 파서 (goal='크기작업')
- width → parsed_value (float)
- height → parsed_value (float)
- depth → parsed_value (float)

### 해상도 파서 (goal='해상도')
- width → parsed_value (float)
- height → parsed_value (float)
- resolution_name → parsed_string_value (string)

## 예시: '4K (3,840 x 2,160)' 파싱 결과

```sql
-- Row 1: 너비
INSERT INTO kt_spec_dimension_mod_table_v01 (
    mdl_code, dimension_type, parsed_value, parsed_string_value, goal
) VALUES (
    'TEST001', 'width', 3840.0, NULL, '해상도'
);

-- Row 2: 높이
INSERT INTO kt_spec_dimension_mod_table_v01 (
    mdl_code, dimension_type, parsed_value, parsed_string_value, goal
) VALUES (
    'TEST001', 'height', 2160.0, NULL, '해상도'
);

-- Row 3: 해상도 타입
INSERT INTO kt_spec_dimension_mod_table_v01 (
    mdl_code, dimension_type, parsed_value, parsed_string_value, goal
) VALUES (
    'TEST001', 'resolution_name', NULL, '4K', '해상도'
);
```

## 중요 사항

1. **필드 선택 규칙**:
   - dimension_type이 숫자 타입(width, height, depth)인 경우 → `parsed_value` 사용
   - dimension_type이 문자열 타입(resolution_name)인 경우 → `parsed_string_value` 사용

2. **NULL 처리**:
   - parsed_value 사용 시 → parsed_string_value는 NULL
   - parsed_string_value 사용 시 → parsed_value는 NULL

3. **타입 안정성**:
   - parsed_value는 항상 FLOAT 타입
   - parsed_string_value는 항상 VARCHAR 타입