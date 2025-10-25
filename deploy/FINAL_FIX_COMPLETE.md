# 최종 수정 완료

## 🐛 발생한 오류
```
KeyError: 'goal'
File "transform_spec_size.py", line 519, in process_spec_data_with_validation
    parsed_row['goal'] = row['goal']
```

## 🔍 원인
- `goal`은 데이터 row에 있는 필드가 아니라 함수 파라미터로 전달되는 값
- 라인 519에서 잘못된 위치(`row['goal']`)에서 goal을 가져오려고 시도

## ✅ 해결
### transform_spec_size.py 라인 519 수정:
```python
# 수정 전 (잘못됨)
parsed_row['goal'] = row['goal']

# 수정 후 (올바름)
parsed_row['goal'] = goal  # 함수 파라미터에서 직접 가져옴
```

## 📊 전체 데이터 흐름

### 1. 함수 시그니처
```python
def process_spec_data_with_validation(engine, goal, truncate_before_insert=True, verbose=True):
    # goal은 파라미터로 전달됨 (예: '해상도')
```

### 2. 파싱 과정
```python
for _, row in df_filtered.iterrows():
    parsed_rows, success, needs_check = parse_data_with_parser(row, parser)
    if success and parsed_rows:
        for parsed_row in parsed_rows:
            parsed_row['goal'] = goal  # 파라미터에서 가져옴
```

### 3. 최종 저장 형식
```sql
-- resolution_name (문자열 타입)
INSERT INTO kt_spec_dimension_mod_table_v01
(dimension_type, parsed_value, parsed_string_value, goal)
VALUES ('resolution_name', NULL, '4K', '해상도');

-- width (숫자 타입)
INSERT INTO kt_spec_dimension_mod_table_v01
(dimension_type, parsed_value, parsed_string_value, goal)
VALUES ('width', 3840.0, NULL, '해상도');
```

## 🚀 실행 명령어

해상도 데이터 재처리:
```bash
python transform_spec_size.py --goal 해상도
```

## ✨ 모든 수정 완료
1. ✅ `parsed_string_value` 필드 처리 추가
2. ✅ `resolution_type` → `parsed_string_value` 변경
3. ✅ `goal` 필드 올바른 위치에서 가져오기

이제 해상도 파싱이 정상적으로 작동하며, resolution_name이 `parsed_string_value` 컬럼에 올바르게 저장됩니다!