-- ================================================================================
-- 해상도 데이터 정리 SQL 스크립트
-- 잘못 저장된 resolution_name 데이터를 정리하고 다시 처리할 수 있도록 준비
-- ================================================================================

-- 1. 현재 상태 확인
-- --------------------------------------------------------------------------------
-- 해상도 관련 데이터의 저장 상태를 확인
SELECT
    '현재 상태' as status,
    COUNT(*) as count,
    dimension_type,
    CASE
        WHEN parsed_value IS NOT NULL THEN 'parsed_value'
        WHEN parsed_string_value IS NOT NULL THEN 'parsed_string_value'
        ELSE 'both_null'
    END as stored_in
FROM kt_spec_dimension_mod_table_v01
WHERE goal = '해상도'
  AND dimension_type IN ('resolution_name', 'width', 'height')
GROUP BY dimension_type, stored_in
ORDER BY dimension_type, stored_in;

-- 2. 잘못된 resolution_name 데이터 확인
-- --------------------------------------------------------------------------------
-- parsed_string_value가 NULL인 resolution_name 데이터 조회
SELECT
    mdl_code,
    goods_nm,
    dimension_type,
    parsed_value,
    parsed_string_value
FROM kt_spec_dimension_mod_table_v01
WHERE goal = '해상도'
  AND dimension_type = 'resolution_name'
  AND (parsed_string_value IS NULL OR parsed_string_value = '')
LIMIT 10;

-- 3. 잘못된 데이터 삭제
-- --------------------------------------------------------------------------------
-- parsed_string_value가 NULL인 resolution_name 데이터 삭제
BEGIN;

-- 삭제할 데이터 수 확인
SELECT COUNT(*) as records_to_delete
FROM kt_spec_dimension_mod_table_v01
WHERE goal = '해상도'
  AND dimension_type = 'resolution_name'
  AND (parsed_string_value IS NULL OR parsed_string_value = '');

-- 실제 삭제 실행
DELETE FROM kt_spec_dimension_mod_table_v01
WHERE goal = '해상도'
  AND dimension_type = 'resolution_name'
  AND (parsed_string_value IS NULL OR parsed_string_value = '');

-- 결과 확인 후 COMMIT 또는 ROLLBACK
-- COMMIT;  -- 삭제를 확정하려면 주석 해제
-- ROLLBACK;  -- 취소하려면 이 줄 사용

-- 4. staging 테이블 업데이트
-- --------------------------------------------------------------------------------
-- 해상도 관련 작업을 다시 처리할 수 있도록 is_completed를 false로 변경
UPDATE kt_spec_validation_table_v03_20251023_staging
SET is_completed = false
WHERE goal = '해상도'
  AND is_target = true;

-- 5. 삭제 후 상태 재확인
-- --------------------------------------------------------------------------------
SELECT
    '정리 후 상태' as status,
    COUNT(*) as count,
    dimension_type,
    CASE
        WHEN parsed_value IS NOT NULL THEN 'parsed_value'
        WHEN parsed_string_value IS NOT NULL THEN 'parsed_string_value'
        ELSE 'both_null'
    END as stored_in
FROM kt_spec_dimension_mod_table_v01
WHERE goal = '해상도'
  AND dimension_type IN ('resolution_name', 'width', 'height')
GROUP BY dimension_type, stored_in
ORDER BY dimension_type, stored_in;

-- ================================================================================
-- 정리 완료 후 해상도 데이터 재처리
-- 다음 명령어 실행:
-- python transform_spec_size.py --goal 해상도
-- ================================================================================