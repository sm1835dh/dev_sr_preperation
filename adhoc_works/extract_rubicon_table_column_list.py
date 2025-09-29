#!/usr/bin/env python3
"""
Rubicon 테이블 컬럼 리스트 추출기

'table_' 접두사를 가진 테이블들의 메타데이터와 샘플 데이터를 추출하는 프로그램
모듈별로 분리된 구조로 데이터베이스 연결, 테이블 발견, 컬럼 정보 추출, 데이터 샘플링을 수행합니다.

사용법:
    python extract_rubicon_table_column_list.py
"""

from table_analyzer import TableAnalyzer

def main():
    """메인 실행 함수"""
    print("Rubicon 테이블 컬럼 리스트 추출기")
    print("="*50)

    analyzer = None
    try:
        # 모듈식 테이블 분석기 사용
        analyzer = TableAnalyzer()
        analyzer.initialize()

        print("PostgreSQL 연결 성공!")
        print(f"Host: {analyzer.db_conn.host}:{analyzer.db_conn.port}")
        print(f"Database: {analyzer.db_conn.database}")

        # 'table_' 접두사를 가진 테이블들 분석
        print("\n'table_' 접두사를 가진 테이블들을 분석합니다...")
        results = analyzer.analyze_tables_with_prefix('sr_')

        if results:
            # 결과 요약 출력
            analyzer.print_summary(results)

            # JSON 파일로 저장
            json_filename = analyzer.export_to_json(results)
            print(f"\n✅ JSON 분석 결과가 '{json_filename}' 파일에 저장되었습니다.")

            # 각 테이블별 CSV 파일로 저장
            csv_files = analyzer.export_to_csv(results)
            if csv_files:
                print(f"✅ 테이블별 CSV 파일 {len(csv_files)}개 생성:")
                for csv_file in csv_files:
                    print(f"   - {csv_file}")

            # 통합 CSV 파일로 저장
            combined_csv = analyzer.export_combined_csv(results)
            print(f"✅ 통합 CSV 파일: {combined_csv}")

            # 각 테이블별 상세 정보 출력 (선택적)
            print_detailed_results = input("\n상세 결과를 화면에 출력하시겠습니까? (y/N): ").lower() == 'y'
            if print_detailed_results:
                print_table_details(results)
        else:
            print("\n⚠️ 'table_' 접두사를 가진 테이블이 없습니다.")

    except Exception as e:
        print(f"\n❌ 프로그램 실행 중 오류 발생: {e}")

    finally:
        if analyzer:
            analyzer.close()

def print_table_details(results):
    """테이블 상세 정보 출력"""
    for result in results:
        if 'error' in result:
            print(f"\n❌ 테이블 '{result['table_name']}' 분석 중 오류: {result['error']}")
            continue

        print(f"\n{'='*80}")
        print(f"테이블명: {result['table_name']}")
        print(f"스키마: {result['schema']}")
        print(f"총 컬럼 수: {result['total_columns']}")
        print(f"총 행 수: {result['total_rows']:,}")
        print(f"분석 시각: {result['analyzed_at']}")
        print(f"{'='*80}")

        if result.get('columns'):
            print(f"{'컬럼명':<20} {'타입':<20} {'코멘트':<30} {'최근 데이터 1':<15} {'최근 데이터 2':<15} {'최근 데이터 3':<15}")
            print("-" * 135)

            for col in result['columns']:
                print(f"{col['column_name']:<20} "
                      f"{col['data_type']:<20} "
                      f"{col['comment'][:28]:<30} "
                      f"{str(col['recent_data_1'])[:13]:<15} "
                      f"{str(col['recent_data_2'])[:13]:<15} "
                      f"{str(col['recent_data_3'])[:13]:<15}")

if __name__ == "__main__":
    main()