import logging
import json
import csv
from datetime import datetime, date
from decimal import Decimal
from db_connection import DatabaseConnection
from table_discovery import TableDiscovery
from column_extractor import ColumnExtractor
from data_sampler import DataSampler

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TableAnalyzer:
    def __init__(self, env_file='.env'):
        """테이블 분석기 클래스"""
        self.db_conn = DatabaseConnection(env_file)
        self.engine = None
        self.table_discovery = None
        self.column_extractor = None
        self.data_sampler = None

    def initialize(self):
        """모듈 초기화"""
        try:
            self.engine = self.db_conn.connect()
            self.table_discovery = TableDiscovery(self.engine)
            self.column_extractor = ColumnExtractor(self.engine)
            self.data_sampler = DataSampler(self.engine)
            logger.info("테이블 분석기 초기화 완료")
        except Exception as e:
            logger.error(f"초기화 중 오류: {e}")
            raise

    def analyze_tables_with_prefix(self, prefix='table_', schema='public'):
        """지정된 접두사를 가진 테이블들 분석"""
        try:
            # 테이블 목록 조회
            tables = self.table_discovery.find_tables_by_prefix(prefix, schema)

            if not tables:
                logger.info(f"'{prefix}' 접두사를 가진 테이블이 없습니다.")
                return []

            results = []
            for table in tables:
                table_name = table['table_name']
                logger.info(f"테이블 '{table_name}' 분석 시작")

                table_result = self.analyze_single_table(table_name, schema)
                results.append(table_result)

            logger.info(f"총 {len(results)}개 테이블 분석 완료")
            return results

        except Exception as e:
            logger.error(f"테이블 분석 중 오류: {e}")
            raise

    def analyze_single_table(self, table_name, schema='public'):
        """단일 테이블 분석"""
        try:
            # 컬럼 정보 추출
            columns = self.column_extractor.get_table_columns(table_name, schema)

            # 데이터 샘플 추출
            samples = self.data_sampler.get_recent_data_samples(table_name, columns, schema, 3)

            # 데이터 요약 정보
            data_summary = self.data_sampler.get_table_data_summary(table_name, columns, schema)

            # 결과 구성
            table_result = {
                'table_name': table_name,
                'schema': schema,
                'total_columns': len(columns),
                'total_rows': data_summary['total_rows'],
                'columns': [],
                'sample_data': samples,
                'analyzed_at': datetime.now().isoformat()
            }

            # 각 컬럼에 대한 상세 정보
            for col in columns:
                col_name = col['column_name']
                formatted_type = self.column_extractor.format_column_type(col)

                # 샘플 데이터에서 해당 컬럼의 값들 추출
                sample_values = []
                for sample in samples[:3]:  # 최근 3개 데이터
                    value = sample.get(col_name)
                    formatted_value = self.data_sampler.format_sample_value(value)
                    sample_values.append(formatted_value)

                # 부족한 샘플 데이터는 'N/A'로 채움
                while len(sample_values) < 3:
                    sample_values.append('N/A')

                column_info = {
                    'column_name': col_name,
                    'data_type': formatted_type,
                    'is_nullable': col['is_nullable'],
                    'comment': col['comment'] or '',
                    'recent_data_1': sample_values[0],
                    'recent_data_2': sample_values[1],
                    'recent_data_3': sample_values[2],
                    'null_count': data_summary['column_stats'].get(col_name, {}).get('null_count', 0)
                }

                table_result['columns'].append(column_info)

            return table_result

        except Exception as e:
            logger.error(f"테이블 '{table_name}' 분석 중 오류: {e}")
            # 오류 발생시에도 기본 구조 반환
            return {
                'table_name': table_name,
                'schema': schema,
                'error': str(e),
                'analyzed_at': datetime.now().isoformat()
            }

    def export_to_json(self, results, filename=None):
        """결과를 JSON 파일로 저장"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"table_analysis_result_{timestamp}.json"

        class DateTimeEncoder(json.JSONEncoder):
            """datetime 및 기타 타입을 JSON 직렬화하기 위한 커스텀 인코더"""
            def default(self, obj):
                if isinstance(obj, (datetime, date)):
                    return obj.isoformat()
                elif isinstance(obj, Decimal):
                    return float(obj)
                elif hasattr(obj, '__dict__'):
                    return str(obj)
                return super().default(obj)

        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(results, f, ensure_ascii=False, indent=2, cls=DateTimeEncoder)

            logger.info(f"분석 결과가 '{filename}' 파일로 저장되었습니다.")
            return filename

        except Exception as e:
            logger.error(f"파일 저장 중 오류: {e}")
            raise

    def export_to_csv(self, results, prefix=None):
        """각 테이블별로 CSV 파일로 저장"""
        if prefix is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            prefix = f"table_columns_{timestamp}"

        csv_files = []

        for result in results:
            if 'error' in result:
                logger.warning(f"테이블 '{result['table_name']}' CSV 저장 건너뜀 (오류 발생)")
                continue

            table_name = result['table_name']
            filename = f"{prefix}_{table_name}.csv"

            try:
                with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
                    # CSV 헤더 정의
                    fieldnames = [
                        '테이블명',
                        '컬럼명',
                        '타입',
                        '코멘트',
                        '최근 데이터 1',
                        '최근 데이터 2',
                        '최근 데이터 3',
                        'NULL 허용',
                        'NULL 개수',
                        '총 행 수'
                    ]

                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    writer.writeheader()

                    # 각 컬럼 정보를 행으로 작성
                    for col in result.get('columns', []):
                        row = {
                            '테이블명': table_name,
                            '컬럼명': col['column_name'],
                            '타입': col['data_type'],
                            '코멘트': col.get('comment', ''),
                            '최근 데이터 1': self._format_csv_value(col.get('recent_data_1', '')),
                            '최근 데이터 2': self._format_csv_value(col.get('recent_data_2', '')),
                            '최근 데이터 3': self._format_csv_value(col.get('recent_data_3', '')),
                            'NULL 허용': 'Y' if col.get('is_nullable') == 'YES' else 'N',
                            'NULL 개수': col.get('null_count', 0),
                            '총 행 수': result.get('total_rows', 0)
                        }
                        writer.writerow(row)

                logger.info(f"CSV 파일 '{filename}' 저장 완료")
                csv_files.append(filename)

            except Exception as e:
                logger.error(f"테이블 '{table_name}' CSV 저장 중 오류: {e}")

        return csv_files

    def export_combined_csv(self, results, filename=None):
        """모든 테이블 정보를 하나의 CSV 파일로 저장"""
        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"all_tables_columns_{timestamp}.csv"

        try:
            with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
                fieldnames = [
                    '테이블명',
                    '컬럼명',
                    '타입',
                    '코멘트',
                    '최근 데이터 1',
                    '최근 데이터 2',
                    '최근 데이터 3',
                    'NULL 허용',
                    'NULL 개수',
                    '총 행 수'
                ]

                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()

                for result in results:
                    if 'error' in result:
                        continue

                    table_name = result['table_name']
                    total_rows = result.get('total_rows', 0)

                    for col in result.get('columns', []):
                        row = {
                            '테이블명': table_name,
                            '컬럼명': col['column_name'],
                            '타입': col['data_type'],
                            '코멘트': col.get('comment', ''),
                            '최근 데이터 1': self._format_csv_value(col.get('recent_data_1', '')),
                            '최근 데이터 2': self._format_csv_value(col.get('recent_data_2', '')),
                            '최근 데이터 3': self._format_csv_value(col.get('recent_data_3', '')),
                            'NULL 허용': 'Y' if col.get('is_nullable') == 'YES' else 'N',
                            'NULL 개수': col.get('null_count', 0),
                            '총 행 수': total_rows
                        }
                        writer.writerow(row)

            logger.info(f"통합 CSV 파일 '{filename}' 저장 완료")
            return filename

        except Exception as e:
            logger.error(f"통합 CSV 파일 저장 중 오류: {e}")
            raise

    def _format_csv_value(self, value):
        """CSV 값 포맷팅"""
        if value is None or value == 'NULL':
            return ''
        elif value == 'N/A':
            return ''
        elif isinstance(value, (datetime, date)):
            return value.isoformat()
        elif isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        else:
            # CSV에서 문제될 수 있는 문자 처리
            str_value = str(value)
            # 줄바꿈 문자를 공백으로 대체
            str_value = str_value.replace('\n', ' ').replace('\r', ' ')
            return str_value

    def print_summary(self, results):
        """분석 결과 요약 출력"""
        print("\n" + "="*60)
        print("테이블 분석 결과 요약")
        print("="*60)

        for result in results:
            if 'error' in result:
                print(f"❌ 테이블: {result['table_name']} (오류: {result['error']})")
                continue

            print(f"\n📋 테이블: {result['table_name']}")
            print(f"   - 컬럼 수: {result['total_columns']}")
            print(f"   - 총 행 수: {result['total_rows']:,}")

            if result.get('columns'):
                print("   - 컬럼 정보:")
                for col in result['columns'][:5]:  # 처음 5개 컬럼만 표시
                    print(f"     • {col['column_name']} ({col['data_type']})")
                if len(result['columns']) > 5:
                    print(f"     ... 외 {len(result['columns']) - 5}개 컬럼")

    def close(self):
        """리소스 정리"""
        if self.db_conn:
            self.db_conn.close()


def main():
    """메인 실행 함수"""
    analyzer = None
    try:
        # 테이블 분석기 초기화
        analyzer = TableAnalyzer()
        analyzer.initialize()

        print("PostgreSQL 연결 성공!")
        print(f"Host: {analyzer.db_conn.host}:{analyzer.db_conn.port}")
        print(f"Database: {analyzer.db_conn.database}")

        # 'table_' 접두사를 가진 테이블들 분석
        print("\n'table_' 접두사를 가진 테이블들을 분석합니다...")
        results = analyzer.analyze_tables_with_prefix('table_')

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

        else:
            print("\n⚠️ 분석할 테이블이 없습니다.")

    except Exception as e:
        print(f"\n❌ 프로그램 실행 중 오류 발생: {e}")
        logger.error(f"메인 실행 중 오류: {e}")

    finally:
        if analyzer:
            analyzer.close()


if __name__ == "__main__":
    main()