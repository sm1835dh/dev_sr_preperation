import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Optional
from datetime import datetime
import json
import os
from dotenv import load_dotenv
import psycopg2
from sqlalchemy import create_engine

# Load environment variables
load_dotenv('.env')

# PostgreSQL settings
PG_HOST = os.getenv('PG_HOST')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')

def get_db_connection():
    """PostgreSQL 데이터베이스 연결 객체를 반환합니다."""
    return psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )

class Text2SQLEvaluator:
    """Text2SQL 평가를 위한 클래스"""

    def __init__(self):
        self.connection = None
        self.evaluation_results = []

    def connect_db(self):
        """데이터베이스 연결"""
        try:
            self.connection = get_db_connection()
            print("✅ 데이터베이스 연결 성공")
            return True
        except Exception as e:
            print(f"❌ 데이터베이스 연결 실패: {e}")
            return False

    def close_connection(self):
        """데이터베이스 연결 종료"""
        if self.connection:
            self.connection.close()
            print("데이터베이스 연결 종료")

    def get_ground_truth(self, query_number: int) -> Tuple[Optional[List[str]], Optional[str], Optional[str]]:
        """
        정답 product_id list 조회

        Returns:
            Tuple of (product_id_list, category, instruction)
        """
        try:
            cursor = self.connection.cursor()
            query = """
                SELECT product_id_list, category, instruction
                FROM tc_check_table_20251015
                WHERE query_number = %s
            """
            cursor.execute(query, (query_number,))
            result = cursor.fetchone()
            cursor.close()

            if result:
                return result[0], result[1], result[2]
            else:
                print(f"⚠️ 쿼리 번호 {query_number}에 대한 정답을 찾을 수 없습니다.")
                return None, None, None

        except Exception as e:
            print(f"❌ 정답 조회 중 오류 발생: {e}")
            return None, None, None

    def calculate_metrics(self, predicted: List[str], ground_truth: List[str]) -> Dict:
        """
        예측 결과와 정답을 비교하여 평가 지표 계산

        Args:
            predicted: 예측된 product_id 리스트
            ground_truth: 정답 product_id 리스트

        Returns:
            평가 지표 딕셔너리
        """
        # Set으로 변환하여 비교
        pred_set = set(predicted) if predicted else set()
        truth_set = set(ground_truth) if ground_truth else set()

        # True Positives: 예측과 정답 모두에 있는 항목
        tp = len(pred_set.intersection(truth_set))

        # False Positives: 예측에만 있고 정답에는 없는 항목
        fp = len(pred_set - truth_set)

        # False Negatives: 정답에만 있고 예측에는 없는 항목
        fn = len(truth_set - pred_set)

        # Precision, Recall, F1 Score 계산
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0
        f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) > 0 else 0

        # Exact Match (완전 일치 여부)
        exact_match = 1 if pred_set == truth_set else 0

        # Jaccard Similarity (IoU)
        jaccard = tp / (tp + fp + fn) if (tp + fp + fn) > 0 else 0

        return {
            'TP': tp,
            'FP': fp,
            'FN': fn,
            'Precision': round(precision, 4),
            'Recall': round(recall, 4),
            'F1_Score': round(f1_score, 4),
            'Exact_Match': exact_match,
            'Jaccard_Similarity': round(jaccard, 4),
            'Predicted_Count': len(pred_set),
            'Ground_Truth_Count': len(truth_set)
        }

    def evaluate_single_query(self, query_number: int, predicted_ids: List[str],
                            verbose: bool = False) -> Dict:
        """
        단일 쿼리에 대한 평가 수행

        Args:
            query_number: 쿼리 번호
            predicted_ids: 예측된 product_id 리스트
            verbose: 상세 출력 여부

        Returns:
            평가 결과 딕셔너리
        """
        # 정답 조회
        ground_truth_ids, category, instruction = self.get_ground_truth(query_number)

        if ground_truth_ids is None:
            return None

        # 평가 지표 계산
        metrics = self.calculate_metrics(predicted_ids, ground_truth_ids)

        # 결과에 메타 정보 추가
        result = {
            'query_number': query_number,
            'category': category,
            'instruction': instruction[:50] + '...' if instruction and len(instruction) > 50 else instruction,
            **metrics
        }

        # 결과 저장
        self.evaluation_results.append(result)

        if verbose:
            print(f"\n{'='*60}")
            print(f"쿼리 번호: {query_number}")
            print(f"카테고리: {category}")
            print(f"{'='*60}")
            print(f"예측된 product_id 수: {metrics['Predicted_Count']}")
            print(f"정답 product_id 수: {metrics['Ground_Truth_Count']}")
            print(f"\n[평가 지표]")
            print(f"  - TP (True Positives): {metrics['TP']}")
            print(f"  - FP (False Positives): {metrics['FP']}")
            print(f"  - FN (False Negatives): {metrics['FN']}")
            print(f"  - Precision: {metrics['Precision']:.2%}")
            print(f"  - Recall: {metrics['Recall']:.2%}")
            print(f"  - F1 Score: {metrics['F1_Score']:.2%}")
            print(f"  - Exact Match: {'✅' if metrics['Exact_Match'] else '❌'}")
            print(f"  - Jaccard Similarity: {metrics['Jaccard_Similarity']:.2%}")

        return result

    def evaluate_batch(self, query_predictions: List[Tuple[int, List[str]]],
                      verbose: bool = False) -> pd.DataFrame:
        """
        여러 쿼리에 대한 일괄 평가

        Args:
            query_predictions: [(query_number, predicted_ids), ...] 형태의 리스트
            verbose: 각 쿼리별 상세 출력 여부

        Returns:
            평가 결과 DataFrame
        """
        # 평가 시작 전에 결과 리스트 초기화하지 않음 (누적 평가를 위해)
        batch_results = []

        for query_number, predicted_ids in query_predictions:
            result = self.evaluate_single_query(query_number, predicted_ids, verbose)
            if result:
                batch_results.append(result)

        # 배치 결과만 DataFrame으로 반환
        return pd.DataFrame(batch_results)

    def get_overall_metrics(self) -> Dict:
        """
        전체 평가 결과 요약

        Returns:
            전체 평가 지표 딕셔너리
        """
        if not self.evaluation_results:
            print("평가된 결과가 없습니다.")
            return None

        df = pd.DataFrame(self.evaluation_results)

        # 중복 제거 (같은 query_number가 여러 번 평가된 경우 최신 결과만 사용)
        df = df.drop_duplicates(subset=['query_number'], keep='last')

        # 전체 통계
        total_tp = df['TP'].sum()
        total_fp = df['FP'].sum()
        total_fn = df['FN'].sum()

        # Micro-averaged metrics (전체 TP, FP, FN 기준)
        micro_precision = total_tp / (total_tp + total_fp) if (total_tp + total_fp) > 0 else 0
        micro_recall = total_tp / (total_tp + total_fn) if (total_tp + total_fn) > 0 else 0
        micro_f1 = 2 * (micro_precision * micro_recall) / (micro_precision + micro_recall) if (micro_precision + micro_recall) > 0 else 0

        # Macro-averaged metrics (각 쿼리별 평균)
        macro_precision = df['Precision'].mean()
        macro_recall = df['Recall'].mean()
        macro_f1 = df['F1_Score'].mean()

        # 카테고리별 성능
        category_metrics = None
        if 'category' in df.columns and df['category'].notna().any():
            category_df = df.groupby('category').agg({
                'Precision': 'mean',
                'Recall': 'mean',
                'F1_Score': 'mean',
                'Exact_Match': 'mean',
                'query_number': 'count'
            }).round(4)
            category_df.rename(columns={'query_number': 'Count'}, inplace=True)
            # DataFrame을 Dict로 변환
            category_metrics = category_df.to_dict('index')

        overall_metrics = {
            'total_queries': len(df),
            'total_tp': int(total_tp),
            'total_fp': int(total_fp),
            'total_fn': int(total_fn),
            'micro_precision': round(micro_precision, 4),
            'micro_recall': round(micro_recall, 4),
            'micro_f1': round(micro_f1, 4),
            'macro_precision': round(macro_precision, 4),
            'macro_recall': round(macro_recall, 4),
            'macro_f1': round(macro_f1, 4),
            'exact_match_rate': round(df['Exact_Match'].mean(), 4),
            'avg_jaccard': round(df['Jaccard_Similarity'].mean(), 4),
            'category_metrics': category_metrics
        }

        return overall_metrics

    def print_overall_report(self):
        """
        전체 평가 리포트 출력
        """
        metrics = self.get_overall_metrics()

        if not metrics:
            return

        print("\n" + "="*70)
        print(" " * 25 + "📊 전체 평가 결과")
        print("="*70)

        print(f"\n[평가 대상]")
        print(f"  총 쿼리 수: {metrics['total_queries']}개")

        print(f"\n[전체 통계]")
        print(f"  - Total TP: {metrics['total_tp']}")
        print(f"  - Total FP: {metrics['total_fp']}")
        print(f"  - Total FN: {metrics['total_fn']}")

        print(f"\n[Micro-averaged Metrics] (전체 TP, FP, FN 기준)")
        print(f"  - Precision: {metrics['micro_precision']:.2%}")
        print(f"  - Recall: {metrics['micro_recall']:.2%}")
        print(f"  - F1 Score: {metrics['micro_f1']:.2%}")

        print(f"\n[Macro-averaged Metrics] (쿼리별 평균)")
        print(f"  - Precision: {metrics['macro_precision']:.2%}")
        print(f"  - Recall: {metrics['macro_recall']:.2%}")
        print(f"  - F1 Score: {metrics['macro_f1']:.2%}")

        print(f"\n[추가 지표]")
        print(f"  - Exact Match Rate: {metrics['exact_match_rate']:.2%}")
        print(f"  - Average Jaccard Similarity: {metrics['avg_jaccard']:.2%}")

        if metrics['category_metrics'] is not None:
            print(f"\n[카테고리별 성능]")
            for category, cat_metrics in metrics['category_metrics'].items():
                print(f"  {category}:")
                for key, value in cat_metrics.items():
                    print(f"    - {key}: {value}")

        print("\n" + "="*70)

    def save_results(self, filename: str = None):
        """
        평가 결과를 파일로 저장

        Args:
            filename: 저장할 파일명 (기본값: text2sql_eval_YYYYMMDD_HHMMSS.csv)
        """
        if not self.evaluation_results:
            print("저장할 평가 결과가 없습니다.")
            return

        if filename is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"text2sql_eval_{timestamp}.csv"

        df = pd.DataFrame(self.evaluation_results)
        # 중복 제거
        df = df.drop_duplicates(subset=['query_number'], keep='last')
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"✅ 평가 결과가 {filename}에 저장되었습니다. (총 {len(df)}개 쿼리)")

    def reset_results(self):
        """평가 결과 초기화"""
        self.evaluation_results = []
        print("평가 결과가 초기화되었습니다.")

    def get_results_dataframe(self) -> pd.DataFrame:
        """
        현재까지의 평가 결과를 DataFrame으로 반환

        Returns:
            평가 결과 DataFrame
        """
        if not self.evaluation_results:
            return pd.DataFrame()

        df = pd.DataFrame(self.evaluation_results)
        # 중복 제거
        df = df.drop_duplicates(subset=['query_number'], keep='last')
        return df