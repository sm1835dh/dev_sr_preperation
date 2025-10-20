from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List, Dict, Optional, Tuple
from contextlib import contextmanager
import uvicorn
from text2sql_evaluator import Text2SQLEvaluator
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# FastAPI app initialization
app = FastAPI(
    title="Text2SQL Evaluation API",
    description="Text2SQL 모델의 예측 결과를 평가하는 API",
    version="1.0.0"
)

# Request/Response Models
class SingleQueryRequest(BaseModel):
    query_number: int
    predicted_product_ids: List[str]

    class Config:
        schema_extra = {
            "example": {
                "query_number": 1,
                "predicted_product_ids": ["P001", "P002", "P003"]
            }
        }

class BatchQueryRequest(BaseModel):
    queries: List[SingleQueryRequest]

    class Config:
        schema_extra = {
            "example": {
                "queries": [
                    {"query_number": 1, "predicted_product_ids": ["P001", "P002"]},
                    {"query_number": 2, "predicted_product_ids": ["P003", "P004", "P005"]},
                    {"query_number": 3, "predicted_product_ids": ["P006"]}
                ]
            }
        }

class MetricsResponse(BaseModel):
    query_number: int
    category: Optional[str]
    instruction: Optional[str]
    TP: int
    FP: int
    FN: int
    Precision: float
    Recall: float
    F1_Score: float
    Exact_Match: int
    Jaccard_Similarity: float
    Predicted_Count: int
    Ground_Truth_Count: int

class SingleQueryResponse(BaseModel):
    status: str
    query_metrics: Optional[MetricsResponse]
    error: Optional[str]

class BatchQueryResponse(BaseModel):
    status: str
    total_queries_evaluated: int
    query_results: List[MetricsResponse]
    overall_metrics: Optional[Dict]
    error: Optional[str]

# Global evaluator instance
evaluator = None

@contextmanager
def get_evaluator():
    """
    Context manager for evaluator with automatic connection handling
    """
    global evaluator
    if evaluator is None:
        evaluator = Text2SQLEvaluator()

    try:
        if evaluator.connection is None or evaluator.connection.closed:
            evaluator.connect_db()
        yield evaluator
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

@app.on_event("startup")
async def startup_event():
    """
    Initialize evaluator on startup
    """
    global evaluator
    evaluator = Text2SQLEvaluator()
    if evaluator.connect_db():
        logger.info("✅ Database connected successfully on startup")
    else:
        logger.error("❌ Failed to connect to database on startup")

@app.on_event("shutdown")
async def shutdown_event():
    """
    Clean up resources on shutdown
    """
    global evaluator
    if evaluator:
        evaluator.close_connection()
        logger.info("Database connection closed")

@app.get("/")
async def root():
    """
    API 상태 확인
    """
    return {
        "service": "Text2SQL Evaluation API",
        "status": "running",
        "endpoints": {
            "single_evaluation": "/evaluate/single",
            "batch_evaluation": "/evaluate/batch",
            "health_check": "/health"
        }
    }

@app.get("/health")
async def health_check():
    """
    Health check endpoint
    """
    with get_evaluator() as eval:
        if eval.connection and not eval.connection.closed:
            return {"status": "healthy", "database": "connected"}
        else:
            return {"status": "unhealthy", "database": "disconnected"}

@app.post("/evaluate/single", response_model=SingleQueryResponse)
async def evaluate_single(request: SingleQueryRequest):
    """
    단일 쿼리 평가 API

    Args:
        request: 쿼리 번호와 예측된 product_id 리스트

    Returns:
        평가 결과 메트릭
    """
    try:
        with get_evaluator() as eval:
            # 이전 결과 초기화
            eval.reset_results()

            # 평가 수행
            result = eval.evaluate_single_query(
                query_number=request.query_number,
                predicted_ids=request.predicted_product_ids,
                verbose=False
            )

            if result is None:
                return SingleQueryResponse(
                    status="error",
                    query_metrics=None,
                    error=f"Query number {request.query_number} not found in database"
                )

            return SingleQueryResponse(
                status="success",
                query_metrics=MetricsResponse(**result),
                error=None
            )

    except Exception as e:
        logger.error(f"Error evaluating single query: {e}")
        return SingleQueryResponse(
            status="error",
            query_metrics=None,
            error=str(e)
        )

@app.post("/evaluate/batch", response_model=BatchQueryResponse)
async def evaluate_batch(request: BatchQueryRequest):
    """
    여러 쿼리 일괄 평가 API

    Args:
        request: 여러 쿼리의 번호와 예측된 product_id 리스트

    Returns:
        각 쿼리별 평가 결과와 전체 요약 메트릭
    """
    try:
        with get_evaluator() as eval:
            # 이전 결과 초기화
            eval.reset_results()

            # 배치 평가를 위한 데이터 준비
            query_predictions = [
                (q.query_number, q.predicted_product_ids)
                for q in request.queries
            ]

            # 배치 평가 수행
            results_df = eval.evaluate_batch(query_predictions, verbose=False)

            if results_df.empty:
                return BatchQueryResponse(
                    status="error",
                    total_queries_evaluated=0,
                    query_results=[],
                    overall_metrics=None,
                    error="No valid queries found for evaluation"
                )

            # 전체 메트릭 계산
            overall_metrics = eval.get_overall_metrics()

            # DataFrame을 dict 리스트로 변환
            query_results = results_df.to_dict('records')

            return BatchQueryResponse(
                status="success",
                total_queries_evaluated=len(query_results),
                query_results=[MetricsResponse(**result) for result in query_results],
                overall_metrics=overall_metrics,
                error=None
            )

    except Exception as e:
        logger.error(f"Error evaluating batch queries: {e}")
        return BatchQueryResponse(
            status="error",
            total_queries_evaluated=0,
            query_results=[],
            overall_metrics=None,
            error=str(e)
        )

@app.get("/evaluate/summary")
async def get_evaluation_summary():
    """
    현재까지의 전체 평가 요약 정보 조회

    Returns:
        전체 평가 요약 메트릭
    """
    try:
        with get_evaluator() as eval:
            overall_metrics = eval.get_overall_metrics()

            if overall_metrics is None:
                return {
                    "status": "no_data",
                    "message": "No evaluation results available"
                }

            return {
                "status": "success",
                "overall_metrics": overall_metrics
            }

    except Exception as e:
        logger.error(f"Error getting evaluation summary: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/evaluate/reset")
async def reset_evaluator():
    """
    평가 결과 초기화

    Returns:
        초기화 상태
    """
    try:
        with get_evaluator() as eval:
            eval.reset_results()
            return {
                "status": "success",
                "message": "Evaluation results have been reset"
            }

    except Exception as e:
        logger.error(f"Error resetting evaluator: {e}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    uvicorn.run(
        "text2sql_api:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )