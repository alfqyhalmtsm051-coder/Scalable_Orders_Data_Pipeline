"""
Phase 2 - Unified FastAPI

Required endpoints:
GET  /health
POST /ingest
POST /indexes
GET  /queries
GET  /queries/{name}
GET  /aggregations
GET  /aggregations/{name}
POST /refresh-mv
GET  /jobs
POST /jobs/{name}/run

Swagger:
GET /docs
"""

import subprocess
import sys
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel

from config.settings import VALIDATED_COLLECTION

from src.mongo_backend import (
    create_mongo_client,
    get_database,
)

from src.phase2.queries_indexes import (
    QUERY_NAMES,
    create_phase2_indexes,
    get_validated_collection,
    run_query,
)

from src.phase2.aggregations import (
    AGGREGATION_NAMES,
    get_validated_collection as get_aggregation_collection,
    run_and_save,
)

from src.phase2.materialized_views import (
    VIEW_NAMES,
    refresh_all,
    refresh_view,
)

from src.phase2.scheduled_jobs import (
    JOB_DEFINITIONS,
    list_jobs,
    run_job,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]


app = FastAPI(
    title="Scalable Orders Data Pipeline - Phase 2 API",
    description=(
        "Unified API for automated evaluation of the "
        "Big Data project."
    ),
    version="2.0.0",
)


# ============================================================
# REQUEST MODELS
# ============================================================

class IngestRequest(BaseModel):
    input_path: str


class RefreshMVRequest(BaseModel):
    view_name: Optional[str] = None


# ============================================================
# HEALTH
# ============================================================

@app.get("/health")
def health():
    """
    Check API + MongoDB connectivity.
    """

    client = None

    try:
        client = create_mongo_client()
        db = get_database(client)

        db.command("ping")

        validated_count = (
            db[VALIDATED_COLLECTION]
            .count_documents({})
        )

        return {
            "status": "OK",
            "mongodb": "OK",
            "database": db.name,
            "validated_records":
                validated_count,
            "api_version": "2.0.0",
        }

    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Health check failed: {exc}",
        )

    finally:
        if client is not None:
            client.close()


# ============================================================
# INGEST
# ============================================================

@app.post("/ingest")
def ingest(request: IngestRequest):
    """
    Uses the SAME Phase 1 pipeline_entry.

    No new ingestion pipeline is created.
    """

    input_path = Path(
        request.input_path
    )

    if not input_path.is_absolute():
        input_path = (
            PROJECT_ROOT
            / input_path
        )

    input_path = input_path.resolve()

    if not input_path.exists():
        raise HTTPException(
            status_code=404,
            detail=(
                f"Input file not found: "
                f"{input_path}"
            ),
        )

    if not input_path.is_file():
        raise HTTPException(
            status_code=400,
            detail="input_path must be a file.",
        )

    command = [
        sys.executable,
        "-m",
        "src.pipeline_entry",
        "--input",
        str(input_path),
    ]

    completed = subprocess.run(
        command,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    if completed.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail={
                "message":
                    "Pipeline execution failed.",
                "return_code":
                    completed.returncode,
                "stdout":
                    completed.stdout,
                "stderr":
                    completed.stderr,
            },
        )

    return {
        "status": "SUCCESS",
        "pipeline":
            "src.pipeline_entry",
        "input_path":
            str(input_path),
        "return_code":
            completed.returncode,
        "stdout":
            completed.stdout,
        "stderr":
            completed.stderr,
    }


# ============================================================
# INDEXES
# ============================================================

@app.post("/indexes")
def indexes():
    """
    Create the three Phase 2 indexes.
    """

    (
        client,
        _db,
        collection,
    ) = get_validated_collection()

    try:
        result = create_phase2_indexes(
            collection
        )

        return {
            "status": "SUCCESS",
            "indexes_count":
                len(result),
            "indexes":
                result,
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        )

    finally:
        client.close()


# ============================================================
# QUERIES
# ============================================================

@app.get("/queries")
def queries():
    """
    List the five available queries.
    """

    return {
        "count":
            len(QUERY_NAMES),
        "queries":
            QUERY_NAMES,
    }


@app.get("/queries/{name}")
def query_by_name(
    name: str,

    status: Optional[str] = None,
    city: Optional[str] = None,
    customer_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,

    limit: int = Query(
        default=100,
        ge=1,
        le=1000,
    ),
):
    """
    Run one named query using URL query parameters.
    """

    if name not in QUERY_NAMES:
        raise HTTPException(
            status_code=404,
            detail={
                "message":
                    f"Unknown query: {name}",
                "available_queries":
                    QUERY_NAMES,
            },
        )

    params = {
        "status": status,
        "city": city,
        "customer_id":
            customer_id,
        "start_date":
            start_date,
        "end_date":
            end_date,
    }

    (
        client,
        _db,
        collection,
    ) = get_validated_collection()

    try:
        try:
            result = run_query(
                collection,
                name,
                params,
                limit=limit,
            )

        except ValueError as exc:
            raise HTTPException(
                status_code=400,
                detail=str(exc),
            )

        return {
            "query_name":
                name,
            "parameters":
                {
                    key: value
                    for key, value
                    in params.items()
                    if value is not None
                },
            "result_count":
                len(result),
            "results":
                jsonable_encoder(
                    result
                ),
        }

    finally:
        client.close()


# ============================================================
# AGGREGATIONS
# ============================================================

@app.get("/aggregations")
def aggregations():
    """
    List the five aggregation reports.
    """

    return {
        "count":
            len(
                AGGREGATION_NAMES
            ),
        "aggregations":
            AGGREGATION_NAMES,
    }


@app.get("/aggregations/{name}")
def aggregation_by_name(
    name: str,
):
    """
    Run one aggregation independently.
    """

    if (
        name
        not in AGGREGATION_NAMES
    ):
        raise HTTPException(
            status_code=404,
            detail={
                "message":
                    (
                        "Unknown "
                        f"aggregation: {name}"
                    ),
                "available_aggregations":
                    AGGREGATION_NAMES,
            },
        )

    (
        client,
        _db,
        collection,
    ) = (
        get_aggregation_collection()
    )

    try:
        report = run_and_save(
            collection,
            name,
        )

        return jsonable_encoder(
            {
                "status":
                    "SUCCESS",
                **report,
            }
        )

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        )

    finally:
        client.close()


# ============================================================
# MATERIALIZED VIEWS
# ============================================================

@app.post("/refresh-mv")
def refresh_materialized_views(
    request: RefreshMVRequest,
):
    """
    Refresh both views, or one selected view.

    Uses incremental watermark logic.
    """

    try:
        if request.view_name is None:

            result = refresh_all()

            return jsonable_encoder(
                {
                    "status":
                        "SUCCESS",
                    "refresh_scope":
                        "all",
                    "views":
                        result,
                }
            )

        if (
            request.view_name
            not in VIEW_NAMES
        ):
            raise HTTPException(
                status_code=404,
                detail={
                    "message":
                        (
                            "Unknown "
                            "materialized view: "
                            f"{request.view_name}"
                        ),
                    "available_views":
                        VIEW_NAMES,
                },
            )

        result = refresh_view(
            request.view_name
        )

        return jsonable_encoder(
            {
                "status":
                    "SUCCESS",
                "refresh_scope":
                    "single",
                "view":
                    result,
            }
        )

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        )


# ============================================================
# JOBS
# ============================================================

@app.get("/jobs")
def jobs():
    """
    List scheduled jobs and schedules.
    """

    return {
        "jobs_count":
            len(
                JOB_DEFINITIONS
            ),
        "jobs":
            list_jobs(),
    }


@app.post("/jobs/{name}/run")
def run_named_job(
    name: str,
):
    """
    Run a scheduled job manually.
    """

    if name not in JOB_DEFINITIONS:
        raise HTTPException(
            status_code=404,
            detail={
                "message":
                    f"Unknown job: {name}",
                "available_jobs":
                    list(
                        JOB_DEFINITIONS
                        .keys()
                    ),
            },
        )

    try:
        result = run_job(
            name,
            triggered_by="api_manual",
        )

        return jsonable_encoder(
            result
        )

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        )


# ============================================================
# ROOT
# ============================================================

@app.get("/")
def root():
    return {
        "project":
            "Scalable Orders Data Pipeline",
        "phase":
            "Big Data - Phase 2",
        "status":
            "READY",
        "swagger":
            "/docs",
        "health":
            "/health",
    }
