"""
settings.py

وظيفة الملف:
الإعدادات المركزية للـ Scalable Orders Data Pipeline.

يحتوي على:
- مسارات المشروع.
- MongoDB Configuration.
- Collections.
- File Router Threshold.
- Python Batch Size.
- Spark Configuration.
- أسماء المحركات وحالات الجودة.

الملف لا يعالج البيانات بنفسه.
"""

import os
from pathlib import Path


# ============================================================
# PROJECT PATHS
# ============================================================

PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parent
    .parent
)

DATA_DIR = (
    PROJECT_ROOT
    / "data"
)

INPUT_DIR = (
    DATA_DIR
    / "input"
)

SAMPLES_DIR = (
    DATA_DIR
    / "samples"
)

REPORTS_DIR = (
    PROJECT_ROOT
    / "reports"
)

RESULTS_JSON_PATH = (
    REPORTS_DIR
    / "results.json"
)

SCREENSHOTS_DIR = (
    REPORTS_DIR
    / "screenshots"
)


# ============================================================
# MONGODB
# ============================================================

MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://127.0.0.1:27017",
)

MONGO_DATABASE = os.getenv(
    "MONGO_DATABASE",
    "orders_bigdata_pipeline",
)

RAW_COLLECTION = (
    "orders_raw"
)

VALIDATED_COLLECTION = (
    "orders_validated"
)

QUARANTINE_COLLECTION = (
    "orders_quarantine"
)


# ============================================================
# FILE ROUTER
# ============================================================

# <= 200MB يستخدم Python Batch.
# > 200MB يستخدم PySpark.
SMALL_FILE_THRESHOLD_MB = int(
    os.getenv(
        "SMALL_FILE_THRESHOLD_MB",
        "200",
    )
)


# ============================================================
# PYTHON BATCH
# ============================================================

# عدد Documents في كل MongoDB insert_many.
# هذا Batching للكتابة وليس Parallel Processing.
BATCH_SIZE = int(
    os.getenv(
        "BATCH_SIZE",
        "5000",
    )
)


# ============================================================
# APACHE SPARK
# ============================================================

SPARK_APP_NAME = (
    "BigDataHybridPipeline"
)

# local[*] يستخدم جميع CPU Cores المتاحة محليًا.
SPARK_MASTER = os.getenv(
    "SPARK_MASTER",
    "local[*]",
)

MONGO_SPARK_FORMAT = (
    "mongodb"
)


# ============================================================
# PROJECT CONSTANTS
# ============================================================

ENGINE_PYTHON_BATCH = (
    "python_batch"
)

ENGINE_PYSPARK = (
    "pyspark"
)

QUALITY_VALID = (
    "valid"
)

QUALITY_CORRECTED = (
    "corrected"
)

QUALITY_QUARANTINED = (
    "quarantined"
)