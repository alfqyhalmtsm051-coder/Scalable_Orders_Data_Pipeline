"""
pipeline_entry.py

وظيفة الملف:
نقطة التشغيل الرئيسية للـ Scalable Orders Data Pipeline.

مسار التنفيذ:
CSV
→ File Router
→ Python Batch للملفات الصغيرة / PySpark للملفات الكبيرة
→ MongoDB orders_raw
→ Quality / Cleaning ELT
→ Valid / Corrected / Quarantine

هذا الملف يعمل كـ Orchestrator:
ينسق بين أجزاء المشروع ولا ينفذ تفاصيل التنظيف أو Spark داخله.
"""

import argparse
import os
import subprocess
import sys
import uuid

from datetime import datetime, timezone
from pathlib import Path


# الإعدادات المركزية للمشروع.
from config.settings import (
    BATCH_SIZE,
    ENGINE_PYSPARK,
    ENGINE_PYTHON_BATCH,
    RAW_COLLECTION,
    SMALL_FILE_THRESHOLD_MB,
)

# Raw Loader الخاص بالملفات الصغيرة.
from src.python_batch_ingest import load_csv_to_raw

# File Router يحدد حجم الملف ومحرك المعالجة المناسب.
from src.size_router import (
    choose_engine,
    get_file_size_mb,
)


# ============================================================
# ROUTER HELPERS
# ============================================================

def normalize_engine(decision):
    """
    توحيد نتيجة File Router وإرجاع أحد المحركين فقط:
    python_batch أو pyspark.

    الدالة تدعم أكثر من شكل محتمل لقيمة الإرجاع
    مثل String أو Tuple/List أو Dictionary.
    """

    valid = {
        ENGINE_PYTHON_BATCH,
        ENGINE_PYSPARK,
    }

    if isinstance(decision, str):
        if decision in valid:
            return decision

    if isinstance(decision, dict):
        for key in (
            "engine",
            "selected_engine",
        ):
            value = decision.get(key)

            if value in valid:
                return value

    if isinstance(decision, (tuple, list)):
        for value in decision:
            if value in valid:
                return value

    raise RuntimeError(
        "Unsupported result returned by "
        f"file_router.choose_engine(): {decision!r}"
    )


def resolve_route(input_path):
    """
    تحديد مسار المعالجة المناسب للملف.

    يتم حساب الحجم ثم استخدام File Router،
    وبعدها يوجد Consistency Gate للتأكد أن القرار
    مطابق فعليًا للـ Threshold الموجود في settings.py.
    """

    path = Path(input_path).resolve()

    if not path.exists():
        raise FileNotFoundError(path)

    size_mb = get_file_size_mb(path)

    raw_decision = choose_engine(path)

    engine = normalize_engine(raw_decision)

    # تحقق مستقل من القرار لمنع توجيه الملف لمحرك غير مناسب.
    expected_engine = (
        ENGINE_PYTHON_BATCH
        if size_mb <= SMALL_FILE_THRESHOLD_MB
        else ENGINE_PYSPARK
    )

    if engine != expected_engine:
        raise RuntimeError(
            "Router consistency failure: "
            f"router={engine}, "
            f"expected={expected_engine}, "
            f"size={size_mb:,.2f} MB, "
            f"threshold={SMALL_FILE_THRESHOLD_MB} MB"
        )

    reason = (
        "file size <= configured threshold"
        if engine == ENGINE_PYTHON_BATCH
        else "file size > configured threshold"
    )

    return {
        "path": path,
        "file_size_mb": size_mb,
        "engine": engine,
        "reason": reason,
    }


# ============================================================
# PYSPARK ENVIRONMENT
# ============================================================

def build_spark_environment():
    env = os.environ.copy()
    java_home = Path(r"%USERPROFILE%\anaconda3\envs\IPPR\Library")
    java_exe = java_home / "bin" / "java.exe"

    if not java_exe.exists():
        raise RuntimeError(f"Configured Java 17 was not found at {java_exe}")

    import pyspark
    spark_home = Path(pyspark.__file__).resolve().parent

    env["JAVA_HOME"] = str(java_home)
    env["SPARK_HOME"] = str(spark_home)
    env["PYSPARK_PYTHON"] = sys.executable
    env["PYSPARK_DRIVER_PYTHON"] = sys.executable
    env["SPARK_LOCAL_IP"] = "127.0.0.1"
    env["PYTHONUTF8"] = "1"
    env["PATH"] = (
        str(java_home / "bin")
        + os.pathsep
        + str(spark_home / "bin")
        + os.pathsep
        + env.get("PATH", "")
    )

    return env


def generate_pipeline_run_id():
    """
    إنشاء معرف فريد لكل تشغيل Pipeline.

    الـ Run ID يستخدم لتتبع سجلات نفس التشغيل داخل orders_raw.
    """

    return (
        "pipeline-"
        + datetime.now(
            timezone.utc
        ).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + uuid.uuid4().hex[:8]
    )


# ============================================================
# ENGINE EXECUTION
# ============================================================

def run_batch_engine(
    input_path,
    batch_size,
):
    """
    تشغيل Python Batch Raw Loader للملفات الصغيرة.
    """

    print()
    print(
        "Dispatching             : "
        "Python Batch Loader"
    )

    return load_csv_to_raw(
        input_path,
        batch_size,
    )


def run_spark_engine(
    input_path,
    run_id,
):
    """
    تشغيل PySpark Raw Loader للملفات الكبيرة.

    Spark يعمل كعملية مستقلة حتى نمرر له
    Java / Spark Environment المجهزة للمشروع.
    """

    print()
    print(
        "Dispatching             : "
        "PySpark Loader"
    )

    command = [
        sys.executable,
        "-m",
        "src.distributed_ingest",

        "--input",
        str(input_path),

        "--collection",
        RAW_COLLECTION,

        # تصريح صريح بالكتابة Append إلى orders_raw الرسمية.
        "--production-raw",

        "--run-id",
        run_id,
    ]

    print(
        "Spark target            : "
        f"{RAW_COLLECTION}"
    )

    print(
        "Spark Raw mode          : "
        "append"
    )

    result = subprocess.run(
        command,
        env=build_spark_environment(),
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "PySpark loader failed with "
            f"exit code {result.returncode}."
        )

    return result.returncode


def run_large_elt(
    raw_run_id,
):
    """
    تشغيل مرحلة Quality / Cleaning ELT
    على Raw Run محدد بعد انتهاء عملية Raw Ingestion.

    تستخدم هذه المرحلة بعد Python Batch أو PySpark.
    """

    print()
    print(
        "Dispatching             : "
        "Quality / Cleaning ELT"
    )

    print(
        f"ELT raw_run_id          : "
        f"{raw_run_id}"
    )

    command = [
        sys.executable,
        "-m",
        "src.quality_elt",

        "--raw-run-id",
        raw_run_id,

        "--skip-dry-run-contract",
    ]

    result = subprocess.run(
        command,
        check=False,
    )

    if result.returncode != 0:
        raise RuntimeError(
            "ELT pipeline failed with "
            f"exit code {result.returncode}."
        )

    return result.returncode


# ============================================================
# COMMAND LINE ARGUMENTS
# ============================================================

def parse_args():
    """
    Arguments التي يستقبلها المشروع من Terminal.
    """

    parser = argparse.ArgumentParser(
        description=(
            "Scalable Orders Data Pipeline: "
            "File Router -> Python Batch / PySpark"
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to dirty CSV input file.",
    )

    # يستخدم فقط في مسار Python Batch.
    parser.add_argument(
        "--batch-size",
        type=int,
        default=BATCH_SIZE,
        help=(
            "Python Batch insert size. "
            "Used only for the small-file path."
        ),
    )

    # اختبار قرار Router فقط بدون معالجة أو كتابة.
    parser.add_argument(
        "--dry-route",
        action="store_true",
        help=(
            "Show Router decision without "
            "loading or modifying any data."
        ),
    )

    # Raw Ingestion فقط ثم التوقف قبل Quality ELT.
    parser.add_argument(
        "--raw-only",
        action="store_true",
        help=(
            "Stop after Raw ingestion. "
            "Useful for controlled Spark evidence capture."
        ),
    )

    return parser.parse_args()


# ============================================================
# MAIN ORCHESTRATOR
# ============================================================

def main():
    """
    تسلسل تشغيل الـPipeline:

    1. قراءة Arguments.
    2. تحديد حجم الملف والمحرك.
    3. تنفيذ Raw Ingestion.
    4. تشغيل Quality ELT.
    """

    args = parse_args()

    route = resolve_route(
        args.input
    )

    print("=" * 88)
    print(
        "HYBRID BIG DATA PIPELINE - FILE ROUTER"
    )
    print("=" * 88)

    print(
        f"Input file              : "
        f"{route['path']}"
    )

    print(
        f"File size               : "
        f"{route['file_size_mb']:,.2f} MB"
    )

    print(
        f"Configured threshold    : "
        f"{SMALL_FILE_THRESHOLD_MB} MB"
    )

    print(
        f"Selected engine         : "
        f"{route['engine']}"
    )

    print(
        f"Reason                  : "
        f"{route['reason']}"
    )

    print(
        f"Dry-route mode          : "
        f"{'YES' if args.dry_route else 'NO'}"
    )

    print("=" * 88)

    # في Dry Route نعرض قرار التوجيه فقط ولا نكتب إلى MongoDB.
    if args.dry_route:

        print()
        print(
            "ROUTER DECISION VERIFIED"
        )

        print(
            "No file processing executed."
        )

        print(
            "No MongoDB write executed."
        )

        return 0


    # ========================================================
    # SMALL FILE -> PYTHON BATCH
    # ========================================================

    if (
        route["engine"]
        == ENGINE_PYTHON_BATCH
    ):

        batch_result = run_batch_engine(
            route["path"],
            args.batch_size,
        )

        # Batch Loader ينشئ Run ID ويعيدها للـOrchestrator.
        batch_run_id = batch_result.get(
            "run_id"
        )

        if not batch_run_id:
            raise RuntimeError(
                "Python Batch loader did not "
                "return a run_id."
            )

        print(
            f"Pipeline run_id         : "
            f"{batch_run_id}"
        )

        if args.raw_only:

            print()
            print(
                "RAW-ONLY MODE: COMPLETE"
            )

            print(
                "ELT was intentionally not started."
            )

        else:

            run_large_elt(
                batch_run_id
            )


    # ========================================================
    # LARGE FILE -> PYSPARK
    # ========================================================

    elif (
        route["engine"]
        == ENGINE_PYSPARK
    ):

        pipeline_run_id = (
            generate_pipeline_run_id()
        )

        print(
            f"Pipeline run_id         : "
            f"{pipeline_run_id}"
        )

        run_spark_engine(
            route["path"],
            pipeline_run_id,
        )

        if args.raw_only:

            print()
            print(
                "RAW-ONLY MODE: COMPLETE"
            )

            print(
                "ELT was intentionally not started."
            )

        else:

            run_large_elt(
                pipeline_run_id
            )

    else:

        raise RuntimeError(
            "Unsupported engine: "
            f"{route['engine']}"
        )


    print()
    print("=" * 88)
    print(
        "HYBRID PIPELINE ENGINE EXECUTION: PASS"
    )
    print("=" * 88)

    return 0


# تشغيل main فقط عند تشغيل الملف كنقطة دخول للمشروع.
if __name__ == "__main__":
    raise SystemExit(
        main()
    )