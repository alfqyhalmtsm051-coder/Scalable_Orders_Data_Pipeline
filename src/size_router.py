"""
size_router.py

وظيفة الملف:
اختيار محرك معالجة البيانات حسب حجم ملف CSV.

<= Threshold  → Python Batch
>  Threshold  → PySpark

الملف يحدد القرار فقط، أما التنفيذ الفعلي فيديره pipeline_entry.py.
"""

import argparse
from pathlib import Path

from config.settings import (
    SMALL_FILE_THRESHOLD_MB,
    ENGINE_PYTHON_BATCH,
    ENGINE_PYSPARK,
)


def get_file_size_mb(file_path: Path) -> float:
    """
    حساب حجم الملف بالـ MiB باستخدام Metadata
    بدون قراءة محتوى الملف إلى الذاكرة.
    """

    if not file_path.exists():
        raise FileNotFoundError(
            f"Input file not found: {file_path}"
        )

    if not file_path.is_file():
        raise ValueError(
            f"Input path is not a file: {file_path}"
        )

    return file_path.stat().st_size / (1024 * 1024)


def choose_engine(file_path: Path) -> tuple[str, float]:
    """
    اختيار Python Batch أو PySpark حسب حجم الملف.
    """

    size_mb = get_file_size_mb(file_path)

    if size_mb <= SMALL_FILE_THRESHOLD_MB:
        engine = ENGINE_PYTHON_BATCH
    else:
        engine = ENGINE_PYSPARK

    return engine, size_mb


def parse_args():
    """Arguments الخاصة بتجربة File Router بشكل مستقل."""

    parser = argparse.ArgumentParser(
        description="Choose Python Batch or PySpark by file size."
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Path to the input CSV file."
    )

    return parser.parse_args()


def main():
    """تشغيل File Router وعرض القرار."""

    args = parse_args()

    file_path = Path(args.input)

    engine, size_mb = choose_engine(file_path)

    print("=" * 60)
    print("FILE ROUTER")
    print("=" * 60)

    print(f"Input file   : {file_path}")
    print(f"File size MB : {size_mb:.2f}")
    print(f"Threshold MB : {SMALL_FILE_THRESHOLD_MB}")
    print(f"Engine       : {engine}")

    print("=" * 60)


if __name__ == "__main__":
    main()