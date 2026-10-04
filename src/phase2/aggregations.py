"""
Phase 2 - Aggregation Reports

التقارير المطلوبة:
1) sales_by_city
2) orders_by_status
3) top_customers
4) payment_methods_summary
5) sales_by_month

كل تقرير:
- يعمل بصورة مستقلة.
- يعتمد على orders_validated.
- يعيد نتائج فعلية من MongoDB.
- يمكن حفظ نتيجته في reports/phase2/aggregations.
"""

import argparse
import json
from pathlib import Path

from config.settings import (
    REPORTS_DIR,
    VALIDATED_COLLECTION,
)

from src.mongo_backend import (
    create_mongo_client,
    get_database,
)


AGGREGATION_NAMES = [
    "sales_by_city",
    "orders_by_status",
    "top_customers",
    "payment_methods_summary",
    "sales_by_month",
]


# ============================================================
# CONNECTION
# ============================================================

def get_validated_collection():
    """إرجاع Collection الخاصة بالطلبات المعتمدة."""

    client = create_mongo_client()
    db = get_database(client)

    return (
        client,
        db,
        db[VALIDATED_COLLECTION],
    )


# ============================================================
# AGGREGATION PIPELINES
# ============================================================

def build_pipeline(name):
    """
    بناء Aggregation Pipeline حسب اسم التقرير.
    """

    if name == "sales_by_city":

        return [
            {
                "$group": {
                    "_id": "$city",
                    "orders_count": {
                        "$sum": 1
                    },
                    "total_sales": {
                        "$sum": "$total_amount"
                    },
                    "average_order_value": {
                        "$avg": "$total_amount"
                    },
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "city": "$_id",
                    "orders_count": 1,
                    "total_sales": 1,
                    "average_order_value": 1,
                }
            },
            {
                "$sort": {
                    "total_sales": -1
                }
            },
        ]


    if name == "orders_by_status":

        return [
            {
                "$group": {
                    "_id": "$status",
                    "orders_count": {
                        "$sum": 1
                    },
                    "total_sales": {
                        "$sum": "$total_amount"
                    },
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "status": "$_id",
                    "orders_count": 1,
                    "total_sales": 1,
                }
            },
            {
                "$sort": {
                    "orders_count": -1
                }
            },
        ]


    if name == "top_customers":

        return [
            {
                "$group": {
                    "_id": {
                        "customer_id":
                            "$customer_id",
                        "customer_name":
                            "$customer_name",
                    },
                    "orders_count": {
                        "$sum": 1
                    },
                    "total_spent": {
                        "$sum": "$total_amount"
                    },
                    "average_order_value": {
                        "$avg": "$total_amount"
                    },
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "customer_id":
                        "$_id.customer_id",
                    "customer_name":
                        "$_id.customer_name",
                    "orders_count": 1,
                    "total_spent": 1,
                    "average_order_value": 1,
                }
            },
            {
                "$sort": {
                    "total_spent": -1
                }
            },
            {
                "$limit": 10
            },
        ]


    if name == "payment_methods_summary":

        return [
            {
                "$group": {
                    "_id":
                        "$payment_method",
                    "orders_count": {
                        "$sum": 1
                    },
                    "total_sales": {
                        "$sum": "$total_amount"
                    },
                    "average_order_value": {
                        "$avg": "$total_amount"
                    },
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "payment_method": "$_id",
                    "orders_count": 1,
                    "total_sales": 1,
                    "average_order_value": 1,
                }
            },
            {
                "$sort": {
                    "orders_count": -1
                }
            },
        ]


    if name == "sales_by_month":

        return [
            {
                "$match": {
                    "order_date": {
                        "$type": "string"
                    }
                }
            },
            {
                "$project": {
                    "month": {
                        "$substrBytes": [
                            "$order_date",
                            0,
                            7,
                        ]
                    },
                    "total_amount": 1,
                }
            },
            {
                "$group": {
                    "_id": "$month",
                    "orders_count": {
                        "$sum": 1
                    },
                    "total_sales": {
                        "$sum": "$total_amount"
                    },
                    "average_order_value": {
                        "$avg": "$total_amount"
                    },
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "month": "$_id",
                    "orders_count": 1,
                    "total_sales": 1,
                    "average_order_value": 1,
                }
            },
            {
                "$sort": {
                    "month": 1
                }
            },
        ]


    raise ValueError(
        f"Unknown aggregation: {name}"
    )


# ============================================================
# RUN AGGREGATION
# ============================================================

def run_aggregation(
    collection,
    name,
):
    """تشغيل تقرير واحد وإرجاع النتيجة الفعلية."""

    pipeline = build_pipeline(name)

    result = list(
        collection.aggregate(
            pipeline,
            allowDiskUse=True,
        )
    )

    return result


# ============================================================
# SAVE REPORT
# ============================================================

def save_report(
    name,
    result,
):
    """حفظ نتيجة التقرير في ملف JSON مستقل."""

    output_dir = (
        Path(REPORTS_DIR)
        / "phase2"
        / "aggregations"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        output_dir
        / f"{name}.json"
    )

    payload = {
        "report_name": name,
        "source_collection":
            VALIDATED_COLLECTION,
        "result_count":
            len(result),
        "results":
            result,
    }

    output_path.write_text(
        json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    return output_path


# ============================================================
# RUN ONE REPORT
# ============================================================

def run_and_save(
    collection,
    name,
):
    """تشغيل تقرير واحد وحفظه."""

    if name not in AGGREGATION_NAMES:

        raise ValueError(
            f"Unknown aggregation: {name}"
        )

    result = run_aggregation(
        collection,
        name,
    )

    path = save_report(
        name,
        result,
    )

    return {
        "report_name":
            name,

        "result_count":
            len(result),

        "report_file":
            str(path),

        "results":
            result,
    }


# ============================================================
# RUN ALL REPORTS
# ============================================================

def run_all():
    """تشغيل وحفظ التقارير الخمسة."""

    (
        client,
        _db,
        collection,
    ) = get_validated_collection()

    try:

        if (
            collection.count_documents({})
            == 0
        ):
            raise RuntimeError(
                "orders_validated is empty."
            )

        reports = {}

        for name in AGGREGATION_NAMES:

            reports[name] = (
                run_and_save(
                    collection,
                    name,
                )
            )

        return reports

    finally:
        client.close()


# ============================================================
# CLI
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Phase 2 Aggregation Reports"
        )
    )

    parser.add_argument(
        "--list",
        action="store_true",
        help="List available aggregation reports.",
    )

    parser.add_argument(
        "--run",
        choices=AGGREGATION_NAMES,
        help="Run one aggregation report.",
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Run all five aggregation reports.",
    )

    args = parser.parse_args()


    if args.list:

        print(
            json.dumps(
                {
                    "aggregations":
                        AGGREGATION_NAMES
                },
                indent=2,
            )
        )

        return


    if args.run:

        (
            client,
            _db,
            collection,
        ) = get_validated_collection()

        try:

            if (
                collection.count_documents({})
                == 0
            ):
                raise RuntimeError(
                    "orders_validated is empty."
                )

            report = run_and_save(
                collection,
                args.run,
            )

            print(
                json.dumps(
                    {
                        "status":
                            "SUCCESS",

                        **report,
                    },
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                )
            )

        finally:
            client.close()

        return


    if args.all:

        reports = run_all()

        print(
            json.dumps(
                {
                    "status":
                        "SUCCESS",

                    "reports_count":
                        len(reports),

                    "reports": {
                        name: {
                            "result_count":
                                report[
                                    "result_count"
                                ],

                            "report_file":
                                report[
                                    "report_file"
                                ],
                        }

                        for name, report
                        in reports.items()
                    },
                },
                ensure_ascii=False,
                indent=2,
                default=str,
            )
        )

        return


    parser.print_help()


if __name__ == "__main__":
    main()
