"""
Phase 2 - Queries, Indexes and Explain

هذا الملف مسؤول عن:
1) خمسة استعلامات عملية على orders_validated.
2) ثلاثة فهارس جديدة خاصة بـ Phase 2.
3) أحد الفهارس Compound Index.
4) Explain executionStats قبل وبعد إنشاء الفهارس.
5) حفظ تقرير المقارنة داخل reports/phase2.
"""

import argparse
import json
from pathlib import Path

from pymongo import ASCENDING

from config.settings import (
    REPORTS_DIR,
    VALIDATED_COLLECTION,
)

from src.mongo_backend import (
    create_mongo_client,
    get_database,
)


# ============================================================
# PHASE 2 INDEX DEFINITIONS
# ============================================================

PHASE2_INDEXES = {
    "ix_phase2_status": [
        ("status", ASCENDING),
    ],

    "ix_phase2_customer_id": [
        ("customer_id", ASCENDING),
    ],

    # Compound Index:
    # يخدم البحث حسب المدينة مع فترة زمنية.
    "ix_phase2_city_order_date": [
        ("city", ASCENDING),
        ("order_date", ASCENDING),
    ],
}


QUERY_NAMES = [
    "orders_by_status",
    "orders_by_city",
    "orders_by_customer",
    "orders_by_date_range",
    "orders_by_city_and_date",
]


# ============================================================
# CONNECTION
# ============================================================

def get_validated_collection():
    """الاتصال بـ MongoDB وإرجاع orders_validated."""

    client = create_mongo_client()
    db = get_database(client)

    return (
        client,
        db,
        db[VALIDATED_COLLECTION],
    )


# ============================================================
# QUERY BUILDERS
# ============================================================

def build_query(name, params):
    """
    بناء MongoDB Filter حسب اسم الاستعلام.

    القيم تأتي من المستخدم أو من الـAPI لاحقًا،
    ولا توجد أسماء مدن أو عملاء أو نتائج ثابتة داخل الكود.
    """

    if name == "orders_by_status":
        status = params.get("status")

        if not status:
            raise ValueError(
                "orders_by_status requires: status"
            )

        return {
            "status": status,
        }


    if name == "orders_by_city":
        city = params.get("city")

        if not city:
            raise ValueError(
                "orders_by_city requires: city"
            )

        return {
            "city": city,
        }


    if name == "orders_by_customer":
        customer_id = params.get("customer_id")

        if not customer_id:
            raise ValueError(
                "orders_by_customer requires: customer_id"
            )

        return {
            "customer_id": customer_id,
        }


    if name == "orders_by_date_range":
        start_date = params.get("start_date")
        end_date = params.get("end_date")

        if not start_date or not end_date:
            raise ValueError(
                "orders_by_date_range requires: "
                "start_date and end_date"
            )

        return {
            "order_date": {
                "$gte": start_date,
                "$lte": end_date,
            }
        }


    if name == "orders_by_city_and_date":
        city = params.get("city")
        start_date = params.get("start_date")
        end_date = params.get("end_date")

        if (
            not city
            or not start_date
            or not end_date
        ):
            raise ValueError(
                "orders_by_city_and_date requires: "
                "city, start_date and end_date"
            )

        return {
            "city": city,
            "order_date": {
                "$gte": start_date,
                "$lte": end_date,
            },
        }


    raise ValueError(
        f"Unknown query name: {name}"
    )


# ============================================================
# RUN QUERY
# ============================================================

def run_query(
    collection,
    name,
    params,
    limit=100,
):
    """تشغيل Query وإرجاع نتيجة حقيقية من MongoDB."""

    mongo_filter = build_query(
        name,
        params,
    )

    cursor = (
        collection
        .find(
            mongo_filter,
            {
                "_id": 0,
                "order_id": 1,
                "order_date": 1,
                "status": 1,
                "customer_id": 1,
                "customer_name": 1,
                "city": 1,
                "payment_method": 1,
                "payment_status": 1,
                "currency": 1,
                "total_amount": 1,
            },
        )
        .limit(limit)
    )

    return list(cursor)


# ============================================================
# INDEX MANAGEMENT
# ============================================================

def create_phase2_indexes(collection):
    """إنشاء الفهارس الثلاثة المطلوبة للمشروع النهائي."""

    created = []

    for index_name, keys in PHASE2_INDEXES.items():

        actual_name = (
            collection.create_index(
                keys,
                name=index_name,
            )
        )

        created.append(
            {
                "name": actual_name,
                "keys": [
                    {
                        "field": field,
                        "direction": direction,
                    }
                    for field, direction in keys
                ],
            }
        )

    return created


def drop_phase2_indexes(collection):
    """
    حذف فهارس Phase 2 فقط.

    لا يتم حذف أي Index تابع للمشروع النصفي.
    """

    existing = {
        item["name"]
        for item in collection.list_indexes()
    }

    dropped = []

    for index_name in PHASE2_INDEXES:

        if index_name in existing:
            collection.drop_index(
                index_name
            )

            dropped.append(
                index_name
            )

    return dropped


# ============================================================
# EXPLAIN
# ============================================================

def explain_query(
    db,
    collection,
    name,
    params,
    limit=100,
):
    """
    تنفيذ explain("executionStats") على Query.
    """

    mongo_filter = build_query(
        name,
        params,
    )

    command = {
        "find": collection.name,
        "filter": mongo_filter,
        "limit": limit,
    }

    result = db.command({
        "explain": command,
        "verbosity": "executionStats",
    })

    stats = result.get(
        "executionStats",
        {},
    )

    winning_plan = (
        result
        .get("queryPlanner", {})
        .get("winningPlan", {})
    )

    return {
        "query": name,
        "filter": mongo_filter,

        "execution_time_ms":
            stats.get(
                "executionTimeMillis",
                0,
            ),

        "documents_examined":
            stats.get(
                "totalDocsExamined",
                0,
            ),

        "keys_examined":
            stats.get(
                "totalKeysExamined",
                0,
            ),

        "documents_returned":
            stats.get(
                "nReturned",
                0,
            ),

        "winning_plan":
            winning_plan,
    }


# ============================================================
# DYNAMIC SAMPLE VALUES
# ============================================================

def get_sample_parameters(collection):
    """
    أخذ قيم حقيقية من البيانات لاستخدامها في تجربة Explain.

    لا نعتمد على مدينة أو عميل أو حالة مكتوبة مسبقًا،
    لأن الدكتور قد يستخدم Dataset مختلفة.
    """

    sample = collection.find_one(
        {
            "status": {
                "$exists": True,
                "$ne": "",
            },
            "customer_id": {
                "$exists": True,
                "$ne": "",
            },
            "city": {
                "$exists": True,
                "$ne": "",
            },
            "order_date": {
                "$exists": True,
                "$ne": "",
            },
        },
        {
            "_id": 0,
            "status": 1,
            "customer_id": 1,
            "city": 1,
            "order_date": 1,
        },
    )

    if not sample:
        raise RuntimeError(
            "orders_validated does not contain "
            "a usable sample record."
        )

    order_date = str(
        sample["order_date"]
    )

    # إذا كان التاريخ ISO نستخدم يوم السجل نفسه.
    if len(order_date) >= 10:

        day = order_date[:10]

        start_date = (
            day + "T00:00:00"
        )

        end_date = (
            day + "T23:59:59.999999"
        )

    else:
        # Fallback لبيانات اختبار مختلفة.
        start_date = order_date
        end_date = order_date

    return {
        "orders_by_status": {
            "status":
                sample["status"],
        },

        "orders_by_customer": {
            "customer_id":
                sample["customer_id"],
        },

        "orders_by_city_and_date": {
            "city":
                sample["city"],

            "start_date":
                start_date,

            "end_date":
                end_date,
        },

        # القيم التالية متاحة أيضًا لتجربة
        # الاستعلامين الآخرين يدويًا أو عبر الـAPI.
        "orders_by_city": {
            "city":
                sample["city"],
        },

        "orders_by_date_range": {
            "start_date":
                start_date,

            "end_date":
                end_date,
        },
    }


# ============================================================
# BEFORE / AFTER EXPLAIN
# ============================================================

def run_explain_comparison():
    """
    تنفيذ Explain لثلاثة Queries:

    1) قبل إنشاء فهارس Phase 2.
    2) إنشاء الفهارس.
    3) بعد إنشاء الفهارس.
    4) حفظ التقرير JSON.
    """

    (
        client,
        db,
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

        sample_params = (
            get_sample_parameters(
                collection
            )
        )

        explain_queries = [
            "orders_by_status",
            "orders_by_customer",
            "orders_by_city_and_date",
        ]

        # نحذف فقط فهارس Phase 2 لكي يكون
        # قياس BEFORE حقيقيًا.
        dropped = (
            drop_phase2_indexes(
                collection
            )
        )

        before = {}

        for name in explain_queries:

            before[name] = explain_query(
                db,
                collection,
                name,
                sample_params[name],
            )

        created = (
            create_phase2_indexes(
                collection
            )
        )

        after = {}

        for name in explain_queries:

            after[name] = explain_query(
                db,
                collection,
                name,
                sample_params[name],
            )

        comparison = {}

        for name in explain_queries:

            b = before[name]
            a = after[name]

            comparison[name] = {
                "before_documents_examined":
                    b["documents_examined"],

                "after_documents_examined":
                    a["documents_examined"],

                "before_keys_examined":
                    b["keys_examined"],

                "after_keys_examined":
                    a["keys_examined"],

                "before_execution_time_ms":
                    b["execution_time_ms"],

                "after_execution_time_ms":
                    a["execution_time_ms"],
            }

        report = {
            "collection":
                VALIDATED_COLLECTION,

            "sample_parameters":
                sample_params,

            "dropped_phase2_indexes":
                dropped,

            "created_indexes":
                created,

            "before":
                before,

            "after":
                after,

            "comparison":
                comparison,
        }

        output_dir = (
            Path(REPORTS_DIR)
            / "phase2"
        )

        output_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        output_path = (
            output_dir
            / "explain_before_after.json"
        )

        output_path.write_text(
            json.dumps(
                report,
                ensure_ascii=False,
                indent=2,
                default=str,
            ),
            encoding="utf-8",
        )

        return (
            report,
            output_path,
        )

    finally:
        client.close()


# ============================================================
# CLI
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Phase 2 Queries, Indexes and Explain"
        )
    )

    parser.add_argument(
        "--list",
        action="store_true",
        help="List the five available queries.",
    )

    parser.add_argument(
        "--create-indexes",
        action="store_true",
        help="Create Phase 2 indexes.",
    )

    parser.add_argument(
        "--explain",
        action="store_true",
        help=(
            "Run executionStats before and after "
            "Phase 2 indexes."
        ),
    )

    args = parser.parse_args()


    if args.list:

        print(
            json.dumps(
                {
                    "queries":
                        QUERY_NAMES
                },
                indent=2,
            )
        )

        return


    if args.create_indexes:

        (
            client,
            _db,
            collection,
        ) = get_validated_collection()

        try:

            result = (
                create_phase2_indexes(
                    collection
                )
            )

            print(
                json.dumps(
                    {
                        "status":
                            "SUCCESS",

                        "indexes":
                            result,
                    },
                    indent=2,
                    default=str,
                )
            )

        finally:
            client.close()

        return


    if args.explain:

        report, path = (
            run_explain_comparison()
        )

        print(
            json.dumps(
                {
                    "status":
                        "SUCCESS",

                    "report":
                        str(path),

                    "comparison":
                        report[
                            "comparison"
                        ],
                },
                indent=2,
                default=str,
            )
        )

        return


    parser.print_help()


if __name__ == "__main__":
    main()
