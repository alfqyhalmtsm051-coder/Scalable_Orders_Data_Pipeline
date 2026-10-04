"""
Phase 2 - Materialized Views

Views:
1) daily_sales_summary
2) top_products_summary

Incremental strategy:
- Source: orders_validated
- Watermark: last_updated_at
- First refresh: BOOTSTRAP
- Later refreshes: process only documents newer than previous watermark.
- Only affected aggregation buckets are rebuilt.
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

from pymongo import ASCENDING, DESCENDING

from config.settings import (
    REPORTS_DIR,
    VALIDATED_COLLECTION,
)

from src.mongo_backend import (
    create_mongo_client,
    get_database,
)


DAILY_VIEW = "daily_sales_summary"
PRODUCT_VIEW = "top_products_summary"

METADATA_COLLECTION = "phase2_mv_metadata"

DAILY_CONTRIBUTIONS = (
    "phase2_mv_daily_contributions"
)

PRODUCT_CONTRIBUTIONS = (
    "phase2_mv_product_contributions"
)

WATERMARK_FIELD = "last_updated_at"

VIEW_NAMES = [
    DAILY_VIEW,
    PRODUCT_VIEW,
]


# ============================================================
# GENERAL HELPERS
# ============================================================

def utc_now():
    return datetime.now(timezone.utc)


def get_db():
    client = create_mongo_client()
    db = get_database(client)

    return client, db


def to_number(value):
    """تحويل القيم الرقمية إلى float بصورة آمنة."""

    if value is None:
        return 0.0

    try:
        if hasattr(value, "to_decimal"):
            value = value.to_decimal()

        return float(value)

    except (TypeError, ValueError):
        return 0.0


def order_key(document):
    """
    استخدام order_id كمفتاح أساسي للمساهمات.
    """

    value = document.get("order_id")

    if value not in (None, ""):
        return str(value)

    return str(document["_id"])


def normalize_day(value):
    """
    تحويل order_date إلى YYYY-MM-DD.
    المشروع الحالي يخزن order_date كنص ISO.
    """

    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")

    value = str(value or "").strip()

    if len(value) >= 10:
        return value[:10]

    return None


def latest_source_watermark(source):
    """
    أخذ أعلى last_updated_at موجود حاليًا.
    """

    document = source.find_one(
        {
            WATERMARK_FIELD: {
                "$type": "date"
            }
        },
        {
            "_id": 0,
            WATERMARK_FIELD: 1,
        },
        sort=[
            (
                WATERMARK_FIELD,
                DESCENDING,
            )
        ],
    )

    if not document:
        return None

    return document.get(
        WATERMARK_FIELD
    )


def previous_watermark(
    metadata_collection,
    view_name,
):
    document = metadata_collection.find_one(
        {
            "_id": view_name
        }
    )

    if not document:
        return None

    return document.get(
        "last_watermark"
    )


def changed_filter(
    previous,
    upper,
):
    """
    تحديد نافذة التحديث التزايدي.

    previous < last_updated_at <= upper
    """

    condition = {
        "$type": "date",
        "$lte": upper,
    }

    if previous is not None:
        condition["$gt"] = previous

    return {
        WATERMARK_FIELD: condition
    }


def save_refresh_report(
    view_name,
    result,
):
    output_dir = (
        Path(REPORTS_DIR)
        / "phase2"
        / "materialized_views"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    output_path = (
        output_dir
        / f"{view_name}_refresh.json"
    )

    output_path.write_text(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    return output_path


def save_metadata(
    metadata,
    view_name,
    previous,
    upper,
    mode,
    processed,
    affected,
    started_at,
    finished_at,
):
    metadata.replace_one(
        {
            "_id": view_name
        },
        {
            "_id": view_name,
            "view_name": view_name,
            "watermark_field":
                WATERMARK_FIELD,
            "previous_watermark":
                previous,
            "last_watermark":
                upper,
            "refresh_mode":
                mode,
            "processed_source_records":
                processed,
            "affected_buckets":
                affected,
            "started_at":
                started_at,
            "finished_at":
                finished_at,
            "status":
                "SUCCESS",
        },
        upsert=True,
    )


# ============================================================
# DAILY SALES MATERIALIZED VIEW
# ============================================================

def refresh_daily_sales(db):
    """
    Incremental refresh for daily_sales_summary.

    نحتفظ بمساهمة كل Order في Collection صغيرة.
    عند تغير Order نعرف اليوم القديم والجديد،
    ثم نعيد Aggregation للأيام المتأثرة فقط.
    """

    source = db[
        VALIDATED_COLLECTION
    ]

    view = db[
        DAILY_VIEW
    ]

    contributions = db[
        DAILY_CONTRIBUTIONS
    ]

    metadata = db[
        METADATA_COLLECTION
    ]

    started_at = utc_now()

    upper = latest_source_watermark(
        source
    )

    previous = previous_watermark(
        metadata,
        DAILY_VIEW,
    )

    if upper is None:
        raise RuntimeError(
            "orders_validated has no usable "
            "last_updated_at values."
        )

    mode = (
        "BOOTSTRAP"
        if previous is None
        else "INCREMENTAL"
    )

    if mode == "BOOTSTRAP":
        view.delete_many({})
        contributions.delete_many({})

    contributions.create_index(
        [
            ("day", ASCENDING)
        ],
        name="ix_mv_daily_day",
    )

    affected_days = set()

    processed = 0

    cursor = source.find(
        changed_filter(
            previous,
            upper,
        ),
        {
            "_id": 1,
            "order_id": 1,
            "order_date": 1,
            "total_amount": 1,
            WATERMARK_FIELD: 1,
        },
    )

    for document in cursor:

        processed += 1

        key = order_key(
            document
        )

        old = contributions.find_one(
            {
                "_id": key
            },
            {
                "_id": 0,
                "day": 1,
            },
        )

        if old and old.get("day"):
            affected_days.add(
                old["day"]
            )

        day = normalize_day(
            document.get(
                "order_date"
            )
        )

        if not day:
            contributions.delete_one(
                {
                    "_id": key
                }
            )

            continue

        affected_days.add(day)

        contributions.replace_one(
            {
                "_id": key
            },
            {
                "_id": key,
                "order_id":
                    document.get(
                        "order_id"
                    ),
                "day":
                    day,
                "total_amount":
                    to_number(
                        document.get(
                            "total_amount"
                        )
                    ),
                WATERMARK_FIELD:
                    document.get(
                        WATERMARK_FIELD
                    ),
            },
            upsert=True,
        )

    # حذف الـBuckets المتأثرة أولًا،
    # ثم إعادة تكوينها من contribution data فقط.
    if affected_days:

        days = sorted(
            affected_days
        )

        view.delete_many(
            {
                "_id": {
                    "$in": days
                }
            }
        )

        pipeline = [
            {
                "$match": {
                    "day": {
                        "$in": days
                    }
                }
            },
            {
                "$group": {
                    "_id": "$day",
                    "orders_count": {
                        "$sum": 1
                    },
                    "total_sales": {
                        "$sum":
                            "$total_amount"
                    },
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "day": "$_id",
                    "orders_count": 1,
                    "total_sales": 1,
                    "average_order_value": {
                        "$cond": [
                            {
                                "$gt": [
                                    "$orders_count",
                                    0,
                                ]
                            },
                            {
                                "$divide": [
                                    "$total_sales",
                                    "$orders_count",
                                ]
                            },
                            0,
                        ]
                    },
                    "refreshed_at": {
                        "$literal":
                            utc_now()
                    },
                }
            },
            {
                "$merge": {
                    "into":
                        DAILY_VIEW,
                    "on":
                        "_id",
                    "whenMatched":
                        "replace",
                    "whenNotMatched":
                        "insert",
                }
            },
        ]

        list(
            contributions.aggregate(
                pipeline,
                allowDiskUse=True,
            )
        )

    finished_at = utc_now()

    save_metadata(
        metadata,
        DAILY_VIEW,
        previous,
        upper,
        mode,
        processed,
        len(affected_days),
        started_at,
        finished_at,
    )

    result = {
        "view_name":
            DAILY_VIEW,
        "status":
            "SUCCESS",
        "refresh_mode":
            mode,
        "watermark_field":
            WATERMARK_FIELD,
        "previous_watermark":
            previous,
        "new_watermark":
            upper,
        "processed_source_records":
            processed,
        "affected_buckets":
            len(affected_days),
        "document_count":
            view.count_documents({}),
        "started_at":
            started_at,
        "finished_at":
            finished_at,
    }

    path = save_refresh_report(
        DAILY_VIEW,
        result,
    )

    result["report_file"] = str(path)

    return result


# ============================================================
# PRODUCT HELPERS
# ============================================================

def parse_order_products(document):
    """
    تحويل items_json إلى مساهمات Product
    خاصة بالطلب الحالي.

    إذا تكرر المنتج داخل نفس الطلب،
    يتم دمجه أولًا حتى orders_count = 1
    لهذا المنتج داخل الطلب.
    """

    raw_items = document.get(
        "items_json"
    )

    if isinstance(raw_items, str):

        try:
            items = json.loads(
                raw_items
            )

        except json.JSONDecodeError:
            return []

    elif isinstance(raw_items, list):
        items = raw_items

    else:
        return []

    grouped = {}

    for item in items:

        if not isinstance(
            item,
            dict,
        ):
            continue

        sku = str(
            item.get("sku")
            or ""
        ).strip()

        name = str(
            item.get("name")
            or ""
        ).strip()

        if sku:
            product_key = (
                f"sku:{sku}"
            )

        elif name:
            product_key = (
                f"name:{name}"
            )

        else:
            continue

        qty = to_number(
            item.get("qty")
        )

        revenue = to_number(
            item.get("total")
        )

        if revenue == 0:

            revenue = (
                qty
                * to_number(
                    item.get(
                        "unit_price"
                    )
                )
            )

        current = grouped.setdefault(
            product_key,
            {
                "product_key":
                    product_key,
                "sku":
                    sku or None,
                "product_name":
                    name or None,
                "quantity_sold":
                    0.0,
                "total_sales":
                    0.0,
            },
        )

        current[
            "quantity_sold"
        ] += qty

        current[
            "total_sales"
        ] += revenue

    return list(
        grouped.values()
    )


# ============================================================
# TOP PRODUCTS MATERIALIZED VIEW
# ============================================================

def refresh_top_products(db):
    """
    Incremental refresh for top_products_summary.

    كل Order له مساهمات Product مستقلة.
    عند تحديث Order:
    - نحذف مساهماته القديمة فقط.
    - نضيف مساهماته الجديدة.
    - نعيد Aggregation للمنتجات المتأثرة فقط.
    """

    source = db[
        VALIDATED_COLLECTION
    ]

    view = db[
        PRODUCT_VIEW
    ]

    contributions = db[
        PRODUCT_CONTRIBUTIONS
    ]

    metadata = db[
        METADATA_COLLECTION
    ]

    started_at = utc_now()

    upper = latest_source_watermark(
        source
    )

    previous = previous_watermark(
        metadata,
        PRODUCT_VIEW,
    )

    if upper is None:
        raise RuntimeError(
            "orders_validated has no usable "
            "last_updated_at values."
        )

    mode = (
        "BOOTSTRAP"
        if previous is None
        else "INCREMENTAL"
    )

    if mode == "BOOTSTRAP":
        view.delete_many({})
        contributions.delete_many({})

    contributions.create_index(
        [
            ("order_id", ASCENDING)
        ],
        name="ix_mv_product_order",
    )

    contributions.create_index(
        [
            ("product_key", ASCENDING)
        ],
        name="ix_mv_product_key",
    )

    affected_products = set()

    processed = 0

    cursor = source.find(
        changed_filter(
            previous,
            upper,
        ),
        {
            "_id": 1,
            "order_id": 1,
            "items_json": 1,
            WATERMARK_FIELD: 1,
        },
    )

    for document in cursor:

        processed += 1

        key = order_key(
            document
        )

        old_documents = list(
            contributions.find(
                {
                    "order_id":
                        key
                },
                {
                    "_id": 0,
                    "product_key": 1,
                },
            )
        )

        for old in old_documents:

            product_key = old.get(
                "product_key"
            )

            if product_key:
                affected_products.add(
                    product_key
                )

        # مساهمات الطلب القديمة لم تعد صالحة.
        contributions.delete_many(
            {
                "order_id": key
            }
        )

        products = parse_order_products(
            document
        )

        new_documents = []

        for product in products:

            product_key = product[
                "product_key"
            ]

            affected_products.add(
                product_key
            )

            new_documents.append(
                {
                    "_id":
                        (
                            f"{key}|"
                            f"{product_key}"
                        ),
                    "order_id":
                        key,
                    "product_key":
                        product_key,
                    "sku":
                        product["sku"],
                    "product_name":
                        product[
                            "product_name"
                        ],
                    "quantity_sold":
                        product[
                            "quantity_sold"
                        ],
                    "total_sales":
                        product[
                            "total_sales"
                        ],
                    "orders_count":
                        1,
                    WATERMARK_FIELD:
                        document.get(
                            WATERMARK_FIELD
                        ),
                }
            )

        if new_documents:

            contributions.insert_many(
                new_documents,
                ordered=False,
            )

    if affected_products:

        product_keys = sorted(
            affected_products
        )

        view.delete_many(
            {
                "_id": {
                    "$in":
                        product_keys
                }
            }
        )

        pipeline = [
            {
                "$match": {
                    "product_key": {
                        "$in":
                            product_keys
                    }
                }
            },
            {
                "$group": {
                    "_id":
                        "$product_key",
                    "sku": {
                        "$first":
                            "$sku"
                    },
                    "product_name": {
                        "$first":
                            "$product_name"
                    },
                    "orders_count": {
                        "$sum":
                            "$orders_count"
                    },
                    "quantity_sold": {
                        "$sum":
                            "$quantity_sold"
                    },
                    "total_sales": {
                        "$sum":
                            "$total_sales"
                    },
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "product_key":
                        "$_id",
                    "sku": 1,
                    "product_name": 1,
                    "orders_count": 1,
                    "quantity_sold": 1,
                    "total_sales": 1,
                    "average_sales_per_order": {
                        "$cond": [
                            {
                                "$gt": [
                                    "$orders_count",
                                    0,
                                ]
                            },
                            {
                                "$divide": [
                                    "$total_sales",
                                    "$orders_count",
                                ]
                            },
                            0,
                        ]
                    },
                    "refreshed_at": {
                        "$literal":
                            utc_now()
                    },
                }
            },
            {
                "$merge": {
                    "into":
                        PRODUCT_VIEW,
                    "on":
                        "_id",
                    "whenMatched":
                        "replace",
                    "whenNotMatched":
                        "insert",
                }
            },
        ]

        list(
            contributions.aggregate(
                pipeline,
                allowDiskUse=True,
            )
        )

    # ترتيب المنتجات بعد التحديث.
    view.create_index(
        [
            ("total_sales", DESCENDING)
        ],
        name="ix_mv_products_sales",
    )

    view.create_index(
        [
            ("rank", ASCENDING)
        ],
        name="ix_mv_products_rank",
    )

    ranking_cursor = view.find(
        {},
        {
            "_id": 1
        },
    ).sort(
        [
            (
                "total_sales",
                DESCENDING,
            ),
            (
                "quantity_sold",
                DESCENDING,
            ),
            (
                "_id",
                ASCENDING,
            ),
        ]
    )

    for rank, document in enumerate(
        ranking_cursor,
        start=1,
    ):
        view.update_one(
            {
                "_id":
                    document["_id"]
            },
            {
                "$set": {
                    "rank":
                        rank
                }
            },
        )

    finished_at = utc_now()

    save_metadata(
        metadata,
        PRODUCT_VIEW,
        previous,
        upper,
        mode,
        processed,
        len(affected_products),
        started_at,
        finished_at,
    )

    result = {
        "view_name":
            PRODUCT_VIEW,
        "status":
            "SUCCESS",
        "refresh_mode":
            mode,
        "watermark_field":
            WATERMARK_FIELD,
        "previous_watermark":
            previous,
        "new_watermark":
            upper,
        "processed_source_records":
            processed,
        "affected_buckets":
            len(affected_products),
        "document_count":
            view.count_documents({}),
        "started_at":
            started_at,
        "finished_at":
            finished_at,
    }

    path = save_refresh_report(
        PRODUCT_VIEW,
        result,
    )

    result["report_file"] = str(path)

    return result


# ============================================================
# PUBLIC REFRESH FUNCTIONS
# ============================================================

def refresh_view(
    view_name,
):
    client, db = get_db()

    try:

        if view_name == DAILY_VIEW:
            return refresh_daily_sales(
                db
            )

        if view_name == PRODUCT_VIEW:
            return refresh_top_products(
                db
            )

        raise ValueError(
            f"Unknown materialized view: "
            f"{view_name}"
        )

    finally:
        client.close()


def refresh_all():
    client, db = get_db()

    try:

        return {
            DAILY_VIEW:
                refresh_daily_sales(
                    db
                ),

            PRODUCT_VIEW:
                refresh_top_products(
                    db
                ),
        }

    finally:
        client.close()


def show_view(
    view_name,
    limit=20,
):
    client, db = get_db()

    try:

        collection = db[
            view_name
        ]

        if view_name == PRODUCT_VIEW:

            cursor = (
                collection
                .find(
                    {},
                    {
                        "_id": 0
                    },
                )
                .sort(
                    "rank",
                    ASCENDING,
                )
                .limit(limit)
            )

        else:

            cursor = (
                collection
                .find(
                    {},
                    {
                        "_id": 0
                    },
                )
                .sort(
                    "day",
                    ASCENDING,
                )
                .limit(limit)
            )

        return list(cursor)

    finally:
        client.close()


# ============================================================
# CLI
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Phase 2 Incremental Materialized Views"
        )
    )

    parser.add_argument(
        "--list",
        action="store_true",
        help="List materialized views.",
    )

    parser.add_argument(
        "--refresh",
        choices=VIEW_NAMES,
        help="Refresh one materialized view.",
    )

    parser.add_argument(
        "--all",
        action="store_true",
        help="Refresh both materialized views.",
    )

    parser.add_argument(
        "--show",
        choices=VIEW_NAMES,
        help="Show saved materialized view data.",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Maximum documents shown with --show.",
    )

    args = parser.parse_args()


    if args.list:

        print(
            json.dumps(
                {
                    "materialized_views":
                        VIEW_NAMES,
                    "watermark_field":
                        WATERMARK_FIELD,
                },
                indent=2,
            )
        )

        return


    if args.refresh:

        result = refresh_view(
            args.refresh
        )

        print(
            json.dumps(
                result,
                ensure_ascii=False,
                indent=2,
                default=str,
            )
        )

        return


    if args.all:

        result = refresh_all()

        print(
            json.dumps(
                {
                    "status":
                        "SUCCESS",
                    "views_count":
                        len(result),
                    "views":
                        result,
                },
                ensure_ascii=False,
                indent=2,
                default=str,
            )
        )

        return


    if args.show:

        result = show_view(
            args.show,
            args.limit,
        )

        print(
            json.dumps(
                {
                    "view_name":
                        args.show,
                    "result_count":
                        len(result),
                    "results":
                        result,
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
