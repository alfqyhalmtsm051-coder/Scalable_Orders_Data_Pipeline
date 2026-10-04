"""
Phase 2 - Scheduled Jobs

Jobs:
1) refresh_materialized_views
2) generate_aggregation_reports

Features:
- APScheduler schedules.
- Manual execution.
- Start/end timestamps.
- SUCCESS / FAILED status.
- Result/error logging in MongoDB.
- JSON report for every run.
"""

import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from config.settings import REPORTS_DIR

from src.mongo_backend import (
    create_mongo_client,
    get_database,
)

from src.phase2.aggregations import (
    run_all as run_all_aggregations,
)

from src.phase2.materialized_views import (
    refresh_all as refresh_all_materialized_views,
)


JOB_RUNS_COLLECTION = "phase2_job_runs"

SCHEDULER_TIMEZONE = "UTC"


JOB_DEFINITIONS = {
    "refresh_materialized_views": {
        "description":
            "Incrementally refresh both materialized views.",

        "schedule": {
            "type": "cron",
            "hour": 0,
            "minute": 5,
            "timezone": SCHEDULER_TIMEZONE,
        },
    },

    "generate_aggregation_reports": {
        "description":
            "Generate all Phase 2 aggregation reports.",

        "schedule": {
            "type": "cron",
            "hour": 0,
            "minute": 15,
            "timezone": SCHEDULER_TIMEZONE,
        },
    },
}


# ============================================================
# HELPERS
# ============================================================

def utc_now():
    return datetime.now(timezone.utc)


def json_safe(value):
    """
    تحويل النتيجة إلى شكل JSON-safe
    قبل تخزينها أو طباعتها.
    """

    return json.loads(
        json.dumps(
            value,
            default=str,
            ensure_ascii=False,
        )
    )


def get_job_runs_collection():
    client = create_mongo_client()
    db = get_database(client)

    return (
        client,
        db[JOB_RUNS_COLLECTION],
    )


def save_job_report(run_record):
    """
    حفظ نسخة JSON لكل تشغيل.
    """

    output_dir = (
        Path(REPORTS_DIR)
        / "phase2"
        / "jobs"
    )

    output_dir.mkdir(
        parents=True,
        exist_ok=True,
    )

    filename = (
        f"{run_record['job_name']}_"
        f"{run_record['run_id']}.json"
    )

    output_path = (
        output_dir
        / filename
    )

    output_path.write_text(
        json.dumps(
            run_record,
            ensure_ascii=False,
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    return output_path


# ============================================================
# JOB FUNCTIONS
# ============================================================

def job_refresh_materialized_views():
    """
    المهمة الأولى:
    تحديث Materialized Views.
    """

    return (
        refresh_all_materialized_views()
    )


def job_generate_aggregation_reports():
    """
    المهمة الثانية:
    إنشاء Aggregation Reports.
    """

    reports = run_all_aggregations()

    return {
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
    }


JOB_FUNCTIONS = {
    "refresh_materialized_views":
        job_refresh_materialized_views,

    "generate_aggregation_reports":
        job_generate_aggregation_reports,
}


# ============================================================
# JOB EXECUTION + LOGGING
# ============================================================

def run_job(
    job_name,
    triggered_by="manual",
):
    """
    تشغيل Job وتسجيل:
    - البداية
    - النهاية
    - الحالة
    - النتيجة أو الخطأ
    """

    if job_name not in JOB_FUNCTIONS:

        raise ValueError(
            f"Unknown job: {job_name}"
        )

    run_id = str(
        uuid.uuid4()
    )

    start_time = utc_now()

    record = {
        "run_id":
            run_id,

        "job_name":
            job_name,

        "triggered_by":
            triggered_by,

        "start_time":
            start_time,

        "end_time":
            None,

        "status":
            "RUNNING",

        "result":
            None,

        "error":
            None,
    }

    client = None

    try:

        client, collection = (
            get_job_runs_collection()
        )

        collection.create_index(
            "run_id",
            unique=True,
            name="uq_phase2_job_run_id",
        )

        collection.create_index(
            [
                ("job_name", 1),
                ("start_time", -1),
            ],
            name="ix_phase2_job_history",
        )

        collection.insert_one(
            record.copy()
        )

        try:

            result = (
                JOB_FUNCTIONS[
                    job_name
                ]()
            )

            end_time = utc_now()

            safe_result = json_safe(
                result
            )

            record.update(
                {
                    "end_time":
                        end_time,

                    "status":
                        "SUCCESS",

                    "result":
                        safe_result,

                    "error":
                        None,
                }
            )

        except Exception as exc:

            end_time = utc_now()

            record.update(
                {
                    "end_time":
                        end_time,

                    "status":
                        "FAILED",

                    "result":
                        None,

                    "error":
                        (
                            f"{type(exc).__name__}: "
                            f"{exc}"
                        ),
                }
            )

        collection.update_one(
            {
                "run_id":
                    run_id
            },
            {
                "$set": {
                    "end_time":
                        record[
                            "end_time"
                        ],

                    "status":
                        record[
                            "status"
                        ],

                    "result":
                        record[
                            "result"
                        ],

                    "error":
                        record[
                            "error"
                        ],
                }
            },
        )

        report_path = save_job_report(
            record
        )

        record[
            "report_file"
        ] = str(report_path)

        if (
            record["status"]
            == "FAILED"
        ):
            raise RuntimeError(
                record["error"]
            )

        return record

    finally:

        if client is not None:
            client.close()


# ============================================================
# JOB LIST
# ============================================================

def list_jobs():
    """
    معلومات المهمتين وجدولهما.
    """

    jobs = []

    for name, definition in (
        JOB_DEFINITIONS.items()
    ):

        jobs.append(
            {
                "name":
                    name,

                "description":
                    definition[
                        "description"
                    ],

                "schedule":
                    definition[
                        "schedule"
                    ],

                "manual_run":
                    True,
            }
        )

    return jobs


# ============================================================
# JOB HISTORY
# ============================================================

def get_job_history(
    limit=20,
):
    client, collection = (
        get_job_runs_collection()
    )

    try:

        cursor = (
            collection
            .find(
                {},
                {
                    "_id": 0
                },
            )
            .sort(
                "start_time",
                -1,
            )
            .limit(limit)
        )

        return list(cursor)

    finally:
        client.close()


# ============================================================
# APSCHEDULER WRAPPER
# ============================================================

def scheduled_job_wrapper(
    job_name,
):
    """
    يستخدمه APScheduler حتى نسجل أن
    التشغيل جاء من Scheduler.
    """

    try:

        result = run_job(
            job_name,
            triggered_by="scheduler",
        )

        print(
            json.dumps(
                {
                    "scheduler_job":
                        job_name,

                    "status":
                        result[
                            "status"
                        ],

                    "run_id":
                        result[
                            "run_id"
                        ],
                },
                indent=2,
                default=str,
            )
        )

    except Exception as exc:

        print(
            json.dumps(
                {
                    "scheduler_job":
                        job_name,

                    "status":
                        "FAILED",

                    "error":
                        str(exc),
                },
                indent=2,
            )
        )


# ============================================================
# SCHEDULER
# ============================================================

def create_scheduler():
    """
    إنشاء Scheduler وإضافة المهمتين.
    """

    scheduler = BlockingScheduler(
        timezone=
            SCHEDULER_TIMEZONE
    )

    for (
        job_name,
        definition,
    ) in JOB_DEFINITIONS.items():

        schedule = (
            definition[
                "schedule"
            ]
        )

        trigger = CronTrigger(
            hour=
                schedule[
                    "hour"
                ],

            minute=
                schedule[
                    "minute"
                ],

            timezone=
                schedule[
                    "timezone"
                ],
        )

        scheduler.add_job(
            scheduled_job_wrapper,

            trigger=trigger,

            args=[
                job_name
            ],

            id=
                job_name,

            name=
                job_name,

            replace_existing=True,

            coalesce=True,

            max_instances=1,
        )

    return scheduler


def start_scheduler():
    """
    تشغيل Scheduler بصورة مستمرة.
    Ctrl+C لإيقافه.
    """

    scheduler = (
        create_scheduler()
    )

    print(
        json.dumps(
            {
                "status":
                    "SCHEDULER_STARTED",

                "timezone":
                    SCHEDULER_TIMEZONE,

                "jobs":
                    list_jobs(),
            },
            ensure_ascii=False,
            indent=2,
        )
    )

    try:
        scheduler.start()

    except (
        KeyboardInterrupt,
        SystemExit,
    ):
        print(
            "Scheduler stopped."
        )


# ============================================================
# CLI
# ============================================================

def main():

    parser = argparse.ArgumentParser(
        description=(
            "Phase 2 Scheduled Jobs"
        )
    )

    parser.add_argument(
        "--list",
        action="store_true",
        help="List jobs and schedules.",
    )

    parser.add_argument(
        "--run",
        choices=list(
            JOB_DEFINITIONS.keys()
        ),
        help="Run one job manually.",
    )

    parser.add_argument(
        "--history",
        action="store_true",
        help="Show previous job executions.",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="History result limit.",
    )

    parser.add_argument(
        "--start",
        action="store_true",
        help="Start APScheduler.",
    )

    args = parser.parse_args()


    if args.list:

        print(
            json.dumps(
                {
                    "jobs_count":
                        len(
                            JOB_DEFINITIONS
                        ),

                    "timezone":
                        SCHEDULER_TIMEZONE,

                    "jobs":
                        list_jobs(),
                },
                ensure_ascii=False,
                indent=2,
            )
        )

        return


    if args.run:

        result = run_job(
            args.run,
            triggered_by="manual",
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


    if args.history:

        result = get_job_history(
            args.limit
        )

        print(
            json.dumps(
                {
                    "history_count":
                        len(result),

                    "history":
                        result,
                },
                ensure_ascii=False,
                indent=2,
                default=str,
            )
        )

        return


    if args.start:

        start_scheduler()
        return


    parser.print_help()


if __name__ == "__main__":
    main()
