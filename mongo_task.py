
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
import json
import os


# подключение
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
DB_NAME = os.environ.get("MONGO_DB", "my_database")
SRC_COLL = "user_events"
ARCHIVE_COLL = "archived_users"


# пороговые даты 
now = datetime.now(timezone.utc)   
registered_before = now - timedelta(days=30)
inactive_since = now - timedelta(days=14)


def main():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    events = db[SRC_COLL]
    archived = db[ARCHIVE_COLL]

    # последняя активность и дата регистрации
    pipeline = [
        {
            "$group": {
                "_id": "$user_id",
                "last_activity": {"$max": "$event_time"},
                "registration_date": {"$min": "$user_info.registration_date"},
            }
        },
        {
            # только те, кто старше 30 дней и не активен 14 дней
            "$match": {
                "$expr": {
                    "$and": [
                        {"$lte": ["$registration_date", registered_before]},
                        {"$lt": ["$last_activity", inactive_since]},
                    ]
                }
            }
        },
    ]

    candidates = list(events.aggregate(pipeline))

    archived_user_ids = []
    archived_at = now

    for doc in candidates:
        user_id = doc["_id"]
        reg = doc.get("registration_date")
        last_act = doc.get("last_activity")

        # upsert: создаём запись, если её нет; если запись есть, то не перезаписываем archived_at
        archived.update_one(
            {"user_id": user_id},
            {
                "$setOnInsert": {
                    "user_id": user_id,
                    "registration_date": reg,
                    "last_activity": last_act,
                    "archived_at": archived_at,
                    "reason": {
                        "registered_before": registered_before,
                        "inactive_since": inactive_since,
                    },
                }
            },
            upsert=True,
        )

        # проверка: действительно ли пользователь впервые попал в архив сегодня,
        # иначе - в сегодняшний отчёт не добавляем
        was_inserted = db.command(
            "find", ARCHIVE_COLL,
            filter={"user_id": user_id, "archived_at": archived_at}
        )["cursor"]["firstBatch"]
        if was_inserted:
            archived_user_ids.append(user_id)

    # отчёт
    report_date_str = now.date().isoformat()  # YYYY-MM-DD
    report_dir = os.path.join(os.getcwd(), "reports")
    os.makedirs(report_dir, exist_ok=True)
    report_path = os.path.join(report_dir, f"{report_date_str}.json")

    report = {
        "date": report_date_str,
        "archived_users_count": len(archived_user_ids),
        "archived_user_ids": sorted(archived_user_ids),
    }

    # json
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=4)

    print(f"✅ Архивация завершена. Пользователей добавлено: {len(archived_user_ids)}")
    print(f"🧾 Отчёт: {report_path}")

if __name__ == "__main__":
    main()

##################################################################################################