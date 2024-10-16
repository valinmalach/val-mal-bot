# import os
# from datetime import datetime

# from dotenv import load_dotenv
# from pytz import timezone
# from xata import XataClient

# load_dotenv()

# XATA_API_KEY = os.getenv("XATA_API_KEY")
# DATABASE_URL = os.getenv("DATABASE_URL")

# client = XataClient(api_key=XATA_API_KEY, db_url=DATABASE_URL)

# users_table_schema = {
#     "columns": [
#         {"name": "username", "type": "string", "unique": True},
#         {"name": "birthday", "type": "datetime", "notNull": False},
#     ],
# }

# assert client.table().create("users").is_success()

# resp = client.table().set_schema("users", users_table_schema)

# assert resp.is_success(), resp

# date_with_tz = (
#     datetime.strptime("2024-10-21 00:00:00", "%Y-%m-%d %H:%M:%S")
#     .astimezone(timezone("Australia/Melbourne"))
#     .isoformat()
# )

# record = {
#     "username": "valinmalach",
#     "birthday": date_with_tz,
# }
# resp = client.records().insert_with_id("users", "389318636201967628", record)
