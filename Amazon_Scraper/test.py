import asyncio
import json
from helpers.db_postgres_handler import AsyncPostgresDBHandler, PostgresDBHandler 
from settings import POSTGRES_HOST, POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_PORT


async def main(batch_size=10):
    items = list()
    db = AsyncPostgresDBHandler(POSTGRES_HOST, POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_PORT)
    await db.connect()
    for d in data:
        items.append(d)
        if len(items) >= batch_size:
            await db.bulk_upsert('staging.stg__dummy', items, conflict_columns=['category', 'subcategory'])
            items = list()
    if items:
        await db.bulk_upsert('staging.stg__dummy', items, conflict_columns=['category', 'subcategory'])
    await db.close()

with open(".json/lc_pages.json") as f:
    data = json.load(f)

asyncio.run(main(100))