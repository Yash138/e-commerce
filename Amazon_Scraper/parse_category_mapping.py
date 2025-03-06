import json
import asyncio
from helpers.utils import extract_paths
from helpers.db_postgres_handler import AsyncPostgresDBHandler 
from settings import POSTGRES_HOST, POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_PORT


with open('categories_mapping.json') as f:
    data = json.load(f)
    
data_dict = extract_paths(data)
if data_dict[0]["category"] == 'bestsellers':
    data_dict.pop(0)

async def main():    
    db = AsyncPostgresDBHandler(POSTGRES_HOST, POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_PORT)
    await db.connect()
    await db.bulk_upsert("curated.amz__category_hierarchy", data_dict, ['category', 'data_node'], update_columns=None)
    await db.close()

asyncio.run(main())
