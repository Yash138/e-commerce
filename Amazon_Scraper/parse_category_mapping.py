import json
import asyncio
from helpers.utils import extract_paths, extract_categories
from helpers.db_postgres_handler import AsyncPostgresDBHandler 
from settings import POSTGRES_HOST, POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_PORT


with open('categories_mapping.json') as f:
    data = json.load(f)
    
data_dict = extract_paths(data)
if data_dict[0].get("category") == 'bestsellers':
    data_dict.pop(0)

category_list = extract_categories(data['bestsellers'])
if category_list[0].get("category_id") == 'bestsellers':
    category_list.pop(0)

async def main():    
    db = AsyncPostgresDBHandler(POSTGRES_HOST, POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_PORT)
    await db.connect()
    await db.bulk_upsert("curated.amz__category_hierarchy", data_dict, ['category', 'data_node'], update_columns=None)
    keys = set()
    for d in category_list:
        if d['category_id'] in keys:
            continue
        keys.add(d['category_id'])
        await db.insert('curated.amz__category_hierarchy_flattened', d)
    
    await db.close()

asyncio.run(main())
