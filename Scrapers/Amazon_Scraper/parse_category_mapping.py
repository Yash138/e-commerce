import json
import asyncio
import sys
from helpers.utils import extract_paths, extract_categories
from helpers.postgres_handler_async import AsyncPostgresDBHandler 
from settings import POSTGRES_HOST, POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_PORT

if len(sys.argv) < 2:
    print("Usage: python parse_category_mapping.py <file_path>")
    sys.exit(1)

file_path = sys.argv[1]
print(f"Reading file: {file_path}")
with open(file_path) as f:
    data = json.load(f)
    
if data.get("category") == 'bestsellers':
    data = data['bestsellers']

data_dict = extract_paths(data)
category_list = extract_categories(data)

async def main():    
    db = AsyncPostgresDBHandler(POSTGRES_HOST, POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_PORT)
    await db.connect()
    await db.bulk_upsert("transformed.amz__category_hierarchy", data_dict, ['category', 'data_node'], update_columns=None)
    keys = set()
    for d in category_list:
        if d['category_id'] in keys:
            continue
        keys.add(d['category_id'])
        await db.insert('transformed.amz__category_hierarchy_flattened', d)
    
    await db.close()

asyncio.run(main())
