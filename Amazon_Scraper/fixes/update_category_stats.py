
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from helpers.postgres_handler import PostgresDBHandler
from settings import POSTGRES_HOST, POSTGRES_DATABASE, POSTGRES_USERNAME, POSTGRES_PASSWORD, POSTGRES_PORT
from category_stats import category_stats

def update_category_stats(update_category):
    with PostgresDBHandler(
            POSTGRES_HOST,
            POSTGRES_DATABASE,
            POSTGRES_USERNAME,
            POSTGRES_PASSWORD,
            POSTGRES_PORT
    ) as postgres_handler:
        # print(f"Category stats {update_category}")
        # self.log(f"Failed URLs: {self.failed_urls}", 20)
        # self.postgres_handler.connect()
        # instead of bulk_upsert, we need to update these values in the table one at a time
        for row in update_category:
            try:
                print(f"Updating category stats for {row['category']} | {row['lowest_category']}")
                postgres_handler.update(
                    table="transformed.amz__category_refresh_controller", 
                    data={
                        "refreshed_pages_upto": row["refreshed_pages_upto"],
                        "total_pages": row["total_pages"],
                        "last_refresh_timestamp": row["last_refresh_timestamp"],
                        "products_per_page": row.get("products_per_page", 0)
                    },
                    conditions={
                        "category": row["category"],
                        "lowest_category": row["lowest_category"]
                    }
                )
            except Exception as e:
                print(f"Error updating category stats for {row['category']} | {row['lowest_category']}: {e}", 40)

update_category = category_stats
update_category_stats(update_category)