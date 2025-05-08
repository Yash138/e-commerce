scrapy crawl AmzCategoryUrls -a list_type=bestsellers
scrapy crawl AmzCategoryUrls -a list_type=movers_and_shakers
scrapy crawl AmzCategoryUrls -a list_type=hot_new_releases
scrapy crawl AmzCategoryUrls -a list_type=most_wished_for
scrapy crawl AmzCategory -a list_type=bestsellers -a batch_size=500 --logfile=./logs/amz_category_bestsellers.log
scrapy crawl AmzCategory -a list_type=movers_and_shakers -a batch_size=500 --logfile=./logs/amz_category_movers_and_shakers.log
scrapy crawl AmzCategory -a list_type=hot_new_releases -a batch_size=500 --logfile=./logs/amz_category_hot_new_releases.log
scrapy crawl AmzCategory -a list_type=most_wished_for -a batch_size=500 --logfile=./logs/amz_category_most_wished_for.log

scrapy crawl AmzProducts -a batch_size=100 --logfile=./logs/amz_product1.log 
    
scrapy crawl AmzProductsLC -a batch_size=100 -s DEPTH_LIMIT=50 --logfile=./logs/amz_lc.log 
# -o ./.json/lc_output.json
scrapy crawl AmzProductsLC -a batch_size=100 -s DEPTH_LIMIT=10 --logfile=./logs/amz_lc.log -o ./.json/product_lc_output.json
scrapy crawl AmzProductsLC -a category=kitchen -a batch_size=1000 -s DEPTH_LIMIT=100 --logfile=./logs/amz_lc_kitchen.log
scrapy crawl AmzProductsLC -a category=kitchen -a lowest_category=3591273031 -a batch_size=1000 -s DEPTH_LIMIT=100 --logfile=./logs/amz_lc_kitchen.log


scrapy crawl CategoryRefresh -a output_file='./.json/categories_mapping1.json' --logfile=./logs/categoryrefresh1.log