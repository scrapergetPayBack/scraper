
import asyncio
import json
from multiprocessing import Process, Queue, cpu_count
from queue import Empty
import re
import httpx
from datetime import datetime, timedelta
from db_models import PriceHistoryModel, ProductModel, UpdatePriceHistoryModel, UpdateProductModel
import database
from bson import ObjectId
import charset_normalizer
import uvicorn
from fastapi import FastAPI
from routes import router
import os



MAX_RETRIES = 3
RETRY_BACKOFF = 2
MAX_CONCURRENT_PAGES = 20
BATCH_SIZE = 10
MAX_WORKERS = 4





async def fetch_products(session, shop_url, page=1, retries=MAX_RETRIES):
    """
    Fetches products from a Shopify store's /products.json endpoint with ETag support.

    Args:
        session (httpx.AsyncClient): A persistent HTTP session.
        shop_url (str): The Shopify store URL.
        page (int): The page number to fetch.
        retries (int): Number of retry attempts.

    Returns:
        list: List of products (if any).
    """

    url = f"https://{shop_url}/products.json?page={page}"
    headers = {
        "Accept-Encoding": "gzip",
        "User-Agent": "Mozilla/5.0",
    }

    # Retrieve ETag from the database
    etag = await database.get_etag_from_db(shop_url, page)
    print(etag)
    # Attach ETag to the header if it exists
    if etag:
        headers["If-None-Match"] = etag

    non_retryable_statuses = {404, 410}  # Non-recoverable errors

    for attempt in range(retries):
        try:
            # Send HTTP GET request
            response = await session.get(url, headers=headers, timeout=30)

            # Handle 304 Not Modified
            if response.status_code == 304:
                print(f"[INFO] No updates for {url} (ETag: {etag})")
                return []

            # Raise exception for HTTP errors
            response.raise_for_status()



            products = response.json().get("products", [])
            new_etag = response.headers.get("etag")
            # Save ETag to the database
            if etag:
                await database.save_etag_to_db(shop_url, page, new_etag, True)
            else:
                await database.save_etag_to_db(shop_url, page, new_etag, False)

            return products

        except httpx.HTTPStatusError as e:
            if e.response.status_code in non_retryable_statuses:
                print(f"[ERROR] Non-recoverable error {e.response.status_code} for {url}. Skipping retries.")
                return []
            print(f"[WARNING] Recoverable error {e.response.status_code} for {url}. Retrying...")

        except httpx.RequestError as e:
            print(f"[ERROR] Network error for {url}: {e}. Retrying...")

        except UnicodeDecodeError as e:
            print(f"[ERROR] Decoding error: {e}. Retrying...")

        except Exception as e:
            print(f"[ERROR] Unexpected error for {url}: {e}. Retrying...")

        # Exponential backoff with jitter
        await asyncio.sleep(2 ** attempt + 0.1 * attempt)

    # If all retries fail, return empty result
    print(f"[ERROR] Failed to fetch {url} after {retries} retries.")
    return []





async def calculate_sale_percentage(shop_id):
    number_of_products_on_sale = await database.number_of_products_on_sale(shop_id)
    print("Number of sale", number_of_products_on_sale)
    total_number_of_products = await database.get_number_of_products(shop_id)

    if(total_number_of_products == 0):
        return 0

    percentage_of_products_on_sale = (number_of_products_on_sale / total_number_of_products) * 100
    return round(percentage_of_products_on_sale)



async def calculate_average_discount(shop_id):
    """
    Difference between price and compare_at_price of each product
    If there is no compare_at_price then the difference should be zero
    And average for every product
    """
    average_discount = await database.calculate_average_discount(shop_id)
    await database.save_average_discount(shop_id)






def calculate_scan_frequency(sale_percentage):
    if(sale_percentage >= 50):
        return '72 hours'
    elif(sale_percentage < 50 and sale_percentage > 25):
        return '7 days'
    else:
        return '30 days'
    


    
def calculate_next_scan_due_date(scan_frequency: str) -> datetime:
    """
    Calculates the next scan due date based on the scan frequency.

    Parameters:
    - scan_frequency (str): Frequency of scans, in the format "72 hours" or "7 days".

    Returns:
    - datetime: The calculated due date for the next scan.
    """
    # Parse the scan_frequency
    match = re.match(r'(\d+) (hours|days)', scan_frequency)
    if not match:
        raise ValueError("Invalid scan_frequency format. Expected format: '<number> hours' or '<number> days'.")

    # Extract the quantity and unit from the scan_frequency string
    quantity = int(match.group(1))
    unit = match.group(2)

    # Calculate the next scan date
    if unit == 'hours':
        next_scan_due_date = datetime.utcnow() + timedelta(hours=quantity)
    elif unit == 'days':
        next_scan_due_date = datetime.utcnow() + timedelta(days=quantity)

    return next_scan_due_date






async def process_the_products(products, shop_domain):
    """
    Process all of the products of one shop identified by its domain (using the existing logic).
    """
    shop_id = await database.find_shop_id_by_domain(shop_domain)
    if not shop_id:
        print(f"Shop not found for domain {shop_domain}")
        return

    if not products:
        return


    product_queries = [
        {"shop_id": ObjectId(shop_id), "title": product["title"], "title2": variant["title"]}
        for product in products
        for variant in product["variants"]
    ]

    existing_products = await database.find_products_in_batch(product_queries)
    product_lookup = {
        (str(product["shop_id"]), product["title"], product["title2"]): product
        for product in existing_products
    }

    new_products_batch, update_products_batch, price_history_batch_create, price_history_batch_update = [], [], [], []

    for product in products:
        for variant in product["variants"]:
            product_data = {
                "shop_id": ObjectId(shop_id),
                "title": product["title"],
                "title2": variant["title"],
                "price": int(float(variant["price"]) * 100),
                "compare_at_price": int(float(variant["compare_at_price"]) * 100) if variant.get("compare_at_price") else None,
                "is_on_sale": False,
            }

            lookup_key = (str(product_data["shop_id"]), product_data["title"], product_data["title2"])
            existing_product = product_lookup.get(lookup_key)

            if existing_product:
                if product_data["price"] != existing_product["price"]:
                    price_history = await database.find_price_history_document(ObjectId(existing_product["_id"]))
                    if price_history:
                        price_history_batch_update.append({
                            "identifier": {"product_id": ObjectId(existing_product["_id"])},
                            "history_entry": {
                                "price": existing_product["price"],
                                "compare_at_price": existing_product["compare_at_price"],
                                "timestamp": existing_product["last_scanned"],
                            },
                        })
                    else:
                        price_history_batch_create.append({
                            "product_id": ObjectId(existing_product["_id"]),
                            "price": existing_product["price"],
                            "compare_at_price": existing_product["compare_at_price"],
                            "timestamp": existing_product["last_scanned"],
                        })
                    product_data["is_on_sale"] = product_data["price"] < existing_product["price"]
                    update_products_batch.append({
                        "identifier": {"_id": ObjectId(existing_product["_id"])},
                        "data": product_data,
                    })
            else:
                product_data["is_on_sale"] = (
                    bool(product_data["compare_at_price"]) and product_data["price"] < product_data["compare_at_price"]
                )
                new_products_batch.append(product_data)


    tasks = []

    if new_products_batch:
        tasks.append(database.batch_insert(database.get_products_collection(), new_products_batch, ProductModel))
    
    if update_products_batch:
        tasks.append(database.batch_update(database.get_products_collection(), update_products_batch, UpdateProductModel))

    if price_history_batch_create:
        tasks.append(database.batch_insert_price_history(database.get_price_history_collection(), price_history_batch_create, PriceHistoryModel))
    
    if price_history_batch_update:
        tasks.append(database.batch_update_price_history(database.get_price_history_collection(), price_history_batch_update))

    # Perform batch operations

    if tasks:
        await asyncio.gather(*tasks)



def get_scraper_state():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    STATE_FILE = os.path.join(BASE_DIR, "scraper_state.json")
    try:
        with open(STATE_FILE, "r") as f:
            state = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        # File is missing or corrupted, so create a default state
        state = {"running": True, "paused": False}

    return state




async def enqueue_products():
    """
    Fetch shops from the database and process them dynamically using a queue.
    """
    queue = asyncio.Queue()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)

    # Producer: Fetch shops from the database and add them to the queue
    async def producer():
        scraper_state = get_scraper_state()
        if not scraper_state.get("running", False):  # Default to False if missing
            scraper_state["running"] = False
        
        # Ensure paused key is not None and exists
        if not scraper_state.get("paused", False):  # Default to False if missing
            scraper_state["paused"] = False

        

        while scraper_state["running"]:
            scraper_state = get_scraper_state()
            if not scraper_state.get("running", False):  # Default to False if missing
                scraper_state["running"] = False
            
            # Ensure paused key is not None and exists
            if not scraper_state.get("paused", False):  # Default to False if missing
                scraper_state["paused"] = False
            
            if scraper_state["paused"]:
                continue

            async for shop_batch in database.get_shops_in_batches(BATCH_SIZE):
                for shop in shop_batch:
                    shop_domain = shop["domain"]
                    for shop_url in shop.get("shop_urls", []):
                        await queue.put((shop_url, shop_domain))
                if not scraper_state["running"]:
                    break
            
            if not scraper_state["running"]:
                break
            # Add termination signals for workers
            for _ in range(MAX_CONCURRENT_PAGES):
                await queue.put(None)

    # Consumer: Process shops from the queue
    async def consumer():
        async with httpx.AsyncClient() as session:
            while True:
                scraper_state = get_scraper_state()
                if not scraper_state.get("running", False):  # Default to False if missing
                    scraper_state["running"] = False
                
                # Ensure paused key is not None and exists
                if not scraper_state.get("paused", False):  # Default to False if missing
                    scraper_state["paused"] = False



                shop_data = await queue.get()
                if shop_data is None:  # Termination signal
                    break
                
                # Pause handling
                while scraper_state["paused"]:
                    await asyncio.sleep(1)
                    scraper_state = get_scraper_state()
                    if not scraper_state.get("running", False):  # Default to False if missing
                        scraper_state["running"] = False
                    
                    # Ensure paused key is not None and exists
                    if not scraper_state.get("paused", False):  # Default to False if missing
                        scraper_state["paused"] = False
                

                shop_url, shop_domain = shop_data
                await fetch_products_for_shop(session, shop_url, semaphore, shop_domain)
                # now that we have finished with one shop completely we can calculate the sale percentage and everything else
                # find the shop_id from the db
                shop_id = await database.find_shop_id_by_domain(shop_domain)
                sale_percentage = await calculate_sale_percentage(shop_id)
                scan_frequency = calculate_scan_frequency(sale_percentage)
                next_scan_due_date = calculate_next_scan_due_date(scan_frequency)
                #now update the shop document
                await database.update_shop_fields({"_id": ObjectId(shop_id)}, {
                    "products_on_sale_percentage":sale_percentage,
                    "scan_frequency": scan_frequency,
                    "next_scan_due_date": next_scan_due_date,
                })
                queue.task_done()

    # Start producer and consumers
    #do i need to put my scraper runing logic here 
    producer_task = asyncio.create_task(producer())
    consumer_tasks = [asyncio.create_task(consumer()) for _ in range(MAX_CONCURRENT_PAGES)]

    await producer_task
    await queue.join()
    for task in consumer_tasks:
        await task



async def fetch_products_for_shop(session, shop_url, semaphore, shop_domain):
    """
    Fetch all pages of products for a shop in batches of 1000 and process them.
    """
    products = []
    page = 1
    batch_size = 30 #30 pages per batch

    async def fetch_page(page):
        async with semaphore:
            return await fetch_products(session, shop_url, page)

    while True:
        page_products = await fetch_page(page)
        print(f"Page:{page} for shop: {shop_url} and there is this {len(page_products)}")

        if not page_products:
            break
        
        products.extend(page_products)
        print("----------------------------------------------------------------------------------")
        print(f"Page:{page} for shop: {shop_url} and there are {len(page_products)} products.")
        print(f"[DEBUG] Page products type: {type(page_products)} for shop: {shop_url}, content: {len(products)} products")
        print("----------------------------------------------------------------------------------")
        while len(products) >= batch_size:
            print("I am here")
            await process_the_products(products[:batch_size], shop_domain)
            products = products[batch_size:]

        page += 1

    if products:
        print(" in the IF statement ------------------I am here")
        await process_the_products(products, shop_url)  

    print(f"[INFO] Finished fetching products for {shop_url}")
    return products



def process_the_products_worker(product_queue, result_queue):
    """
    Worker to process products in a separate process.
    """
    while True:
        try:
            shop_domain, products = product_queue.get(timeout=5)
        except Empty:
            break

        try:
            # Ensure that asyncio is run within the worker process for the async function
            asyncio.run(process_the_products(products, shop_domain))
            result_queue.put((shop_domain, True))
        except Exception as e:
            print(f"Error processing shop {shop_domain}: {e}")
            result_queue.put((shop_domain, False))



async def main():
    """
    Main entry point for the scraper.
    """
    await enqueue_products()

if __name__ == "__main__":
    asyncio.run(main())
