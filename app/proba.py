import asyncio
import json
from multiprocessing import Process, Queue, cpu_count
from queue import Empty
import httpx
from datetime import datetime
from db_models import PriceHistoryModel, ProductModel, UpdatePriceHistoryModel, UpdateProductModel
import database
from bson import ObjectId
import charset_normalizer


MAX_RETRIES = 3
RETRY_BACKOFF = 2
MAX_CONCURRENT_PAGES = 20
BATCH_SIZE = 10
MAX_WORKERS = 4

\



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
                logger.info(f"[INFO] No updates for {url} (ETag: {etag})")
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
                "price": float(variant["price"]),
                "compare_at_price": float(variant.get("compare_at_price")) if variant.get("compare_at_price") else None,
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
                            "data": {
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
        tasks.append(database.batch_insert(database.get_price_history_collection(), price_history_batch_create, PriceHistoryModel))
    
    if price_history_batch_update:
        tasks.append(database.batch_update(database.get_price_history_collection(), price_history_batch_update, UpdatePriceHistoryModel))

    # Perform batch operations

    if tasks:
        await asyncio.gather(*tasks)



async def enqueue_products():
    """
    Fetch shops from the database and process them dynamically using a queue.
    """
    queue = asyncio.Queue()
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)

    # Producer: Fetch shops from the database and add them to the queue
    async def producer():
        async for shop_batch in database.get_shops_in_batches(BATCH_SIZE):
            for shop in shop_batch:
                shop_domain = shop["domain"]
                for shop_url in shop.get("shop_urls", []):
                    await queue.put((shop_url, shop_domain))

        # Add termination signals for workers
        for _ in range(MAX_CONCURRENT_PAGES):
            await queue.put(None)

    # Consumer: Process shops from the queue
    async def consumer():
        async with httpx.AsyncClient() as session:
            while True:
                shop_data = await queue.get()
                if shop_data is None:  # Termination signal
                    break

                shop_url, shop_domain = shop_data
                await fetch_products_for_shop(session, shop_url, semaphore, shop_domain)
                queue.task_done()

    # Start producer and consumers
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
