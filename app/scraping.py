# # import asyncio
# # import httpx
# # from httpx import HTTPStatusError, RequestError
# # from datetime import datetime, timedelta
# # import re
# # import database
# # from bson import ObjectId





# # async def fetch_products(shop_url, page = 1, retries = 3):
# #     url = f"https://{shop_url}/products.json?page={page}"
# #     async with httpx.AsyncClient() as client:
# #         for attempt in range(retries):
# #             try:
# #                 headers = {
# #                     "Accept-Encoding": "gzip",
# #                     "User-Agent": "Mozilla/5.0", 
# #                 }
# #                 response = await client.get(url, headers=headers, timeout=30)
# #                 response.raise_for_status()
# #                 products = response.json().get('products', [])

# #                 return products
# #             except HTTPStatusError as e:
# #                 print(f"HTTP error occurred: {e.response.status_code} for {shop_url}")
# #                 await asyncio.sleep(2 ** attempt)
# #             except RequestError as e:
# #                 print(f"Request error occurred: {e}")
# #                 await asyncio.sleep(2 ** attempt)
# #             except Exception as e:
# #                 print(f"An unexpected error occurred: {e}")
# #                 await asyncio.sleep(2 ** attempt)
# #         return []







# # def process_the_products(products, shop_domain):
# #     """
# #     Process all of the products of one shop that is determined by the shop_domain
# #     """
# #     #find the _id of the shop in db
# #     shop_id = database.find_shop_id_by_domain(shop_domain)
# #     for i in range(len(products)):

# #         # Iterate over each variant in the product
# #         for variant in range(len(products[i]['variants'])):
# #             # Extract variant-specific information
# #             product_data = {
# #                 'shop_id': ObjectId(shop_id),
# #                 'title': products[i]['title'],
# #                 'title2': products[i]['variants'][variant]['title'],
# #                 'price': float(products[i]['variants'][variant]['price']),
# #                 'compare_at_price': float(products[i]['variants'][variant].get('compare_at_price')) if products[i]['variants'][variant].get('compare_at_price') else None,
# #                 'is_on_sale': False #will adjust down bellow
# #             }
# #             ###check if that product exists in the database already
# #             searchResultOfTheProduct = database.find_product(shop_id, title=products[i]['title'], title2=products[i]['variants'][variant]['title'])
            
# #             if(searchResultOfTheProduct):
# #                 #check if the new product price is different with the current product price if that is the case:
# #                 ## update the products collection and insert the previous product information in the price_history_collection
# #                 if(product_data['price'] == searchResultOfTheProduct['price']):
# #                     continue
# #                 else:
# #                     #check if the price_history_document for this one already exists if it exists then update the field if it doesnt exist create a new one

# #                     if(database.find_price_history_document(ObjectId(searchResultOfTheProduct["_id"]))):
# #                         database.update_price_history({"product_id":ObjectId(searchResultOfTheProduct["_id"])}, {
# #                             "price":searchResultOfTheProduct["price"],
# #                             "compare_at_price":searchResultOfTheProduct["compare_at_price"],
# #                             "timestamp":searchResultOfTheProduct['last_scanned']
# #                         })
# #                     else:
# #                         database.create_price_history_document({
# #                             "product_id": ObjectId(searchResultOfTheProduct["_id"]),
# #                             "price":searchResultOfTheProduct["price"],
# #                             "compare_at_price":searchResultOfTheProduct["compare_at_price"],
# #                             "timestamp":searchResultOfTheProduct['last_scanned']
# #                         })
                    
# #                     if (product_data['price'] < searchResultOfTheProduct['price']):
# #                         database.update_product_fields({
# #                             "_id":ObjectId(searchResultOfTheProduct['_id'])
# #                         },{
# #                             "price":product_data['price'],
# #                             "compare_at_price":searchResultOfTheProduct['price'],  #maybe we can put here the price of the one in the history collection
# #                             "is_on_sale":True
# #                         })
# #                     else:
# #                         database.update_product_fields({
# #                             "_id":ObjectId(searchResultOfTheProduct['_id'])
# #                         },{
# #                             "price":product_data['price'],
# #                             "compare_at_price":product_data['compare_at_price'],
# #                             "is_on_sale":False
# #                         })
# #             else:
# #                 #create new product and if the compare_at_price exists and the price difference is positive then set the is_on_sale to True either way set it on False 
# #                 product_data['is_on_sale'] = bool(product_data['compare_at_price']) and product_data['price'] < product_data['compare_at_price']
# #                 database.create_product(product_data)




# # def calculate_sale_percentage(shop_id):
# #     number_of_products_on_sale = database.number_of_products_on_sale(shop_id)
# #     print("Number of sale", number_of_products_on_sale)
# #     total_number_of_products = database.get_number_of_products(shop_id)

# #     if(total_number_of_products == 0):
# #         return 0

# #     percentage_of_products_on_sale = (number_of_products_on_sale / total_number_of_products) * 100
# #     return round(percentage_of_products_on_sale)
# # def calculate_scan_frequency(sale_percentage):
# #     if(sale_percentage >= 50):
# #         return '72 hours'
# #     elif(sale_percentage < 50 and sale_percentage > 25):
# #         return '7 days'
# #     else:
# #         return '30 days'
# # def calculate_next_scan_due_date(scan_frequency: str) -> datetime:
# #     """
# #     Calculates the next scan due date based on the scan frequency.

# #     Parameters:
# #     - scan_frequency (str): Frequency of scans, in the format "72 hours" or "7 days".

# #     Returns:
# #     - datetime: The calculated due date for the next scan.
# #     """
# #     # Parse the scan_frequency
# #     match = re.match(r'(\d+) (hours|days)', scan_frequency)
# #     if not match:
# #         raise ValueError("Invalid scan_frequency format. Expected format: '<number> hours' or '<number> days'.")

# #     # Extract the quantity and unit from the scan_frequency string
# #     quantity = int(match.group(1))
# #     unit = match.group(2)

# #     # Calculate the next scan date
# #     if unit == 'hours':
# #         next_scan_due_date = datetime.utcnow() + timedelta(hours=quantity)
# #     elif unit == 'days':
# #         next_scan_due_date = datetime.utcnow() + timedelta(days=quantity)

# #     return next_scan_due_date
# # def update_shop_sale_percentage(shop_domain):
# #     shop_id = database.find_shop_id_by_domain(shop_domain)
# #     sale_percentage = calculate_sale_percentage(shop_id) # products on sale
# #     scan_frequency = calculate_scan_frequency(sale_percentage)
# #     next_scan_due_date = calculate_next_scan_due_date(scan_frequency)
# #     database.update_shop_fields({"_id": ObjectId(shop_id)}, {
# #         "products_on_sale_percentage":sale_percentage,
# #         "scan_frequency": scan_frequency,
# #         "next_scan_due_date": next_scan_due_date,
# #     })






# # async def fetch_pages(url, shop_domain):
# #     """
# #     Fetch all of the pages of the /products.json  API of one specific shop specified by shop_domain
# #     Arguments:
# #     url - a specific url of the shop
# #     shop_domain - unique identifier of the company

# #     It gets all of the product page by page fetch_products() and then process each product in the process_the_products()
# #     """
# #     page = 1
# #     while True:

# #         products = await fetch_products(url, page)
# #         process_the_products(products, shop_domain)
# #         print(f"Processed {len(products)} products for shop {shop_domain}")
# #         if not products:
# #             break
# #         page += 1
# #     #when we have processed all of the products now we have to determine the sale_percentage
# #     #get the number of all of the products of that specific store and get the number of all of the products of that store that are on sale
# #     update_shop_sale_percentage(shop_domain)

# # # async def main():
# # #     await fetch_pages("shop.travelandleisure.com", "travelandleisure.com");
# # # async def main():
# # #     batch_size = 20  # Testing needed to determine optimal batch size
# # #     for shop_batch in database.get_shops_in_batches(batch_size):
# # #         for shop in shop_batch:
# # #             for shop_url in shop.get('shop_urls', []):
# # #                 try:
# # #                     # Attempt to fetch pages
# # #                     await fetch_pages(shop_url, shop["domain"])
# # #                 except Exception as e:
# # #                     # Log the error and continue with the next URL
# # #                     print(f"Failed to fetch pages for {shop_url} in shop {shop['domain']}: {e}")




# # # asyncio.run(main())

# # async def main():
# #     batch_size = 20  # Testing needed to determine optimal batch size
# #     semaphore = asyncio.Semaphore(10)  # Limit concurrent tasks if needed

# #     async def process_shop(shop):
# #         async with semaphore:
# #             tasks = [fetch_pages(shop_url, shop["domain"]) for shop_url in shop.get('shop_urls', [])]
# #             # Run all shop_url fetches concurrently
# #             await asyncio.gather(*tasks, return_exceptions=True)

# #     # Process each batch of shops concurrently
# #     for shop_batch in database.get_shops_in_batches(batch_size):
# #         tasks = [process_shop(shop) for shop in shop_batch]
# #         await asyncio.gather(*tasks, return_exceptions=True)

# # # # Run the main function asynchronously
# # asyncio.run(main())

# import asyncio
# import httpx
# from httpx import HTTPStatusError, RequestError
# from datetime import datetime, timedelta
# import database
# from bson import ObjectId


# async def fetch_products(shop_url, page=1, retries=3):
#     """
#     Fetches products from a shop's /products.json endpoint.
#     """
#     url = f"https://{shop_url}/products.json?page={page}"
#     async with httpx.AsyncClient() as client:
#         for attempt in range(retries):
#             try:
#                 headers = {
#                     "Accept-Encoding": "gzip",
#                     "User-Agent": "Mozilla/5.0",
#                 }
#                 response = await client.get(url, headers=headers, timeout=30)
#                 response.raise_for_status()
#                 products = response.json().get('products', [])
#                 return products
#             except HTTPStatusError as e:
#                 print(f"HTTP error occurred: {e.response.status_code} for {shop_url}")
#                 await asyncio.sleep(2 ** attempt)
#             except RequestError as e:
#                 print(f"Request error occurred: {e}")
#                 await asyncio.sleep(2 ** attempt)
#             except Exception as e:
#                 print(f"An unexpected error occurred: {e}")
#                 await asyncio.sleep(2 ** attempt)
#         return []


# async def process_the_products(products, shop_domain):
#     """
#     Process all of the products of one shop identified by its domain.
#     """
#     shop_id = await database.find_shop_id_by_domain(shop_domain)
#     if not shop_id:
#         print(f"Shop not found for domain {shop_domain}")
#         return

#     for product in products:
#         for variant in product['variants']:
#             product_data = {
#                 'shop_id': ObjectId(shop_id),
#                 'title': product['title'],
#                 'title2': variant['title'],
#                 'price': float(variant['price']),
#                 'compare_at_price': float(variant.get('compare_at_price')) if variant.get('compare_at_price') else None,
#                 'is_on_sale': False,  # will adjust below
#             }

#             search_result = await database.find_product(
#                 shop_id, title=product['title'], title2=variant['title']
#             )

#             if search_result:
#                 # If product exists, check for price differences and update accordingly
#                 if product_data['price'] != search_result['price']:
#                     price_history_record = await database.find_price_history_document(ObjectId(search_result["_id"]))
                    
#                     if price_history_record:
#                         await database.update_price_history(
#                             {"product_id": ObjectId(search_result["_id"])},
#                             {
#                                 "price": search_result["price"],
#                                 "compare_at_price": search_result["compare_at_price"],
#                                 "timestamp": search_result["last_scanned"],
#                             },
#                         )
#                     else:
#                         await database.create_price_history_document({
#                             "product_id": ObjectId(search_result["_id"]),
#                             "price": search_result["price"],
#                             "compare_at_price": search_result["compare_at_price"],
#                             "timestamp": search_result["last_scanned"],
#                         })
                    
#                     product_data['is_on_sale'] = product_data['price'] < search_result['price']
#                     await database.update_product_fields(
#                         {"_id": ObjectId(search_result["_id"])},
#                         {
#                             "price": product_data['price'],
#                             "compare_at_price": product_data['compare_at_price'],
#                             "is_on_sale": product_data['is_on_sale'],
#                         },
#                     )
#             else:
#                 # Create new product
#                 product_data['is_on_sale'] = bool(product_data['compare_at_price']) and product_data['price'] < product_data['compare_at_price']
#                 await database.create_product(product_data)


# async def calculate_sale_percentage(shop_id):
#     """
#     Calculates the percentage of products on sale for a shop.
#     """
#     number_of_products_on_sale = await database.number_of_products_on_sale(shop_id)
#     total_number_of_products = await database.get_number_of_products(shop_id)

#     if total_number_of_products == 0:
#         return 0

#     percentage_of_products_on_sale = (number_of_products_on_sale / total_number_of_products) * 100
#     return round(percentage_of_products_on_sale)


# async def update_shop_sale_percentage(shop_domain):
#     """
#     Updates the shop's sale percentage, scan frequency, and next scan due date.
#     """
#     shop_id = await database.find_shop_id_by_domain(shop_domain)
#     if not shop_id:
#         print(f"Shop not found for domain {shop_domain}")
#         return

#     sale_percentage = await calculate_sale_percentage(shop_id)
#     scan_frequency = (
#         '72 hours' if sale_percentage >= 50 else '7 days' if sale_percentage > 25 else '30 days'
#     )

#     next_scan_due_date = datetime.utcnow() + (
#         timedelta(hours=72) if scan_frequency == '72 hours' else
#         timedelta(days=7) if scan_frequency == '7 days' else
#         timedelta(days=30)
#     )

#     await database.update_shop_fields(
#         {"_id": ObjectId(shop_id)},
#         {
#             "products_on_sale_percentage": sale_percentage,
#             "scan_frequency": scan_frequency,
#             "next_scan_due_date": next_scan_due_date,
#         },
#     )


# async def fetch_pages(url, shop_domain):
#     """
#     Fetch all pages of products from the shop and process them.
#     """
#     page = 1
#     while True:
#         products = await fetch_products(url, page)
#         if not products:
#             break
#         await process_the_products(products, shop_domain)
#         print(f"Processed {len(products)} products for shop {shop_domain}")
#         page += 1

#     await update_shop_sale_percentage(shop_domain)


# async def process_shop(shop):
#     """
#     Processes a single shop by fetching pages for each shop URL.
#     """
#     async with asyncio.Sem

# import asyncio
# import httpx
# from httpx import HTTPStatusError, RequestError
# from datetime import datetime, timedelta
# import database
# from bson import ObjectId


# async def fetch_products(shop_url, page=1, retries=3):
#     """
#     Fetches products from a shop's /products.json endpoint.
#     """
#     url = f"https://{shop_url}/products.json?page={page}"
#     async with httpx.AsyncClient() as client:
#         for attempt in range(retries):
#             try:
#                 headers = {
#                     "Accept-Encoding": "gzip",
#                     "User-Agent": "Mozilla/5.0",
#                 }
#                 response = await client.get(url, headers=headers, timeout=30)
#                 response.raise_for_status()
#                 products = response.json().get('products', [])
#                 return products
#             except HTTPStatusError as e:
#                 print(f"HTTP error occurred: {e.response.status_code} for {shop_url}")
#                 await asyncio.sleep(2 ** attempt)
#             except RequestError as e:
#                 print(f"Request error occurred: {e}")
#                 await asyncio.sleep(2 ** attempt)
#             except Exception as e:
#                 print(f"An unexpected error occurred: {e}")
#                 await asyncio.sleep(2 ** attempt)
#         return []


# async def process_the_products(products, shop_domain):
#     """
#     Process all of the products of one shop identified by its domain.
#     """
#     shop_id = await database.find_shop_id_by_domain(shop_domain)
#     if not shop_id:
#         print(f"Shop not found for domain {shop_domain}")
#         return

#     for product in products:
#         for variant in product['variants']:
#             product_data = {
#                 'shop_id': ObjectId(shop_id),
#                 'title': product['title'],
#                 'title2': variant['title'],
#                 'price': float(variant['price']),
#                 'compare_at_price': float(variant.get('compare_at_price')) if variant.get('compare_at_price') else None,
#                 'is_on_sale': False,  # will adjust below
#             }

#             search_result = await database.find_product(
#                 shop_id, title=product['title'], title2=variant['title']
#             )

#             if search_result:
#                 # If product exists, check for price differences and update accordingly
#                 if product_data['price'] != search_result['price']:
#                     price_history_record = await database.find_price_history_document(ObjectId(search_result["_id"]))
                    
#                     if price_history_record:
#                         await database.update_price_history(
#                             {"product_id": ObjectId(search_result["_id"])},
#                             {
#                                 "price": search_result["price"],
#                                 "compare_at_price": search_result["compare_at_price"],
#                                 "timestamp": search_result["last_scanned"],
#                             },
#                         )
#                     else:
#                         await database.create_price_history_document({
#                             "product_id": ObjectId(search_result["_id"]),
#                             "price": search_result["price"],
#                             "compare_at_price": search_result["compare_at_price"],
#                             "timestamp": search_result["last_scanned"],
#                         })
                    
#                     product_data['is_on_sale'] = product_data['price'] < search_result['price']
#                     await database.update_product_fields(
#                         {"_id": ObjectId(search_result["_id"])},
#                         {
#                             "price": product_data['price'],
#                             "compare_at_price": product_data['compare_at_price'],
#                             "is_on_sale": product_data['is_on_sale'],
#                         },
#                     )
#             else:
#                 # Create new product
#                 product_data['is_on_sale'] = bool(product_data['compare_at_price']) and product_data['price'] < product_data['compare_at_price']
#                 await database.create_product(product_data)


# async def calculate_sale_percentage(shop_id):
#     """
#     Calculates the percentage of products on sale for a shop.
#     """
#     number_of_products_on_sale = await database.number_of_products_on_sale(shop_id)
#     total_number_of_products = await database.get_number_of_products(shop_id)

#     if total_number_of_products == 0:
#         return 0

#     percentage_of_products_on_sale = (number_of_products_on_sale / total_number_of_products) * 100
#     return round(percentage_of_products_on_sale)


# async def update_shop_sale_percentage(shop_domain):
#     """
#     Updates the shop's sale percentage, scan frequency, and next scan due date.
#     """
#     shop_id = await database.find_shop_id_by_domain(shop_domain)
#     if not shop_id:
#         print(f"Shop not found for domain {shop_domain}")
#         return

#     sale_percentage = await calculate_sale_percentage(shop_id)
#     scan_frequency = (
#         '72 hours' if sale_percentage >= 50 else '7 days' if sale_percentage > 25 else '30 days'
#     )

#     next_scan_due_date = datetime.utcnow() + (
#         timedelta(hours=72) if scan_frequency == '72 hours' else
#         timedelta(days=7) if scan_frequency == '7 days' else
#         timedelta(days=30)
#     )

#     await database.update_shop_fields(
#         {"_id": ObjectId(shop_id)},
#         {
#             "products_on_sale_percentage": sale_percentage,
#             "scan_frequency": scan_frequency,
#             "next_scan_due_date": next_scan_due_date,
#         },
#     )


# async def fetch_pages(url, shop_domain):
#     """
#     Fetch all pages of products from the shop and process them.
#     """
#     page = 1
#     while True:
#         products = await fetch_products(url, page)
#         if not products:
#             break
#         await process_the_products(products, shop_domain)
#         # print(f"Processed {len(products)} products for shop {shop_domain}")
#         page += 1

#     await update_shop_sale_percentage(shop_domain)


# async def process_shop(shop):
#     """
#     Processes a single shop by fetching pages for each shop URL.
#     """
#     async with asyncio.Semaphore(25):
#         try:
#             print(f"Processing shop: {shop['domain']}")
#             tasks = [fetch_pages(shop_url, shop["domain"]) for shop_url in shop.get('shop_urls', [])]
#             results = await asyncio.gather(*tasks, return_exceptions=True)
#             # for result in results:
#             #     if isinstance(result, Exception):
#             #         print(f"Error processing {shop['domain']}: {result}")
#             #     else:
#             #         print(f"Processed data for {shop['domain']}")

#             print(f"------------------------------------------------I have finished processing data for {shop['domain']}------------------------------------------------")
#         except Exception as e:
#             print(f"Unexpected error processing shop {shop['domain']}: {e}")


# async def main():
#     """
#     Entry point for the scraper.
#     Processes shops in batches and handles them concurrently.
#     """
#     batch_size = 10

#     async for shop_batch in database.get_shops_in_batches(batch_size):
#         if not shop_batch:
#             print("No shops to process in this batch.")
#             break
        
#         # if isinstance(shop_batch, str):
#         #     print(f"Shop data is a string: {shop_batch}")
#         # else:
#         #     print("Is not a sting something else")
#         print(f"Processing batch of {len(shop_batch)} shops")
#         tasks = [process_shop(shop) for shop in shop_batch]

#         # Execute all tasks in the current batch
#         results = await asyncio.gather(*tasks, return_exceptions=True)
#         print("--------------------------------------------Results are--------------------------------------------")
#         print(results)



# # Ensure to run the main function
# if __name__ == "__main__":
#     asyncio.run(main())



# import asyncio
# import httpx
# from httpx import HTTPStatusError, RequestError
# from datetime import datetime, timedelta
# import database
# from bson import ObjectId
# import asyncio
# import httpx
# from httpx import HTTPStatusError, RequestError
# from datetime import datetime, timedelta
# import database
# from bson import ObjectId


# async def fetch_products(shop_url, page, retries=3):
#     """
#     Fetches products from a shop's /products.json endpoint.
#     """
#     url = f"https://{shop_url}/products.json?page={page}"
#     async with httpx.AsyncClient() as client:
#         for attempt in range(retries):
#             try:
#                 headers = {
#                     "Accept-Encoding": "gzip",
#                     "User-Agent": "Mozilla/5.0",
#                 }
#                 response = await client.get(url, headers=headers, timeout=30)
#                 response.raise_for_status()
#                 return response.json().get('products', [])
#             except HTTPStatusError as e:
#                 print(f"HTTP error for {shop_url} on page {page}: {e.response.status_code}")
#                 await asyncio.sleep(2 ** attempt)
#             except RequestError as e:
#                 print(f"Request error for {shop_url} on page {page}: {e}")
#                 await asyncio.sleep(2 ** attempt)
#             except Exception as e:
#                 print(f"Unexpected error for {shop_url} on page {page}: {e}")
#                 await asyncio.sleep(2 ** attempt)
#         return []

# async def fetch_pages(shop_url, shop_domain, max_concurrent_pages=10):
#     """
#     Fetch all pages of products for a shop concurrently, stopping on empty responses.
#     """
#     semaphore = asyncio.Semaphore(max_concurrent_pages)
#     tasks = {}
#     page = 1
#     active_pages = set()

#     async def fetch_and_process_page(page):
#         async with semaphore:
#             try:
#                 print(f"Fetching page {page} for {shop_domain}")
#                 products = await fetch_products(shop_url, page)
#                 if products:
#                     await process_the_products(products, shop_domain)
#                 return products
#             except Exception as e:
#                 print(f"Error processing page {page} for {shop_domain}: {e}")
#                 return None

#     while True:
#         if page not in active_pages:  # Prevent duplicate processing
#             task = asyncio.create_task(fetch_and_process_page(page))
#             tasks[page] = task
#             active_pages.add(page)

#             # Await the task for the current page
#             result = await task
#             active_pages.remove(page)

#             # Stop if the page has no products
#             if not result:
#                 print(f"Stopping fetch at page {page} for {shop_domain} - no more products.")
#                 break

#             page += 1

#     # Wait for any remaining tasks to complete (though there shouldn't be any)
#     if tasks:
#         await asyncio.gather(*tasks.values(), return_exceptions=True)

#     # Update shop stats after all pages are processed
#     await database.update_shop_sale_percentage(shop_domain)
#     print(f"Finished processing shop: {shop_domain}")

# async def process_the_products(products, shop_domain):
#     """
#     Process all of the products of one shop identified by its domain.
#     """
#     shop_id = await database.find_shop_id_by_domain(shop_domain)
#     if not shop_id:
#         print(f"Shop not found for domain {shop_domain}")
#         return

#     for product in products:
#         for variant in product['variants']:
#             product_data = {
#                 'shop_id': ObjectId(shop_id),
#                 'title': product['title'],
#                 'title2': variant['title'],
#                 'price': float(variant['price']),
#                 'compare_at_price': float(variant.get('compare_at_price')) if variant.get('compare_at_price') else None,
#                 'is_on_sale': False,  # will adjust below
#             }

#             search_result = await database.find_product(
#                 shop_id, title=product['title'], title2=variant['title']
#             )

#             if search_result:
#                 # If product exists, check for price differences and update accordingly
#                 if product_data['price'] != search_result['price']:
#                     price_history_record = await database.find_price_history_document(ObjectId(search_result["_id"]))
                    
#                     if price_history_record:
#                         await database.update_price_history(
#                             {"product_id": ObjectId(search_result["_id"])},
#                             {
#                                 "price": search_result["price"],
#                                 "compare_at_price": search_result["compare_at_price"],
#                                 "timestamp": search_result["last_scanned"],
#                             },
#                         )
#                     else:
#                         await database.create_price_history_document({
#                             "product_id": ObjectId(search_result["_id"]),
#                             "price": search_result["price"],
#                             "compare_at_price": search_result["compare_at_price"],
#                             "timestamp": search_result["last_scanned"],
#                         })
                    
#                     product_data['is_on_sale'] = product_data['price'] < search_result['price']
#                     await database.update_product_fields(
#                         {"_id": ObjectId(search_result["_id"])},
#                         {
#                             "price": product_data['price'],
#                             "compare_at_price": product_data['compare_at_price'],
#                             "is_on_sale": product_data['is_on_sale'],
#                         },
#                     )
#             else:
#                 # Create new product
#                 product_data['is_on_sale'] = bool(product_data['compare_at_price']) and product_data['price'] < product_data['compare_at_price']
#                 await database.create_product(product_data)


# async def process_shop(shop):
#     """
#     Processes a single shop by fetching pages for each shop URL.
#     """
#     async with asyncio.Semaphore(10):
#         try:
#             print(f"Processing shop: {shop['domain']}")
#             tasks = [
#                 fetch_pages(shop_url, shop["domain"], max_concurrent_pages=10)
#                 for shop_url in shop.get('shop_urls', [])
#             ]
#             await asyncio.gather(*tasks, return_exceptions=True)
#             print(f"Finished processing shop: {shop['domain']}")
#         except Exception as e:
#             print(f"Unexpected error processing shop {shop['domain']}: {e}")


# async def main():
#     """
#     Entry point for the scraper.
#     Processes shops in batches and handles them concurrently.
#     """
#     batch_size = 10

#     async for shop_batch in database.get_shops_in_batches(batch_size):
#         if not shop_batch:
#             print("No shops to process in this batch.")
#             break

#         print(f"Processing batch of {len(shop_batch)} shops")
#         tasks = [process_shop(shop) for shop in shop_batch]

#        # Execute all tasks in the current batch
#         await asyncio.gather(*tasks, return_exceptions=True)



# if __name__ == "__main__":
#     asyncio.run(main())













































# async def process_the_products(products, shop_domain):
#     """
#     Process all of the products of one shop identified by its domain.
#     """
#     shop_id = await database.find_shop_id_by_domain(shop_domain)
#     if not shop_id:
#         print(f"Shop not found for domain {shop_domain}")
#         return

#     for product in products:
#         for variant in product["variants"]:
#             product_data = {
#                 "shop_id": ObjectId(shop_id),
#                 "title": product["title"],
#                 "title2": variant["title"],
#                 "price": float(variant["price"]),
#                 "compare_at_price": float(variant.get("compare_at_price"))
#                 if variant.get("compare_at_price")
#                 else None,
#                 "is_on_sale": False,  # will adjust below
#             }

#             search_result = await database.find_product(
#                 shop_id, title=product["title"], title2=variant["title"]
#             )

#             if search_result:
#                 # If product exists, check for price differences and update accordingly
#                 if product_data["price"] != search_result["price"]:
#                     price_history_record = await database.find_price_history_document(
#                         ObjectId(search_result["_id"])
#                     )

#                     if price_history_record:
#                         await database.update_price_history(
#                             {"product_id": ObjectId(search_result["_id"])},
#                             {
#                                 "price": search_result["price"],
#                                 "compare_at_price": search_result["compare_at_price"],
#                                 "timestamp": search_result["last_scanned"],
#                             },
#                         )
#                     else:
#                         await database.create_price_history_document(
#                             {
#                                 "product_id": ObjectId(search_result["_id"]),
#                                 "price": search_result["price"],
#                                 "compare_at_price": search_result["compare_at_price"],
#                                 "timestamp": search_result["last_scanned"],
#                             }
#                         )

#                     product_data["is_on_sale"] = product_data["price"] < search_result["price"]
#                     await database.update_product_fields(
#                         {"_id": ObjectId(search_result["_id"])},
#                         {
#                             "price": product_data["price"],
#                             "compare_at_price": product_data["compare_at_price"],
#                             "is_on_sale": product_data["is_on_sale"],
#                         },
#                     )
#             else:
#                 # Create new product
#                 product_data["is_on_sale"] = (
#                     bool(product_data["compare_at_price"])
#                     and product_data["price"] < product_data["compare_at_price"]
#                 )
#                 await database.create_product(product_data)

# async def process_the_products(products, shop_domain):
#     """
#     Process all of the products of one shop identified by its domain.
#     """
#     shop_id = await database.find_shop_id_by_domain(shop_domain)
#     if not shop_id:
#         print(f"Shop not found for domain {shop_domain}")
#         return

#     # Batches for database operations
#     new_products_batch = []
#     update_products_batch = []
#     price_history_batch_create = []
#     price_history_batch_update = []

#     for product in products:
#         for variant in product["variants"]:
#             product_data = {
#                 "shop_id": ObjectId(shop_id),
#                 "title": product["title"],
#                 "title2": variant["title"],
#                 "price": float(variant["price"]),
#                 "compare_at_price": float(variant.get("compare_at_price"))
#                 if variant.get("compare_at_price")
#                 else None,
#                 "is_on_sale": False,  # will adjust below
#             }

#             search_result = await database.find_product(
#                 shop_id, title=product["title"], title2=variant["title"]
#             )

#             if search_result:
#                 # If product exists, check for price differences and update accordingly
#                 if product_data["price"] != search_result["price"]:
#                     # Check if a price history document exists
#                     price_history_record = await database.find_price_history_document(
#                         ObjectId(search_result["_id"])
#                     )

#                     if price_history_record:
#                         price_history_batch_update.append({
#                             "identifier": {"product_id": ObjectId(search_result["_id"])},
#                             "data": {
#                                 "price": search_result["price"],
#                                 "compare_at_price": search_result["compare_at_price"],
#                                 "timestamp": search_result["last_scanned"],
#                             },
#                         })
#                     else:
#                         price_history_batch_create.append({
#                             "product_id": ObjectId(search_result["_id"]),
#                             "price": search_result["price"],
#                             "compare_at_price": search_result["compare_at_price"],
#                             "timestamp": search_result["last_scanned"],
#                         })

#                     product_data["is_on_sale"] = product_data["price"] < search_result["price"]
#                     update_products_batch.append({
#                         "identifier": {"_id": ObjectId(search_result["_id"])},
#                         "data": {
#                             "price": product_data["price"],
#                             "compare_at_price": product_data["compare_at_price"],
#                             "is_on_sale": product_data["is_on_sale"],
#                         },
#                     })
#             else:
#                 # Create new product
#                 product_data["is_on_sale"] = (
#                     bool(product_data["compare_at_price"])
#                     and product_data["price"] < product_data["compare_at_price"]
#                 )
#                 new_products_batch.append(product_data)

#     # Execute batch operations
#     if new_products_batch:
#         await database.batch_insert(database.get_products_collection(), new_products_batch, ProductModel)

#     if update_products_batch:
#         await database.batch_update(database.get_products_collection(), update_products_batch, UpdateProductModel)

#     if price_history_batch_create:
#         await database.batch_insert(database.get_price_history_collection(), price_history_batch_create, PriceHistoryModel)

#     if price_history_batch_update:
#         await database.batch_update(database.get_price_history_collection(), price_history_batch_update, UpdatePriceHistoryModel)


















































# import asyncio
# import httpx
# from httpx import HTTPStatusError, RequestError
# from datetime import datetime, timedelta
# from db_models import PriceHistoryModel, ProductModel, UpdatePriceHistoryModel, UpdateProductModel
# import database
# from bson import ObjectId


# async def fetch_products(shop_url, page, retries=3):
#     """
#     Fetches products from a shop's /products.json endpoint.
#     """
#     url = f"https://{shop_url}/products.json?page={page}"
#     async with httpx.AsyncClient() as client:
#         for attempt in range(retries):
#             try:
#                 headers = {
#                     "Accept-Encoding": "gzip",
#                     "User-Agent": "Mozilla/5.0",
#                 }
#                 response = await client.get(url, headers=headers, timeout=30)
#                 response.raise_for_status()
#                 return response.json().get("products", [])
#             except HTTPStatusError as e:
#                 print(f"HTTP error for {shop_url} on page {page}: {e.response.status_code}")
#                 await asyncio.sleep(2 ** attempt)
#             except RequestError as e:
#                 print(f"Request error for {shop_url} on page {page}: {e}")
#                 await asyncio.sleep(2 ** attempt)
#             except Exception as e:
#                 print(f"Unexpected error for {shop_url} on page {page}: {e}")
#                 await asyncio.sleep(2 ** attempt)
#         return []


# async def fetch_pages(shop_url, shop_domain, max_concurrent_pages=50):
#     """
#     Fetch all pages of products for a shop concurrently, stopping on empty responses.
#     """
#     semaphore = asyncio.Semaphore(max_concurrent_pages)
#     active_pages = set()
#     tasks = {}

#     async def fetch_and_process_page(page):
#         async with semaphore:
#             try:
#                 if page in active_pages:
#                     return
#                 active_pages.add(page)
#                 print(f"Fetching page {page} for {shop_domain}")
#                 products = await fetch_products(shop_url, page)
#                 if products:
#                     await process_the_products(products, shop_domain)
#                 return products
#             finally:
#                 active_pages.remove(page)

#     page = 1
#     while True:
#         if page not in active_pages:
#             tasks[page] = asyncio.create_task(fetch_and_process_page(page))
#             result = await tasks[page]
#             if not result:  # Stop if no products are found
#                 print(f"Stopping fetch at page {page} for {shop_domain} - no more products.")
#                 break
#             page += 1

#     # Wait for all remaining tasks to complete
#     await asyncio.gather(*tasks.values(), return_exceptions=True)

#     # Update shop stats after all pages are processed
#     await database.update_shop_sale_percentage(shop_domain)
#     print(f"Finished processing shop: {shop_domain}")





# async def process_the_products(products, shop_domain):
#     """
#     Process all of the products of one shop identified by its domain.
#     """
#     shop_id = await database.find_shop_id_by_domain(shop_domain)
#     if not shop_id:
#         print(f"Shop not found for domain {shop_domain}")
#         return

#     # Prepare a batch for product lookup
#     product_queries = [
#         {"shop_id": ObjectId(shop_id), "title": product["title"], "title2": variant["title"]}
#         for product in products
#         for variant in product["variants"]
#     ]

#     # Perform a batch lookup for existing products
#     existing_products = await database.find_products_in_batch(product_queries)
#     product_lookup = {
#         (str(product["shop_id"]), product["title"], product["title2"]): product
#         for product in existing_products
#     }

#     # Prepare batches for database operations
#     new_products_batch = []
#     update_products_batch = []
#     price_history_batch_create = []
#     price_history_batch_update = []

#     for product in products:
#         for variant in product["variants"]:
#             product_data = {
#                 "shop_id": ObjectId(shop_id),
#                 "title": product["title"],
#                 "title2": variant["title"],
#                 "price": float(variant["price"]),
#                 "compare_at_price": float(variant.get("compare_at_price"))
#                 if variant.get("compare_at_price")
#                 else None,
#                 "is_on_sale": False,  # will adjust below
#             }

#             # Key for lookup
#             lookup_key = (str(product_data["shop_id"]), product_data["title"], product_data["title2"])
#             search_result = product_lookup.get(lookup_key)

#             if search_result:
#                 # If product exists, check for price differences and prepare updates
#                 if product_data["price"] != search_result["price"]:
#                     price_history_record = await database.find_price_history_document(
#                         ObjectId(search_result["_id"])
#                     )

#                     if price_history_record:
#                         price_history_batch_update.append({
#                             "identifier": {"product_id": ObjectId(search_result["_id"])},
#                             "data": {
#                                 "price": search_result["price"],
#                                 "compare_at_price": search_result["compare_at_price"],
#                                 "timestamp": search_result["last_scanned"],
#                             },
#                         })
#                     else:
#                         price_history_batch_create.append({
#                             "product_id": ObjectId(search_result["_id"]),
#                             "price": search_result["price"],
#                             "compare_at_price": search_result["compare_at_price"],
#                             "timestamp": search_result["last_scanned"],
#                         })

#                     product_data["is_on_sale"] = product_data["price"] < search_result["price"]
#                     update_products_batch.append({
#                         "identifier": {"_id": ObjectId(search_result["_id"])},
#                         "data": {
#                             "price": product_data["price"],
#                             "compare_at_price": product_data["compare_at_price"],
#                             "is_on_sale": product_data["is_on_sale"],
#                         },
#                     })
#             else:
#                 # Create new product
#                 product_data["is_on_sale"] = (
#                     bool(product_data["compare_at_price"])
#                     and product_data["price"] < product_data["compare_at_price"]
#                 )
#                 new_products_batch.append(product_data)

#     # Execute batch operations
#     if new_products_batch:
#         await database.batch_insert(database.get_products_collection(), new_products_batch, ProductModel)

#     if update_products_batch:
#         await database.batch_update(database.get_products_collection(), update_products_batch, UpdateProductModel)

#     if price_history_batch_create:
#         await database.batch_insert(database.get_price_history_collection(), price_history_batch_create, PriceHistoryModel)

#     if price_history_batch_update:
#         await database.batch_update(database.get_price_history_collection(), price_history_batch_update, UpdatePriceHistoryModel)













# async def process_shop(shop):
#     """
#     Processes a single shop by fetching pages for each shop URL.
#     """
#     try:
#         print(f"Processing shop: {shop['domain']}")
#         tasks = [
#             fetch_pages(shop_url, shop["domain"], max_concurrent_pages=50)
#             for shop_url in shop.get("shop_urls", [])
#         ]
#         await asyncio.gather(*tasks, return_exceptions=True)
#         print(f"Finished processing shop: {shop['domain']}")
#     except Exception as e:
#         print(f"Unexpected error processing shop {shop['domain']}: {e}")


# async def producer(queue, batch_size):
#     """
#     Continuously fetch shop batches and add them to the queue.
#     """
#     async for shop_batch in database.get_shops_in_batches(batch_size):
#         for shop in shop_batch:
#             await queue.put(shop)  # Add each shop to the queue
#         print(f"Added {len(shop_batch)} shops to the queue.")
    
#     # Signal to consumers that no more items will be added
#     for _ in range(queue.maxsize):
#         await queue.put(None)

# async def consumer(queue):
#     """
#     Continuously process shops from the queue.
#     """
#     while True:
#         shop = await queue.get()
#         if shop is None:  # Stop if None is encountered
#             break

#         try:
#             await process_shop(shop)  # Process the shop
#         except Exception as e:
#             print(f"Error processing shop {shop['domain']}: {e}")
#         finally:
#             queue.task_done()

# async def main():
#     """
#     Main entry point for the scraper.
#     """
#     batch_size = 20
#     max_workers = 50  # Number of concurrent workers
#     queue = asyncio.Queue(maxsize=batch_size * 4)  # Queue size

#     # Create producer and consumer tasks
#     producer_task = asyncio.create_task(producer(queue, batch_size))
#     consumer_tasks = [asyncio.create_task(consumer(queue)) for _ in range(max_workers)]

#     # Wait for producer and consumers to finish
#     await producer_task
#     await queue.join()  # Ensure all items are processed

#     # Cancel remaining consumer tasks
#     for task in consumer_tasks:
#         task.cancel()
#     await asyncio.gather(*consumer_tasks, return_exceptions=True)
#     # await process_shop({"domain":"cato.org"})

# if __name__ == "__main__":
#     asyncio.run(main())





























































import asyncio
import httpx
from datetime import datetime
from db_models import PriceHistoryModel, ProductModel, UpdatePriceHistoryModel, UpdateProductModel
import database
from bson import ObjectId

MAX_RETRIES = 3
RETRY_BACKOFF = 2  # Exponential backoff base
MAX_CONCURRENT_PAGES = 50  # Limit concurrent requests per shop
BATCH_SIZE = 20  # Number of shops processed in one batch
MAX_WORKERS = 50  # Number of consumer workers


async def fetch_with_retries(url, retries=MAX_RETRIES):
    """
    Fetch a URL with retries and exponential backoff.
    """
    async with httpx.AsyncClient() as client:
        for attempt in range(retries):
            try:
                headers = {
                    "Accept-Encoding": "gzip",
                    "User-Agent": "Mozilla/5.0",
                }
                response = await client.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                return response.json()
            except httpx.HTTPStatusError as e:
                print(f"HTTP error: {e.response.status_code} for {url}. Retrying...")
            except httpx.RequestError as e:
                print(f"Request error: {e} for {url}. Retrying...")
            except Exception as e:
                print(f"Unexpected error: {e} for {url}. Retrying...")
            await asyncio.sleep(RETRY_BACKOFF ** attempt)
    return None


async def fetch_products(shop_url, page):
    """
    Fetch products from a shop's /products.json endpoint.
    """
    url = f"https://{shop_url}/products.json?page={page}"
    response = await fetch_with_retries(url)
    return response.get("products", []) if response else []


async def process_page(shop_url, shop_domain, page, semaphore):
    """
    Fetch and process a single page of products.
    """
    async with semaphore:
        print(f"Fetching page {page} for {shop_domain}")
        products = await fetch_products(shop_url, page)
        if products:
            await process_the_products(products, shop_domain)
        return bool(products)  # True if products found, False otherwise


async def fetch_pages(shop_url, shop_domain):
    """
    Fetch all pages of products for a shop concurrently.
    """
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_PAGES)
    page = 1
    while True:
        has_products = await process_page(shop_url, shop_domain, page, semaphore)
        if not has_products:
            print(f"No more products for {shop_domain} at page {page}.")
            break
        page += 1
    await database.update_shop_sale_percentage(shop_domain)
    print(f"Finished processing shop: {shop_domain}")


async def process_the_products(products, shop_domain):
    """
    Process all of the products of one shop identified by its domain.
    """
    shop_id = await database.find_shop_id_by_domain(shop_domain)
    if not shop_id:
        print(f"Shop not found for domain {shop_domain}")
        return

    # Prepare product queries for batch lookup
    product_queries = [
        {"shop_id": ObjectId(shop_id), "title": product["title"], "title2": variant["title"]}
        for product in products
        for variant in product["variants"]
    ]

    # Perform batch lookup
    existing_products = await database.find_products_in_batch(product_queries)
    product_lookup = {
        (str(product["shop_id"]), product["title"], product["title2"]): product
        for product in existing_products
    }

    # Prepare batches
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

    # Perform batch operations
    await asyncio.gather(
        database.batch_insert(database.get_products_collection(), new_products_batch, ProductModel),
        database.batch_update(database.get_products_collection(), update_products_batch, UpdateProductModel),
        database.batch_insert(database.get_price_history_collection(), price_history_batch_create, PriceHistoryModel),
        database.batch_update(database.get_price_history_collection(), price_history_batch_update, UpdatePriceHistoryModel),
    )


async def producer(queue):
    """
    Fetch shop batches and enqueue them.
    """
    async for shop_batch in database.get_shops_in_batches(BATCH_SIZE):
        for shop in shop_batch:
            await queue.put(shop)
        print(f"Enqueued {len(shop_batch)} shops.")
    for _ in range(MAX_WORKERS):
        await queue.put(None)  # Signal consumers to stop


async def consumer(queue):
    """
    Dequeue and process shops.
    """
    while True:
        shop = await queue.get()
        if shop is None:
            break
        await process_shop(shop)
        queue.task_done()


async def process_shop(shop):
    """
    Process a single shop.
    """
    print(f"Processing shop: {shop['domain']}")
    tasks = [fetch_pages(shop_url, shop["domain"]) for shop_url in shop.get("shop_urls", [])]
    await asyncio.gather(*tasks)
    print(f"Finished shop: {shop['domain']}")


async def main():
    """
    Main entry point.
    """
    queue = asyncio.Queue()
    producer_task = asyncio.create_task(producer(queue))
    consumer_tasks = [asyncio.create_task(consumer(queue)) for _ in range(MAX_WORKERS)]

    await producer_task
    await queue.join()

    for task in consumer_tasks:
        task.cancel()
    await asyncio.gather(*consumer_tasks, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())

























































# import asyncio
# import httpx
# from httpx import HTTPStatusError, RequestError
# from datetime import datetime
# from bson import ObjectId
# import database

# # Global HTTP Client for reuse
# http_client = httpx.AsyncClient(
#     headers={"User-Agent": "Mozilla/5.0"},
#     timeout=httpx.Timeout(15.0),
#     limits=httpx.Limits(max_connections=200, max_keepalive_connections=100)
# )


# async def fetch_products(shop_url, page, retries=3):
#     """
#     Fetches products from a shop's /products.json endpoint.
#     """
#     url = f"https://{shop_url}/products.json?page={page}"
#     for attempt in range(retries):
#         try:
#             response = await http_client.get(url)
#             response.raise_for_status()
#             return response.json().get("products", [])
#         except HTTPStatusError as e:
#             print(f"HTTP error for {shop_url} on page {page}: {e.response.status_code}")
#             await asyncio.sleep(2 ** attempt)
#         except RequestError as e:
#             print(f"Request error for {shop_url} on page {page}: {e}")
#             await asyncio.sleep(2 ** attempt)
#         except Exception as e:
#             print(f"Unexpected error for {shop_url} on page {page}: {e}")
#             await asyncio.sleep(2 ** attempt)
#     return []


# async def fetch_pages(shop_url, shop_domain, max_concurrent_pages=20):
#     """
#     Fetch multiple pages of products for a shop concurrently, stopping when no more data is available.
#     """
#     semaphore = asyncio.Semaphore(max_concurrent_pages)
#     #fetched_pages = set()  # Track fetched pages to prevent duplicates
#     tasks = {}

#     async def fetch_and_process_page(page):
#         async with semaphore:
#             # if page in fetched_pages:
#             #     return  # Skip if page already fetched
#             # fetched_pages.add(page)

#             print(f"Fetching page {page} for {shop_domain}")
#             products = await fetch_products(shop_url, page)
#             if products:
#                 await process_the_products(products, shop_domain)
#             return products

#     page = 1
#     while True:
#         #if page not in fetched_pages:
#         tasks[page] = asyncio.create_task(fetch_and_process_page(page))
#         result = await tasks[page]
#         if not result:  # Stop if no products are found
#             print(f"Stopping fetch at page {page} for {shop_domain} - no more products.")
#             break
#         page += 1

#     # Wait for all remaining tasks to complete
#     await asyncio.gather(*tasks.values(), return_exceptions=True)

#     # Update shop stats after processing
#     await database.update_shop_sale_percentage(shop_domain)
#     print(f"Finished processing shop: {shop_domain}")


# async def process_the_products(products, shop_domain):
#     """
#     Process all of the products of one shop identified by its domain.
#     """
#     shop_id = await database.find_shop_id_by_domain(shop_domain)
#     if not shop_id:
#         print(f"Shop not found for domain {shop_domain}")
#         return

#     batch_to_insert = []
#     for product in products:
#         for variant in product["variants"]:
#             product_data = {
#                 "shop_id": ObjectId(shop_id),
#                 "title": product["title"],
#                 "title2": variant["title"],
#                 "price": float(variant["price"]),
#                 "compare_at_price": float(variant.get("compare_at_price")) if variant.get("compare_at_price") else None,
#                 "is_on_sale": False,
#             }

#             search_result = await database.find_product(
#                 shop_id, title=product["title"], title2=variant["title"]
#             )

#             if search_result:
#                 if product_data["price"] != search_result["price"]:
#                     await database.update_price_history_or_create(
#                         ObjectId(search_result["_id"]), search_result
#                     )
#                     product_data["is_on_sale"] = product_data["price"] < search_result["price"]
#                     print("price history is created")
#                     await database.update_product_fields(
#                         {"_id": ObjectId(search_result["_id"])}, product_data
#                     )
#             else:
#                 product_data["is_on_sale"] = (
#                     product_data["compare_at_price"] and product_data["price"] < product_data["compare_at_price"]
#                 )
#                 batch_to_insert.append(product_data)

#     if batch_to_insert:
#         await database.insert_many_products(batch_to_insert)


# async def process_shop(shop):
#     """
#     Processes a single shop by fetching pages for each shop URL.
#     """
#     try:
#         print(f"Processing shop: {shop['domain']}")
#         tasks = [
#             fetch_pages(shop_url, shop["domain"], max_concurrent_pages=20)
#             for shop_url in shop.get("shop_urls", [])
#         ]
#         await asyncio.gather(*tasks, return_exceptions=True)
#         print(f"Finished processing shop: {shop['domain']}")
#     except Exception as e:
#         print(f"Unexpected error processing shop {shop['domain']}: {e}")


# async def producer(queue, batch_size):
#     """
#     Continuously fetch shop batches and add them to the queue.
#     """
#     async for shop_batch in database.get_shops_in_batches(batch_size):
#         for shop in shop_batch:
#             await queue.put(shop)
#         print(f"Added {len(shop_batch)} shops to the queue.")

#     for _ in range(queue.maxsize):
#         await queue.put(None)  # Signal consumers to stop


# async def consumer(queue):
#     """
#     Continuously process shops from the queue.
#     """
#     while True:
#         shop = await queue.get()
#         if shop is None:
#             break

#         try:
#             await process_shop(shop)
#         except Exception as e:
#             print(f"Error processing shop {shop['domain']}: {e}")
#         finally:
#             queue.task_done()


# async def main():
#     """
#     Main entry point for the scraper.
#     """
#     batch_size = 10
#     max_workers = 50
#     queue = asyncio.Queue(maxsize=batch_size * 4)

#     producer_task = asyncio.create_task(producer(queue, batch_size))
#     consumer_tasks = [asyncio.create_task(consumer(queue)) for _ in range(max_workers)]

#     await producer_task
#     await queue.join()

#     for task in consumer_tasks:
#         task.cancel()
#     await asyncio.gather(*consumer_tasks, return_exceptions=True)

#     await http_client.aclose()


# if __name__ == "__main__":
#     asyncio.run(main())























































# import asyncio
# import httpx
# from httpx import HTTPStatusError, RequestError
# from bson import ObjectId
# import database


# # Reusable HTTP Client
# http_client = httpx.AsyncClient(
#     headers={"User-Agent": "Mozilla/5.0"},
#     timeout=httpx.Timeout(15.0),
#     limits=httpx.Limits(max_connections=200, max_keepalive_connections=100)
# )


# async def fetch_products(shop_url, page, retries=3):
#     """
#     Fetches products from a shop's /products.json endpoint.
#     """
#     url = f"https://{shop_url}/products.json?page={page}"
#     for attempt in range(retries):
#         try:
#             response = await http_client.get(url)
#             response.raise_for_status()
#             return response.json().get("products", [])
#         except HTTPStatusError as e:
#             print(f"HTTP error for {shop_url} on page {page}: {e.response.status_code}")
#             await asyncio.sleep(2 ** attempt)
#         except RequestError as e:
#             print(f"Request error for {shop_url} on page {page}: {e}")
#             await asyncio.sleep(2 ** attempt)
#         except Exception as e:
#             print(f"Unexpected error for {shop_url} on page {page}: {e}")
#             await asyncio.sleep(2 ** attempt)
#     return []


# async def fetch_pages(shop_url, shop_domain, max_concurrent_pages=50):
#     """
#     Fetch all pages of products for a shop concurrently, stopping on empty responses.
#     """
#     semaphore = asyncio.Semaphore(max_concurrent_pages)
#     active_pages = set()
#     tasks = {}

#     async def fetch_and_process_page(page):
#         async with semaphore:
#             try:
#                 if page in active_pages:
#                     return
#                 active_pages.add(page)
#                 print(f"Fetching page {page} for {shop_domain}")
#                 products = await fetch_products(shop_url, page)
#                 if products:
#                     await process_the_products(products, shop_domain)
#                 return products
#             finally:
#                 active_pages.remove(page)

#     page = 1
#     while True:
#         if page not in active_pages:
#             tasks[page] = asyncio.create_task(fetch_and_process_page(page))
#             result = await tasks[page]
#             if not result:  # Stop if no products are found
#                 print(f"Stopping fetch at page {page} for {shop_domain} - no more products.")
#                 break
#             page += 1

#     # Wait for all remaining tasks to complete
#     await asyncio.gather(*tasks.values(), return_exceptions=True)

#     # Update shop stats after all pages are processed
#     await database.update_shop_sale_percentage(shop_domain)
#     print(f"Finished processing shop: {shop_domain}")


# async def process_the_products(products, shop_domain, batch_size=1000):
#     """
#     Process all of the products of one shop identified by its domain.
#     Perform batch insertions to optimize speed and reduce database requests.
#     """
#     shop_id = await database.find_shop_id_by_domain(shop_domain)
#     if not shop_id:
#         print(f"Shop not found for domain {shop_domain}")
#         return

#     batch_to_insert = []  # Temporary storage for products to insert
#     for product in products:
#         for variant in product["variants"]:
#             product_data = {
#                 "shop_id": ObjectId(shop_id),
#                 "title": product["title"],
#                 "title2": variant["title"],
#                 "price": float(variant["price"]),
#                 "compare_at_price": float(variant.get("compare_at_price"))
#                 if variant.get("compare_at_price")
#                 else None,
#                 "is_on_sale": False,  # Will adjust below
#             }

#             search_result = await database.find_product(
#                 shop_id, title=product["title"], title2=variant["title"]
#             )

#             if search_result:
#                 # If product exists, check for price differences and update accordingly
#                 if product_data["price"] != search_result["price"]:
#                     price_history_record = await database.find_price_history_document(
#                         ObjectId(search_result["_id"])
#                     )

#                     if price_history_record:
#                         await database.update_price_history(
#                             {"product_id": ObjectId(search_result["_id"])},
#                             {
#                                 "price": search_result["price"],
#                                 "compare_at_price": search_result["compare_at_price"],
#                                 "timestamp": search_result["last_scanned"],
#                             },
#                         )
#                     else:
#                         await database.create_price_history_document(
#                             {
#                                 "product_id": ObjectId(search_result["_id"]),
#                                 "price": search_result["price"],
#                                 "compare_at_price": search_result["compare_at_price"],
#                                 "timestamp": search_result["last_scanned"],
#                             }
#                         )

#                     product_data["is_on_sale"] = product_data["price"] < search_result["price"]
#                     await database.update_product_fields(
#                         {"_id": ObjectId(search_result["_id"])},
#                         {
#                             "price": product_data["price"],
#                             "compare_at_price": product_data["compare_at_price"],
#                             "is_on_sale": product_data["is_on_sale"],
#                         },
#                     )
#             else:
#                 # Prepare product data for batch insertion
#                 product_data["is_on_sale"] = (
#                     bool(product_data["compare_at_price"])
#                     and product_data["price"] < product_data["compare_at_price"]
#                 )
#                 batch_to_insert.append(product_data)

#             # If the batch size is reached, insert the products and clear the batch
#             if len(batch_to_insert) >= batch_size:
#                 await database.insert_many_products(batch_to_insert)
#                 batch_to_insert.clear()

#     # Insert any remaining products in the batch
#     if batch_to_insert:
#         await database.insert_many_products(batch_to_insert)



# async def process_shop(shop):
#     """
#     Processes a single shop by fetching pages for each shop URL.
#     """
#     try:
#         print(f"Processing shop: {shop['domain']}")
#         tasks = [
#             fetch_pages(shop_url, shop["domain"], max_concurrent_pages=30)
#             for shop_url in shop.get("shop_urls", [])
#         ]
#         await asyncio.gather(*tasks, return_exceptions=True)
#         print(f"Finished processing shop: {shop['domain']}")
#     except Exception as e:
#         print(f"Unexpected error processing shop {shop['domain']}: {e}")


# async def producer(queue, batch_size):
#     """
#     Continuously fetch shop batches and add them to the queue.
#     """
#     async for shop_batch in database.get_shops_in_batches(batch_size):
#         for shop in shop_batch:
#             await queue.put(shop)
#         print(f"Added {len(shop_batch)} shops to the queue.")

#     # Signal to consumers that no more items will be added
#     for _ in range(queue.maxsize):
#         await queue.put(None)


# async def consumer(queue):
#     """
#     Continuously process shops from the queue.
#     """
#     while True:
#         shop = await queue.get()
#         if shop is None:
#             break

#         try:
#             await process_shop(shop)
#         except Exception as e:
#             print(f"Error processing shop {shop['domain']}: {e}")
#         finally:
#             queue.task_done()


# async def main():
#     """
#     Main entry point for the scraper.
#     """
#     # batch_size = 20
#     # max_workers = 40
#     # queue = asyncio.Queue(maxsize=batch_size * 4)

#     # # Create producer and consumer tasks
#     # producer_task = asyncio.create_task(producer(queue, batch_size))
#     # consumer_tasks = [asyncio.create_task(consumer(queue)) for _ in range(max_workers)]

#     # Wait for producer and consumers to finish
#     # await producer_task
#     # await queue.join()

#     # # Cancel remaining consumer tasks
#     # for task in consumer_tasks:
#     #     task.cancel()
#     # await asyncio.gather(*consumer_tasks, return_exceptions=True)

#     # # Close the reusable HTTP client
#     # await http_client.aclose()
#     await process_shop({'domain':"cato.org"})


# if __name__ == "__main__":
#     asyncio.run(main())

