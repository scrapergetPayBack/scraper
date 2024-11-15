import asyncio
import httpx
from httpx import HTTPStatusError, RequestError
from datetime import datetime, timedelta
import re
import database
from bson import ObjectId





async def fetch_products(shop_url, page = 1, retries = 3):
    url = f"https://{shop_url}/products.json?page={page}"
    async with httpx.AsyncClient() as client:
        for attempt in range(retries):
            try:
                headers = {
                    "Accept-Encoding": "gzip",
                    "User-Agent": "Mozilla/5.0", 
                }
                response = await client.get(url, headers=headers, timeout=30)
                response.raise_for_status()
                products = response.json().get('products', [])

                return products
            except HTTPStatusError as e:
                print(f"HTTP error occurred: {e.response.status_code} for {shop_url}")
                await asyncio.sleep(2 ** attempt)
            except RequestError as e:
                print(f"Request error occurred: {e}")
                await asyncio.sleep(2 ** attempt)
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                await asyncio.sleep(2 ** attempt)
        return []







def process_the_products(products, shop_domain):
    """
    Process all of the products of one shop that is determined by the shop_domain
    """
    #find the _id of the shop in db
    shop_id = database.find_shop_id_by_domain(shop_domain)
    for i in range(len(products)):

        # Iterate over each variant in the product
        for variant in range(len(products[i]['variants'])):
            # Extract variant-specific information
            product_data = {
                'shop_id': ObjectId(shop_id),
                'title': products[i]['title'],
                'title2': products[i]['variants'][variant]['title'],
                'price': float(products[i]['variants'][variant]['price']),
                'compare_at_price': float(products[i]['variants'][variant].get('compare_at_price')) if products[i]['variants'][variant].get('compare_at_price') else None,
                'is_on_sale': False #will adjust down bellow
            }
            ###check if that product exists in the database already
            searchResultOfTheProduct = database.find_product(shop_id, title=products[i]['title'], title2=products[i]['variants'][variant]['title'])
            
            if(searchResultOfTheProduct):
                #check if the new product price is different with the current product price if that is the case:
                ## update the products collection and insert the previous product information in the price_history_collection
                if(product_data['price'] == searchResultOfTheProduct['price']):
                    continue
                else:
                    #check if the price_history_document for this one already exists if it exists then update the field if it doesnt exist create a new one

                    if(database.find_price_history_document(ObjectId(searchResultOfTheProduct["_id"]))):
                        database.update_price_history({"product_id":ObjectId(searchResultOfTheProduct["_id"])}, {
                            "price":searchResultOfTheProduct["price"],
                            "compare_at_price":searchResultOfTheProduct["compare_at_price"],
                            "timestamp":searchResultOfTheProduct['last_scanned']
                        })
                    else:
                        database.create_price_history_document({
                            "product_id": ObjectId(searchResultOfTheProduct["_id"]),
                            "price":searchResultOfTheProduct["price"],
                            "compare_at_price":searchResultOfTheProduct["compare_at_price"],
                            "timestamp":searchResultOfTheProduct['last_scanned']
                        })
                    
                    if (product_data['price'] < searchResultOfTheProduct['price']):
                        database.update_product_fields({
                            "_id":ObjectId(searchResultOfTheProduct['_id'])
                        },{
                            "price":product_data['price'],
                            "compare_at_price":searchResultOfTheProduct['price'],  #maybe we can put here the price of the one in the history collection
                            "is_on_sale":True
                        })
                    else:
                        database.update_product_fields({
                            "_id":ObjectId(searchResultOfTheProduct['_id'])
                        },{
                            "price":product_data['price'],
                            "compare_at_price":product_data['compare_at_price'],
                            "is_on_sale":False
                        })
            else:
                #create new product and if the compare_at_price exists and the price difference is positive then set the is_on_sale to True either way set it on False 
                product_data['is_on_sale'] = bool(product_data['compare_at_price']) and product_data['price'] < product_data['compare_at_price']
                database.create_product(product_data)




def calculate_sale_percentage(shop_id):
    number_of_products_on_sale = database.number_of_products_on_sale(shop_id)
    print("Number of sale", number_of_products_on_sale)
    total_number_of_products = database.get_number_of_products(shop_id)

    if(total_number_of_products == 0):
        return 0

    percentage_of_products_on_sale = (number_of_products_on_sale / total_number_of_products) * 100
    return round(percentage_of_products_on_sale)
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
def update_shop_sale_percentage(shop_domain):
    shop_id = database.find_shop_id_by_domain(shop_domain)
    sale_percentage = calculate_sale_percentage(shop_id) # products on sale
    scan_frequency = calculate_scan_frequency(sale_percentage)
    next_scan_due_date = calculate_next_scan_due_date(scan_frequency)
    database.update_shop_fields({"_id": ObjectId(shop_id)}, {
        "products_on_sale_percentage":sale_percentage,
        "scan_frequency": scan_frequency,
        "next_scan_due_date": next_scan_due_date,
    })






async def fetch_pages(url, shop_domain):
    """
    Fetch all of the pages of the /products.json  API of one specific shop specified by shop_domain
    Arguments:
    url - a specific url of the shop
    shop_domain - unique identifier of the company

    It gets all of the product page by page fetch_products() and then process each product in the process_the_products()
    """
    page = 1
    while True:

        products = await fetch_products(url, page)
        process_the_products(products, shop_domain)
        print(f"Processed {len(products)} products for shop {shop_domain}")
        if not products:
            break
        page += 1
    #when we have processed all of the products now we have to determine the sale_percentage
    #get the number of all of the products of that specific store and get the number of all of the products of that store that are on sale
    update_shop_sale_percentage(shop_domain)

# async def main():
#     await fetch_pages("shop.travelandleisure.com", "travelandleisure.com");
# async def main():
#     batch_size = 20  # Testing needed to determine optimal batch size
#     for shop_batch in database.get_shops_in_batches(batch_size):
#         for shop in shop_batch:
#             for shop_url in shop.get('shop_urls', []):
#                 try:
#                     # Attempt to fetch pages
#                     await fetch_pages(shop_url, shop["domain"])
#                 except Exception as e:
#                     # Log the error and continue with the next URL
#                     print(f"Failed to fetch pages for {shop_url} in shop {shop['domain']}: {e}")




# asyncio.run(main())

async def main():
    batch_size = 20  # Testing needed to determine optimal batch size
    semaphore = asyncio.Semaphore(10)  # Limit concurrent tasks if needed

    async def process_shop(shop):
        async with semaphore:
            tasks = [fetch_pages(shop_url, shop["domain"]) for shop_url in shop.get('shop_urls', [])]
            # Run all shop_url fetches concurrently
            await asyncio.gather(*tasks, return_exceptions=True)

    # Process each batch of shops concurrently
    for shop_batch in database.get_shops_in_batches(batch_size):
        tasks = [process_shop(shop) for shop in shop_batch]
        await asyncio.gather(*tasks, return_exceptions=True)

# # Run the main function asynchronously
asyncio.run(main())

