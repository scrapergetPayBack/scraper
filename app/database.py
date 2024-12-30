
from typing import Dict, List
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from db_models import HistoryEntryModel, ShopModel, UpdatePriceHistoryModel, UpdateProductModel
from db_models import ShopUpdateModel, ProductModel, PriceHistoryModel
from bson import ObjectId
from datetime import datetime
from pydantic import ValidationError



# uri = "mongodb://admin:your-secure-password@172.235.51.17:27017"
uri = "mongodb+srv://username:password@172.235.51.17:27017"
client = AsyncIOMotorClient(uri)




def get_database():
    """
    Returns the database object. This is synchronous since accessing
    the database or collections is not an async operation.
    """
    return client['shopify_stores']

def get_shop_collection():
    """
    Returns the shop collection.
    """
    return get_database()['shop']

def get_products_collection():
    """
    Returns the products collection.
    """
    return get_database()['products']

def get_price_history_collection():
    """
    Returns the price history collection.
    """
    return get_database()['price_history']


def get_etags_collection():
    db = client["shopify_stores"]
    return db["etags"]



# ----------------------Create documents functions----------------------
async def create_shop(shop_data: ShopModel):
    try:
        shop = ShopModel(**shop_data)
        shop_collection = get_shop_collection()
        result = await shop_collection.insert_one(shop.dict(exclude_unset=False))
        print("Shop created with ID:", result.inserted_id)
    except ValidationError as ve:
        print("Validation error:", ve)
    except Exception as e:
        print("Error creating shop:", e)

async def create_product(product_data: ProductModel):
    try:
        validated_product = ProductModel(**product_data)
        product_document = validated_product.dict()
        collection = get_products_collection()
        result = await collection.insert_one(product_document)
        #print("Product created with ID:", result.inserted_id)
    except Exception as e:
        print("Error creating product:", e)
async def insert_many_products(products):
    """
    Inserts multiple products into the database in one operation.
    Validates each product using the ProductModel schema before insertion.

    Args:
        products (list): A list of product dictionaries to insert.
    """
    try:
        collection = get_products_collection()

        # Validate all products using ProductModel
        validated_products = []
        for product in products:
            try:
                validated_product = ProductModel(**product)
                validated_products.append(validated_product.dict(exclude_unset=True))
            except ValidationError as ve:
                print(f"Validation error for product: {product['title']} - {ve}")

        if validated_products:
            # Perform batch insertion
            result = await collection.insert_many(validated_products, ordered=False)
            print(f"Successfully inserted {len(result.inserted_ids)} products in batch.")
        else:
            print("No valid products to insert.")

    except Exception as e:
        print(f"Error inserting products in batch: {e}")



async def create_price_history_document(price_history_data: PriceHistoryModel):
    """Insert a single price history document."""
    try:
        validated_data = PriceHistoryModel(**price_history_data)
        collection = get_price_history_collection()
        result = await collection.insert_one(validated_data.dict())
        return result.inserted_id
    except ValidationError as ve:
        print("Validation error:", ve)
    except Exception as e:
        print("Error creating price history document:", e)

# ----------------------Update documents functions----------------------
async def update_document(collection, identifier, update_data, model):
    """Generic function to update a document."""
    try:
        validated_data = model(**update_data)
        sanitized_updates = validated_data.dict(exclude_unset=True)
        result = await collection.update_one(identifier, {"$set": sanitized_updates})
        return result
    except ValidationError as ve:
        print("Validation error:", ve)
    except Exception as e:
        print("Error updating document:", e)

async def update_shop_fields(identifier, update_data: ShopUpdateModel):
    """Update fields in the shop collection."""
    return await update_document(get_shop_collection(), identifier, update_data, ShopUpdateModel)



async def update_product_fields(identifier, update_data: UpdateProductModel):
    """Update fields in the products collection."""
    return await update_document(get_products_collection(), identifier, update_data, UpdateProductModel)

async def update_price_history(identifier, update_data: UpdatePriceHistoryModel):
    """Update fields in the price history collection."""
    return await update_document(get_price_history_collection(), identifier, update_data, UpdatePriceHistoryModel)

async def batch_insert(collection, data_list, model):
    """Batch insert documents into a collection."""
    try:
        validated_data = [model(**data).dict() for data in data_list]
        result = await collection.insert_many(validated_data)
        return result.inserted_ids
    except ValidationError as ve:
        print("Validation error:", ve)
    except Exception as e:
        print("Error during batch insertion:", e)

async def batch_insert_price_history(collection, data_list, price_history_model):
    """Batch insert for price history collection, transforms data into the new structure."""
    try:
        transformed_data = []
        for data in data_list:
            transformed_data.append(price_history_model(
                product_id=data["product_id"],
                history=[{
                    "price": data["price"],
                    "compare_at_price": data.get("compare_at_price"),
                    "timestamp": data["timestamp"]
                }]
            ).dict())

        result = await collection.insert_many(transformed_data)
        return result.inserted_ids
    except ValidationError as ve:
        print("Validation error:", ve)
    except Exception as e:
        print("Error during batch insertion:", e)




async def batch_update(collection, updates, model):
    """Batch update documents using a list of updates."""
    try:
        bulk_operations = [
            UpdateOne(update['identifier'], {"$set": model(**update['data']).dict(exclude_unset=True)})
            for update in updates
        ]
        result = await collection.bulk_write(bulk_operations)
        return result
    except ValidationError as ve:
        print("Validation error:", ve)
    except Exception as e:
        print("Error during batch update:", e)


async def batch_update_price_history(collection, updates: List[Dict]):
    """Batch update documents by pushing a new history entry into the history array."""
    bulk_operations = []
    try:
        for update in updates:
            # Expect 'history_entry' data in each update
            entry = HistoryEntryModel(**update['history_entry'])
            bulk_operations.append(
                UpdateOne(
                    update['identifier'],
                    {"$push": {"history": entry.dict(exclude_unset=True)}}
                )
            )

        if bulk_operations:
            result = await collection.bulk_write(bulk_operations)
            return result
        else:
            return None

    except ValidationError as ve:
        print("Validation error:", ve)
    except Exception as e:
        print("Error during batch update:", e)

        
# ----------------------Delete documents functions----------------------
async def delete_shop(identifier):
    try:
        shop_collection = get_shop_collection()
        shop_result = await shop_collection.delete_one(identifier)
        if shop_result.deleted_count:
            print(f"Deleted shop with identifier: {identifier}")
            shop_id = identifier.get("_id")
            if shop_id:
                product_collection = get_products_collection()
                await product_collection.delete_many({"shop_id": shop_id})
        else:
            print("No matching shop document found to delete.")
    except Exception as e:
        print(f"Error deleting shop: {e}")

async def delete_product(identifier):
    try:
        product_collection = get_products_collection()
        product = await product_collection.find_one(identifier)
        if product:
            product_id = product["_id"]
            await product_collection.delete_one(identifier)
            price_history_collection = get_price_history_collection()
            await price_history_collection.delete_many({"product_id": product_id})
            print(f"Deleted product and associated price history for ID: {product_id}")
        else:
            print("No matching product found.")
    except Exception as e:
        print(f"Error deleting product: {e}")

async def delete_price_history(identifier):
    try:
        price_history_collection = get_price_history_collection()
        result = await price_history_collection.delete_one(identifier)
        if result.deleted_count:
            print(f"Deleted price history with identifier: {identifier}")
        else:
            print("No matching document found to delete.")
    except Exception as e:
        print(f"Error deleting price history: {e}")

# ----------------------Find documents functions----------------------
async def find_shop_id_by_domain(shop_domain):
    try:
        collection = get_shop_collection()
        shop = await collection.find_one({"domain": shop_domain}, {"_id": 1})
        return shop["_id"] if shop else None
    except Exception as e:
        print(f"Error finding shop by domain: {e}")
        return None

async def find_shop_by_domain(shop_domain):
    try:
        collection = get_shop_collection()
        shop = await collection.find_one({"domain": shop_domain}, {"_id": 0})
        return shop if shop else None
    except Exception as e:
        print(f"Error finding shop by domain: {e}")
        return None


async def find_product(shop_id, title, title2):
    try:
        collection = get_products_collection()
        query = {"shop_id": ObjectId(shop_id), "title": title, "title2": title2}
        return await collection.find_one(query)
    except Exception as e:
        print(f"Error finding product: {e}")
        return None

async def find_price_history_document(product_id):
    try:
        collection = get_price_history_collection()
        return await collection.find_one({"product_id": ObjectId(product_id)})
    except Exception as e:
        print(f"Error finding price history document: {e}")
        return None



async def find_products_in_batch(product_queries):
    """
    Fetch multiple products matching the batch queries.
    """
    query_filter = {"$or": product_queries}
    return await get_products_collection().find(query_filter).to_list(None)



async def find_price_histories_in_batch(product_ids):
    """
    Fetch price history records for a batch of product IDs.
    """
    query_filter = {"product_id": {"$in": product_ids}}
    return await get_price_history_collection().find(query_filter).to_list(None)





# ----------------------Get documents functions----------------------
async def get_shops_in_batches(batch_size):
    """
    Fetches shops from the collection in batches and returns a list of dictionaries in each batch.
    Handles exceptions gracefully using try-except blocks.
    """
    try:
        collection = get_shop_collection()  # Get the shop collection
        cursor = collection.find().batch_size(batch_size)  # Create a cursor with specified batch size

        batch = []  # Temporary storage for the current batch

        async for shop in cursor:
            try:
                # Check if the shop contains the required 'domain' field
                if 'domain' in shop:
                    batch.append(shop)  # Add valid shop to the batch
                else:
                    print(f"Skipping invalid shop: {shop}")
                    if isinstance(shop, str):  # Additional logging for unexpected string types
                        print(f"Shop data is a string: {shop}")

                # Yield the batch when it reaches the batch size
                if len(batch) == batch_size:
                    yield batch
                    batch = []  # Reset the batch
            except Exception as e:
                print(f"Error processing shop: {e}")

        # Yield any remaining shops that didn't fill the last batch
        if batch:
            yield batch

    except Exception as e:
        print(f"Error fetching shops in batches: {e}")


async def number_of_products_on_sale(shop_id):
    """
    Counts the number of products on sale for a given shop ID.
    """
    return await get_products_collection().count_documents({"shop_id": shop_id, "is_on_sale": True})

async def get_number_of_products(shop_id):
    """
    Counts the total number of products for a given shop ID.
    """
    return await get_products_collection().count_documents({"shop_id": shop_id})




#--------------------------------ETag documents function-----------------------------------------------
async def get_etag_from_db(shop_url: str, page: int) -> str:
    """
    Retrieve the ETag for a specific shop_url and page from the database.

    Args:
        db: MongoDB client instance.
        shop_url (str): The domain of the Shopify store, e.g., 'store.calm.com'.
        page (int): The page number for which to retrieve the ETag.

    Returns:
        str: The ETag for the specific page. Returns None if not found.
    """
    document = await get_etags_collection().find_one({"shop_url": shop_url}, {"_id": 0, "page_etags": 1})
    if document and len(document["page_etags"]) >= page:
        return document["page_etags"][page - 1]  # Pages are 1-based
    return None

async def save_etag_to_db( shop_url: str, page: int, new_etag: str, exists: bool):
    """
    Save or update the ETag for a specific shop_url and page in the database.

    Args:
        shop_url (str): The domain of the Shopify store, e.g., 'store.calm.com'.
        page (int): The page number for which to save or update the ETag.
        new_etag (str): The new ETag to save or update.
        exists (bool): If True, update the existing ETag for the specified page.
                       If False, create a new ETag at the specified location.

    Returns:
        None
    """
    collection = get_etags_collection()

    if exists:
        # Update the ETag for the specific page
        update_query = {
            "shop_url": shop_url
        }
        update_operation = {
            "$set": {f"page_etags.{page - 1}": new_etag}  # Pages are 1-based, MongoDB arrays are 0-based
        }
        await collection.update_one(update_query, update_operation)
    else:
        # Add a new ETag at the specified page
        update_query = {
            "shop_url": shop_url
        }
        update_operation = {
            "$push": {
                "page_etags": {
                    "$each": [new_etag],
                    "$position": page - 1  # Insert at the specific position (1-based to 0-based)
                }
            }
        }
        await collection.update_one(update_query, update_operation, upsert=True)
