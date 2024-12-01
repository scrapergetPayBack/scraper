# # from pymongo.mongo_client import MongoClient
# # from pymongo.server_api import ServerApi

# from motor.motor_asyncio import AsyncIOMotorClient


# from db_models import ShopModel, UpdatePriceHistoryModel, UpdateProductModel
# from db_models import ShopUpdateModel
# from db_models import ProductModel
# from db_models import PriceHistoryModel
# from bson import ObjectId
# from datetime import datetime, timedelta, timezone
# from pydantic import ValidationError






# # Create a new client and connect to the server
# uri = "mongodb+srv://milanmilancen12345:TusbFJTRnzR8Lra7@cluster0shopify.1wo95.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0Shopify"
# client = AsyncIOMotorClient(uri)







# # Send a ping to confirm a successful connection
def test_connection():
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)






# def get_database():
#     return client['shopify_stores']






# # ----------------------get all collections----------------------
# def get_shop_collection():
#     db = get_database()
#     return db['shop']
# def get_products_collection():
#     db = get_database()
#     return db['products']
# def get_price_history_collection():
#     db = get_database()
#     return db['price_history']






# # ----------------------Create documents functions go below----------------------
# def create_shop(shop_data:ShopModel):
#     try:
#         shop = ShopModel(**shop_data)
#         shop_collection = get_shop_collection()
#         result = shop_collection.insert_one(shop.dict(exclude_unset=False))
#         print("Shop has been created")
#     except ValidationError as ve:
#         print("Validation error", ve)
#     except Exception as e:
#         print("Error creating shop:", e)
# def create_product(product_data:ProductModel):
#     """
#     Inserts a new product into the products collection.

#     Parameters:
#     - product_data (dict): Dictionary containing product details:
#       shop_id (str), title (str), variants (list), and last_scanned (datetime)

#     Returns:
#     - result (dict): The result of the insert operation
#     """
#     try:
#         # Validate and sanitize the product data using ProductModel
#         validated_product = ProductModel(**product_data)

#         # Convert the validated data to a dictionary
#         product_document = validated_product.dict()
        
#         # Insert the product into the collection
#         collection = get_products_collection()
#         result = collection.insert_one(product_document)

#         print(f"Product created with ID: {result.inserted_id}")
#         return result

#     except Exception as e:
#         print("Error creating product:", e)
# def create_price_history_document(price_history_data:PriceHistoryModel):
#     """
#     Inserts a new price history entry into the price_history collection.

#     Parameters:
#     - price_history_data (dict): Dictionary containing price history details:
#       product_id (ObjectId), price (float), compare_at_price (Optional[float]), and timestamp (datetime)

#     Returns:
#     - result (dict): The result of the insert operation
#     """
#     try:
#         # Validate and sanitize the price history data using PriceHistoryModel
#         validated_price_history = PriceHistoryModel(**price_history_data)

#         # Convert the validated data to a dictionary
#         price_history_document = validated_price_history.dict()

#         # Insert the document into the price_history collection
#         collection = get_price_history_collection()
#         result = collection.insert_one(price_history_document)

#         print(f"Price history document created with ID: {result.inserted_id}")
#         return result

#     except ValidationError as ve:
#         print("Validation error:", ve)
#     except Exception as e:
#         print("Error creating price history document:", e)






# # ----------------------Update documents functions go below----------------------
# def update_shop_fields(identifier, update_data:ShopUpdateModel):
#     """
#     Parameters:
#     - identifier (dict): A dictionary to locate the document (e.g., {"domain": "bethesda.net"} or {"_id": ObjectId("...")})
#     - update_data (dict): A dictionary with the fields to update: 'last_scanned', 'products_on_sale_percentage', 'scan_frequency', and 'next_scan_due_date'
#     """
#     try:
#         validated_data = ShopUpdateModel(**update_data)
        

#         sanitized_updates = validated_data.dict()

#         collection = get_shop_collection()
#         result = collection.update_one(identifier, {"$set": sanitized_updates})
        
#         if result.matched_count:
#             print(f"Updated the shop")
#         else:
#             print("No matching document found to update.")
#         return result
#     except ValidationError as ve:
#         print("Validation error:", ve)
#     except Exception as e:
#         print("Error updating shop:", e)
# def update_product_fields(identifier, update_data:UpdateProductModel):
#     """
#     Updates specified fields in a product document in the products collection.

#     Parameters:
#     - identifier (dict): A dictionary to locate the document (e.g., {"_id": ObjectId("...")})
#     - update_data (dict): A dictionary with the fields to update, such as 'price', 'compare_at_price', and 'last_scanned'

#     Returns:
#     - result (dict): The result of the update operation
#     """
#     try:
#         # Validate and sanitize the update data using UpdateProductModel
#         validated_data = UpdateProductModel(**update_data)

#         # Convert to a dictionary and exclude None values
#         sanitized_updates = validated_data.dict(exclude_unset=True)

#         # Retrieve the product collection
#         collection = get_products_collection()
        
#         # Perform the update operation
#         result = collection.update_one(identifier, {"$set": sanitized_updates})

#         if result.matched_count:
#             print(f"Updated product with ID: {identifier}")
#         else:
#             print("No matching product found to update.")
#         return result
#     except ValidationError as ve:
#         print("Validation error:", ve)
#     except Exception as e:
#         print("Error updating product:", e)
# def update_price_history(identifier, update_data:UpdatePriceHistoryModel):
#     """
#     Updates specified fields in a product document in the products collection.

#     Parameters:
#     - identifier (dict): A dictionary to locate the document (e.g., {"_id": ObjectId("...")})
#     - update_data (dict): A dictionary with the fields to update, such as 'price', 'compare_at_price', and 'timestamp'

#     Returns:
#     - result (dict): The result of the update operation
#     """
#     try:
#         # Validate and sanitize the update data using UpdateProductModel
#         validated_data = UpdatePriceHistoryModel(**update_data)

#         # Convert to a dictionary and exclude None values
#         sanitized_updates = validated_data.dict(exclude_unset=True)

#         # Retrieve the product collection
#         collection = get_price_history_collection()
        
#         # Perform the update operation
#         result = collection.update_one(identifier, {"$set": sanitized_updates})

#         if result.matched_count:
#             print(f"Updated price_history with ID: {identifier}")
#         else:
#             print("No matching product found to update.")
#         return result
#     except ValidationError as ve:
#         print("Validation error:", ve)
#     except Exception as e:
#         print("Error updating product:", e)






# # ----------------------Delete document functions go below----------------------
# def delete_shop(identifier):
#     """
#     Deletes a shop document from the shop collection based on the identifier.
#     Also performs a cascading delete to remove all products associated with this shop.

#     Parameters:
#     - identifier (dict): A dictionary to locate the shop document (e.g., {"_id": ObjectId("...")})

#     Returns:
#     - result (dict): The result of the shop delete operation
#     """
#     try:
#         # Delete the shop
#         shop_collection = get_shop_collection()
#         shop_result = shop_collection.delete_one(identifier)

#         if shop_result.deleted_count:
#             print(f"Successfully deleted the shop document with identifier: {identifier}")

#             # Cascade delete: remove products associated with this shop_id
#             shop_id = identifier.get("_id")
#             if shop_id:
#                 product_collection = get_products_collection()
#                 product_result = product_collection.delete_many({"shop_id": shop_id})

#                 print(f"Successfully deleted {product_result.deleted_count} associated product(s) for shop_id: {shop_id}")
#             return shop_result
#         else:
#             print("No matching shop document found to delete.")
#             return shop_result

#     except Exception as e:
#         print(f"Error deleting shop: {e}")
# def delete_product(identifier):
#     """
#     Deletes a product document from the products collection based on the identifier.
#     Also performs a cascading delete to remove all related price history entries.

#     Parameters:
#     - identifier (dict): A dictionary to locate the document (e.g., {"_id": ObjectId("...")})

#     Returns:
#     - result (dict): The result of the delete operation
#     """
#     try:
#         # Retrieve the products collection
#         products_collection = get_products_collection()
        
#         # First, find the product document to get the product ID
#         product = products_collection.find_one(identifier)
        
#         if product:
#             product_id = product["_id"]
            
#             # Delete the product from the products collection
#             result = products_collection.delete_one(identifier)

#             if result.deleted_count:
#                 print(f"Successfully deleted the product with identifier: {identifier}")

#                 # Cascade delete: remove all price history documents associated with this product_id
#                 price_history_collection = get_price_history_collection()
#                 price_history_result = price_history_collection.delete_many({"product_id": product_id})

#                 print(f"Successfully deleted {price_history_result.deleted_count} associated price history document(s) for product_id: {product_id}")
#             else:
#                 print("No matching product found to delete.")
                
#             return result
#         else:
#             print("No matching product found to delete.")
#             return None

#     except Exception as e:
#         print(f"Error deleting product: {e}")
# def delete_price_history(identifier):
#     """
#     Deletes a document from the price_history collection based on the given identifier.

#     Parameters:
#     - identifier (dict): A dictionary to locate the document (e.g., {"product_id": ObjectId("...")})

#     Returns:
#     - result (dict): The result of the delete operation
#     """
#     try:
#         # Initialize the MongoDB client and access the price_history collection
#         price_history_collection = get_price_history_collection()
        
#         # Perform the delete operation
#         result = price_history_collection.delete_one(identifier)

#         if result.deleted_count:
#             print(f"Successfully deleted the document with identifier: {identifier}")
#         else:
#             print("No matching document found to delete.")
#         return result

#     except Exception as e:
#         print(f"Error deleting price history document: {e}")






# # ----------------------FIND documents functions go below----------------------
# def find_shop_id_by_domain(shop_domain):
#     """
#     Finds and returns the shop_id of a shop based on the provided shop_domain.

#     Parameters:
#     - shop_domain (str): The domain of the shop to search for.

#     Returns:
#     - shop_id (ObjectId or None): The shop_id if found, else None.
#     """
#     try:
#         # Get the shop collection
#         collection = get_shop_collection()
        
#         # Search for the shop by domain
#         shop = collection.find_one({"domain": shop_domain}, {"_id": 1})
        
#         # Return the shop_id if found
#         if shop:
#             return shop["_id"]
#         else:
#             #print("No shop found with the specified domain.")
#             return None

#     except Exception as e:
#         #print(f"Error finding shop by domain: {e}")
#         return None
# def find_product(shop_id, title, title2):
#     """
#     Finds a product document in the products collection based on shop_id, title, and title2.

#     Parameters:
#     - shop_id (ObjectId): The ID of the shop this product belongs to
#     - title (str): The title of the product
#     - title2 (str): The title inside the variants object

#     Returns:
#     - dict or None: The found product document, or None if no match is found
#     """
#     try:
#         # Retrieve the products collection
#         collection = get_products_collection()
        
#         # Define the search criteria
#         query = {
#             "shop_id": ObjectId(shop_id),
#             "title": title,
#             "title2": title2
#         }
        
#         # Find the product
#         product = collection.find_one(query)
        
#         # if product:
#         #     print("Product found:", product)
#         # else:
#         #     print("No matching product found.")
        
#         return product

#     except Exception as e:
#         print(f"Error finding product: {e}")
#         return None
# def find_price_history_document(product_id):
#     """
#     Finds the unique price history record for a given product ID.

#     Parameters:
#     - product_id (ObjectId): The ID of the product to search for in the price history.

#     Returns:
#     - dict: The price history record for the specified product, or None if no record is found.
#     """
#     try:
#         price_history_collection = get_price_history_collection()
#         # Retrieve the unique price history document for the specified product_id
#         price_history_record = price_history_collection.find_one(
#             {"product_id": ObjectId(product_id)}
#         )
        
#         return price_history_record  # Returns None if not found
    
#     except Exception as e:
#         print(f"An error occurred while retrieving price history: {e}")
#         return None
# def find_shop_id_by_domain(shop_domain):
#     """
#     Finds the shop_id of a shop by its domain.
    
#     Parameters:
#     - shop_domain (str): The domain of the shop to search for.
    
#     Returns:
#     - ObjectId: The shop_id of the found shop, or None if not found.
#     """
#     shop = get_shop_collection().find_one({"domain": shop_domain})
#     if shop:
#         return shop["_id"]
#     else:
#         return None






# # ----------------------GET documents functions go below----------------------
# def get_shops_in_batches(batch_size):
#     """
#     Retrieves shops from the 'shop' collection in batches.

#     Parameters:
#     - batch_size (int): The number of shops to retrieve per batch.

#     Yields:
#     - List of shops in each batch.
#     """
#     collection = get_shop_collection()
    
#     # Get the total number of shops in the collection
#     total_shops = collection.count_documents({})
    
#     # Loop through the collection in batches
#     for offset in range(0, total_shops, batch_size):
#         shops_batch = collection.find().skip(offset).limit(batch_size)
#         yield list(shops_batch)
# def number_of_products_on_sale(shop_id):
#     """
#     Counts the number of products on sale for a specific shop_id.
    
#     Parameters:
#     - shop_id (ObjectId): The ID of the shop to search for.
    
#     Returns:
#     - int: Number of products marked as 'on sale' for the shop.
#     """
#     return get_products_collection().count_documents({"shop_id": shop_id, "is_on_sale": True})
# def get_number_of_products(shop_id):
#     """
#     Gets the total number of products for a specific shop_id.
    
#     Parameters:
#     - shop_id (ObjectId): The ID of the shop to search for.
    
#     Returns:
#     - int: Total number of products for the shop.
#     """
#     return get_products_collection().count_documents({"shop_id": shop_id})


# from motor.motor_asyncio import AsyncIOMotorClient
# from db_models import ShopModel, UpdatePriceHistoryModel, UpdateProductModel
# from db_models import ShopUpdateModel, ProductModel, PriceHistoryModel
# from bson import ObjectId
# from datetime import datetime
# from pydantic import ValidationError


# # Create a new client and connect to the server
# uri = "mongodb+srv://milanmilancen12345:TusbFJTRnzR8Lra7@cluster0shopify.1wo95.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0Shopify"
# client = AsyncIOMotorClient(uri)


# def get_database():
#     return client['shopify_stores']


# # ----------------------Get all collections----------------------
# async def get_shop_collection():
#     return get_database()['shop']


# def get_products_collection():
#     return get_database()['products']


# def get_price_history_collection():
#     return get_database()['price_history']


# # ----------------------Create documents functions----------------------
# async def create_shop(shop_data: ShopModel):
#     try:
#         shop = ShopModel(**shop_data)
#         shop_collection = get_shop_collection()
#         result = await shop_collection.insert_one(shop.dict(exclude_unset=False))
#         print("Shop created with ID:", result.inserted_id)
#     except ValidationError as ve:
#         print("Validation error:", ve)
#     except Exception as e:
#         print("Error creating shop:", e)


# async def create_product(product_data: ProductModel):
#     try:
#         validated_product = ProductModel(**product_data)
#         product_document = validated_product.dict()
#         collection = get_products_collection()
#         result = await collection.insert_one(product_document)
#         print("Product created with ID:", result.inserted_id)
#     except Exception as e:
#         print("Error creating product:", e)


# async def create_price_history_document(price_history_data: PriceHistoryModel):
#     try:
#         validated_price_history = PriceHistoryModel(**price_history_data)
#         price_history_document = validated_price_history.dict()
#         collection = get_price_history_collection()
#         result = await collection.insert_one(price_history_document)
#         print("Price history document created with ID:", result.inserted_id)
#     except ValidationError as ve:
#         print("Validation error:", ve)
#     except Exception as e:
#         print("Error creating price history document:", e)


# # ----------------------Update documents functions----------------------
# async def update_shop_fields(identifier, update_data: ShopUpdateModel):
#     try:
#         validated_data = ShopUpdateModel(**update_data)
#         sanitized_updates = validated_data.dict()
#         collection = get_shop_collection()
#         result = await collection.update_one(identifier, {"$set": sanitized_updates})
#         if result.matched_count:
#             print("Updated the shop.")
#         else:
#             print("No matching document found to update.")
#         return result
#     except ValidationError as ve:
#         print("Validation error:", ve)
#     except Exception as e:
#         print("Error updating shop:", e)


# async def update_product_fields(identifier, update_data: UpdateProductModel):
#     try:
#         validated_data = UpdateProductModel(**update_data)
#         sanitized_updates = validated_data.dict(exclude_unset=True)
#         collection = get_products_collection()
#         result = await collection.update_one(identifier, {"$set": sanitized_updates})
#         if result.matched_count:
#             print(f"Updated product with ID: {identifier}")
#         else:
#             print("No matching product found to update.")
#         return result
#     except ValidationError as ve:
#         print("Validation error:", ve)
#     except Exception as e:
#         print("Error updating product:", e)


# async def update_price_history(identifier, update_data: UpdatePriceHistoryModel):
#     try:
#         validated_data = UpdatePriceHistoryModel(**update_data)
#         sanitized_updates = validated_data.dict(exclude_unset=True)
#         collection = get_price_history_collection()
#         result = await collection.update_one(identifier, {"$set": sanitized_updates})
#         if result.matched_count:
#             print(f"Updated price history with ID: {identifier}")
#         else:
#             print("No matching document found to update.")
#         return result
#     except ValidationError as ve:
#         print("Validation error:", ve)
#     except Exception as e:
#         print("Error updating price history:", e)


# # ----------------------Delete documents functions----------------------
# async def delete_shop(identifier):
#     try:
#         shop_collection = get_shop_collection()
#         shop_result = await shop_collection.delete_one(identifier)
#         if shop_result.deleted_count:
#             print(f"Deleted shop with identifier: {identifier}")
#             shop_id = identifier.get("_id")
#             if shop_id:
#                 product_collection = get_products_collection()
#                 await product_collection.delete_many({"shop_id": shop_id})
#         else:
#             print("No matching shop document found to delete.")
#     except Exception as e:
#         print(f"Error deleting shop: {e}")


# async def delete_product(identifier):
#     try:
#         product_collection = get_products_collection()
#         product = await product_collection.find_one(identifier)
#         if product:
#             product_id = product["_id"]
#             await product_collection.delete_one(identifier)
#             price_history_collection = get_price_history_collection()
#             await price_history_collection.delete_many({"product_id": product_id})
#             print(f"Deleted product and associated price history for ID: {product_id}")
#         else:
#             print("No matching product found.")
#     except Exception as e:
#         print(f"Error deleting product: {e}")


# async def delete_price_history(identifier):
#     try:
#         price_history_collection = get_price_history_collection()
#         result = await price_history_collection.delete_one(identifier)
#         if result.deleted_count:
#             print(f"Deleted price history with identifier: {identifier}")
#         else:
#             print("No matching document found to delete.")
#     except Exception as e:
#         print(f"Error deleting price history: {e}")


# # ----------------------Find documents functions----------------------
# async def find_shop_id_by_domain(shop_domain):
#     try:
#         collection = get_shop_collection()
#         shop = await collection.find_one({"domain": shop_domain}, {"_id": 1})
#         return shop["_id"] if shop else None
#     except Exception as e:
#         print(f"Error finding shop by domain: {e}")
#         return None


# async def find_product(shop_id, title, title2):
#     try:
#         collection = get_products_collection()
#         query = {"shop_id": ObjectId(shop_id), "title": title, "title2": title2}
#         return await collection.find_one(query)
#     except Exception as e:
#         print(f"Error finding product: {e}")
#         return None


# async def find_price_history_document(product_id):
#     try:
#         collection = get_price_history_collection()
#         return await collection.find_one({"product_id": ObjectId(product_id)})
#     except Exception as e:
#         print(f"Error finding price history document: {e}")
#         return None


# # ----------------------Get documents functions----------------------
# async def get_shops_in_batches(batch_size):
#     """
#     Asynchronously retrieves shops in batches from the MongoDB collection.
#     """
#     collection = await get_shop_collection()
    
#     # Use an async cursor to fetch documents
#     cursor = collection.find()
#     cursor.batch_size(batch_size)  # Define the batch size for fetching documents in batches
    
#     async for shop in cursor:

        
#         # Check if the shop is a dictionary and contains necessary keys
#         if isinstance(shop, dict) and 'domain' in shop:
#             yield shop
#         else:
#             print(f"Skipping invalid shop: {shop}")

# async def number_of_products_on_sale(shop_id):
#     return await get_products_collection().count_documents({"shop_id": shop_id, "is_on_sale": True})


# async def get_number_of_products(shop_id):
#     return await get_products_collection().count_documents({"shop_id": shop_id})


from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import UpdateOne
from db_models import ShopModel, UpdatePriceHistoryModel, UpdateProductModel
from db_models import ShopUpdateModel, ProductModel, PriceHistoryModel
from bson import ObjectId
from datetime import datetime
from pydantic import ValidationError

# Create a new client and connect to the server
#uri = "mongodb+srv://milanmilancen12345:TusbFJTRnzR8Lra7@cluster0shopify.1wo95.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0Shopify"
uri = "mongodb://admin:your-secure-password@172.235.40.90:27017"
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


# async def create_price_history_document(price_history_data: PriceHistoryModel):
#     try:
#         validated_price_history = PriceHistoryModel(**price_history_data)
#         price_history_document = validated_price_history.dict()
#         collection = get_price_history_collection()
#         result = await collection.insert_one(price_history_document)
#         #print("Price history document created with ID:", result.inserted_id)
#     except ValidationError as ve:
#         print("Validation error:", ve)
#     except Exception as e:
#         print("Error creating price history document:", e)

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
# async def update_shop_fields(identifier, update_data: ShopUpdateModel):
#     try:
#         validated_data = ShopUpdateModel(**update_data)
#         sanitized_updates = validated_data.dict()
#         collection = get_shop_collection()
#         result = await collection.update_one(identifier, {"$set": sanitized_updates})
#         if result.matched_count:
#             print("Updated the shop.")
#         else:
#             print("No matching document found to update.")
#         return result
#     except ValidationError as ve:
#         print("Validation error:", ve)
#     except Exception as e:
#         print("Error updating shop:", e)

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



# async def update_product_fields(identifier, update_data: UpdateProductModel):
#     try:
#         validated_data = UpdateProductModel(**update_data)
#         sanitized_updates = validated_data.dict(exclude_unset=True)
#         collection = get_products_collection()
#         result = await collection.update_one(identifier, {"$set": sanitized_updates})
#         # if result.matched_count:
#         #     print(f"Updated product with ID: {identifier}")
#         # else:
#         #     print("No matching product found to update.")
#         return result
#     except ValidationError as ve:
#         print("Validation error:", ve)
#     except Exception as e:
#         print("Error updating product:", e)

# async def update_price_history(identifier, update_data: UpdatePriceHistoryModel):
#     try:
#         validated_data = UpdatePriceHistoryModel(**update_data)
#         sanitized_updates = validated_data.dict(exclude_unset=True)
#         collection = get_price_history_collection()
#         result = await collection.update_one(identifier, {"$set": sanitized_updates})
#         # if result.matched_count:
#         #     print(f"Updated price history with ID: {identifier}")
#         # else:
#         #     print("No matching document found to update.")
#         return result
#     except ValidationError as ve:
#         print("Validation error:", ve)
#     except Exception as e:
#         print("Error updating price history:", e)
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
# async def get_shops_in_batches(batch_size):
#     """
#     Fetches shops from the collection in batches and returns a list of dictionaries in each batch.
#     """
#     collection = get_shop_collection()  # Get the shop collection
#     cursor = collection.find().batch_size(batch_size)  # Create a cursor with specified batch size

#     batch = []  # Temporary storage for the current batch

#     async for shop in cursor:
#         if isinstance(shop, dict) and 'domain' in shop:
#             batch.append(shop)  # Add valid shop to the batch
#         else:
#             print(f"Skipping invalid shop: {shop}")
#             if isinstance(shop, str):
#                 print(f"Shop data is a string: {shop}")

#         # Yield the batch if it reaches the batch size
#         if len(batch) == batch_size:
#             yield batch
#             batch = []  # Reset the batch

#     # Yield any remaining shops that didn't fill the last batch
#     if batch:
#         yield batch
async def get_shops_in_batches(batch_size):
    """
    Fetches shops from the collection in batches and returns a list of dictionaries in each batch.
    """
    collection = get_shop_collection()  # Get the shop collection
    cursor = collection.find().batch_size(batch_size)  # Create a cursor with specified batch size

    batch = []  # Temporary storage for the current batch

    async for shop in cursor:
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

    # Yield any remaining shops that didn't fill the last batch
    if batch:
        yield batch


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


# async def number_of_products_on_sale(shop_id):
#     return await get_products_collection().count_documents({"shop_id": shop_id, "is_on_sale": True})

# async def get_number_of_products(shop_id):
#     return await get_products_collection().count_documents({"shop_id": shop_id})
