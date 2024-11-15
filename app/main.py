import database
import import_csv
from datetime import datetime, timedelta, timezone
from bson import ObjectId
from scraping import fetch_products
import asyncio

shop_data = {
    "name": "Bethesda",
    "domain": "bethesda.net",
    "shop_urls": ["gear.bethesda.net", "international.gear.bethesda.net"],
    
}

database.test_connection()

# database.update_shop_fields({"_id":ObjectId("672f5a3f88a853fd0bf80d9b")}, {"last_scanned": datetime.utcnow(),
#     "products_on_sale_percentage": 80,
#     "scan_frequency": "30 days",
#     "next_scan_due_date": datetime.now(timezone.utc) + timedelta(days=30)})

# database.delete_shop({"_id": ObjectId("672f5a3f88a853fd0bf80d9b")})

# database.create_shop(shop_data)

# database.update_product_fields({"_id": ObjectId("672f74eff1d6877d20a6c360")}, 
#                                {
#                                 "price": 24.99,
#                                 "compare_at_price": 29.99,
#                                 "last_scanned": datetime.utcnow()  
#                                })

# database.delete_product({"_id": ObjectId("672f9b1b3ed35d34a59bcd1f")})

# database.create_product({
#     "shop_id": ObjectId("672f5a4088a853fd0bf80d9d"),
#     "title": "Example Product",
#     "title2": "Default Product",
#     "price": 29.99,
#     "compare_at_price": 35.99,
#     "last_scanned": datetime.utcnow()
# })

# database.create_price_history_document({
#     "product_id": ObjectId("672f9e9126e607aa22dce17d"),
#     "price": 19.99,
#     "compare_at_price": 24.99,
#     "timestamp": datetime.utcnow()
# })


# database.create_product({
#     shop_id: ObjectId(""),
#     title:"Product1",
#     title2: "Default Product",
#     price:29.99,
#     compare_at_price:30,
# })

# database.create_price_history_document({
#     "product_id": ObjectId("672f9e9126e607aa22dce17d"),
#     "price": 19.99,
#     "compare_at_price": 25.99
# })


# database.update_price_history({"_id":ObjectId("672f9eb3398ae9c6b2a2cbf1")}, {
#     "price": 39.99,
#     "compare_at_price": 59.99
# })

# database.delete_price_history({"_id":ObjectId("672f9eb3398ae9c6b2a2cbf1")})

# batch_size = 10
# for batch in database.get_shops_in_batches(batch_size):
#     for shop in range(0, len(batch)):
#         print(f"{batch[shop]}\n")


# shop_id = database.find_shop_id_by_domain("nike.com")
# print(shop_id)

# database.create_product({
#     "shop_id": ObjectId("672f5a4488a853fd0bf80db9"),
#     "title": "537819-727980-010-98c07d73-9134-4929-8142-a019902b68a1-7b82d378-f620-4b49-91d8-33524abe761a",
#     "title2": "MEN'S TEAM LS LEGEND CREW",
#     "price": 40.33,
# })