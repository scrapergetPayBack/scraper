import asyncio
import csv
import database
file_path = '53ba7bc8-ddcf-4f11-9192-d9f243cd0229.csv'  # Replace with your actual file path

async def read_csv_into_database():
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        for row in reader:

            domain = row.get('Root Domain', '').strip()
            location_on_site = row.get('Location on Site', '').strip()
            company = row.get('Company', '').strip()
            
            # Split the 'Location on Site' field by ';' and store in a list
            location_list = location_on_site.split(';')
            
            # Now lets insert all of this data in the database
            shop_data = {
                "name": company,
                "domain": domain,
                "shop_urls": location_list,
            }

            await database.create_shop(shop_data)



if __name__ == '__main__':
    asyncio.run(read_csv_into_database())
