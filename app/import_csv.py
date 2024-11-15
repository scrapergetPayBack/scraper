import csv
import database
file_path = 'C:\\Users\\milan\\OneDrive\\Desktop\\All-Live-Shopify-Sitescsv\\All-Live-Shopify-Sites.csv'  # Replace with your actual file path

def read_csv_into_database():
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        for row in reader:
            domain = row.get('Domain', '').strip()
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

            database.create_shop(shop_data)

            