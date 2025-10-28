import requests
import decimal
from bs4 import BeautifulSoup
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
import traceback # To print detailed errors

# Import all our database components from models.py
from models import (
    SessionLocal, 
    UnlistedStock, 
    create_unique_hash, 
    create_db_and_tables
)

# --- 1. Ethical Scraping Headers (Compliance) ---
HEADERS = {
    'User-Agent': 'UnlistedStocksBot/1.0 (+contact@domain.com)'
}

# --- 2. The Processing Function (No Changes) ---
def process_and_save(db: Session, item: dict):
    """
    Takes a dictionary of scraped data, processes it,
    and saves it to the database.
    """
    
    # 2a. Run the deduplication logic
    hash_id = create_unique_hash(
        company_name=item["company_name"],
        source_url=item["source_url"]
    )
    
    # 2b. Check if this item already exists
    exists = db.query(UnlistedStock).filter(UnlistedStock.unique_hash == hash_id).first()
    if exists:
        print(f"  SKIPPING (Duplicate): {item['company_name']}")
        return

    # 2c. Create the new database object
    new_stock = UnlistedStock(
        unique_hash=hash_id,
        company_name=item["company_name"],
        status=item.get("status", "Pre-IPO"), # Default status
        last_known_price=item.get("last_known_price"),
        price_currency=item.get("price_currency", "INR"), # Default currency
        sector=item.get("sector"),
        country=item.get("country", "India"), # Default country
        source_name=item["source_name"],
        source_url=item["source_url"],
        additional_metadata=item.get("metadata") 
    )

    # 2d. Add to session and commit
    try:
        db.add(new_stock)
        db.commit()
        print(f"  SUCCESS (Added): {new_stock.company_name}")
    except IntegrityError:
        db.rollback()
        print(f"  ERROR (IntegrityError): {item['company_name']}")
    except Exception as e:
        db.rollback()
        print(f"  ERROR (General): {item['company_name']} - {e}")

# --- 3. Helper function to clean text ---
def clean_text(text):
    """ Helper to remove extra whitespace and newlines. """
    if not text:
        return None
    return text.strip().replace('\n', ' ').replace('\r', ' ')

# --- 4. PARSER 1: UnlistedZone ---
def parse_unlistedzone_page(url: str, html_content: str) -> dict:
    """
    Uses BeautifulSoup to parse the HTML of an UnlistedZone page.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    item = {"source_url": url, "source_name": "UnlistedZone"}
    
    try:
        item["company_name"] = clean_text(soup.find('h1').text)
        
        all_strong_tags = soup.find_all('strong')
        for tag in all_strong_tags:
            tag_text = clean_text(tag.text)
            if not tag_text:
                continue

            if "Buy Price" in tag_text:
                price_str = clean_text(tag.find_next_sibling(text=True))
                price_cleaned = price_str.replace('₹', '').replace(',', '').strip()
                item["last_known_price"] = decimal.Decimal(price_cleaned)
                item["price_currency"] = "INR"

            if "Sector" in tag_text:
                item["sector"] = clean_text(tag.find_next_sibling(text=True))
                
            if "Status" in tag_text:
                item["status"] = clean_text(tag.find_next_sibling(text=True))
        
        return item
        
    except Exception as e:
        print(f"    ERROR: Failed to parse {url}. Error: {e}")
        traceback.print_exc() # Print full error
        return None
# --- 5. NEW - PARSER 2: UnlistedArena ---
def parse_unlistedarena_page(url: str, html_content: str) -> dict:
    """
    Uses BeautifulSoup to parse the HTML of an UnlistedArena page.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    item = {"source_url": url, "source_name": "UnlistedArena"}
    
    try:
        # --- FIX: Find the first <h1> tag, regardless of class ---
        company_name_tag = soup.find('h1') 
        
        if not company_name_tag:
            # If we still can't find it, we must stop
            print(f"    ERROR: Could not find <h1> tag for company name on {url}")
            return None
            
        item["company_name"] = clean_text(company_name_tag.text)
        # --- End of Fix ---
        
        # Find all table rows in the price table
        # We'll make this parser more robust by looking for the table
        data_table = soup.find('table', class_='unlisted-price-table')
        
        if data_table:
            rows = data_table.find_all('tr')
            for row in rows:
                cells = row.find_all('td')
                if len(cells) == 2:
                    # Clean the key to be sure
                    key = clean_text(cells[0].text).replace(':', '')
                    value = clean_text(cells[1].text)
                    
                    if "Unlisted Share Price" in key:
                        price_cleaned = value.replace('₹', '').replace(',', '').strip()
                        item["last_known_price"] = decimal.Decimal(price_cleaned)
                        item["price_currency"] = "INR"
                    elif "Sector" in key:
                        item["sector"] = value
                    elif "Face Value" in key:
                        item.setdefault("metadata", {})["face_value"] = value
        
        return item
        
    except Exception as e:
        print(f"    ERROR: Failed to parse {url}. Error: {e}")
        traceback.print_exc() # Print full error
        return None

# --- 6. NEW: Scraper Task List ---
# A list of all scraping jobs we need to run.
# We map the source name to the correct parser function.
SCRAPER_TASKS = [
    {
        "source_name": "UnlistedZone",
        "url": "https://unlistedzone.com/shares/buy-sell-hdb-financial-services-unlisted-shares-hdb-finance-share-price",
        "parser": parse_unlistedzone_page
    },
    {
        "source_name": "UnlistedArena",
        "url": "https://www.unlistedarena.com/unlisted-shares-list/buy-tata-capital-unlisted-shares/",
        "parser": parse_unlistedarena_page
    },
    # We can add more tasks here (UnlistedKart, etc.)
]

# --- 7. UPDATED: The Main Scraper Function ---
def run_scrapers():
    """
    Main scraping function. Loops through all tasks,
    fetches, parses, and saves data.
    """
    print("--- Starting Modular Scraper ---")
    db = SessionLocal()
    
    try:
        # Loop through each task
        for task in SCRAPER_TASKS:
            print(f"\nProcessing source: {task['source_name']}")
            target_url = task['url']
            parser_func = task['parser']
            
            try:
                # 7a. Fetch the Live HTML
                print(f"  Fetching: {target_url}")
                response = requests.get(target_url, headers=HEADERS, timeout=10)
                
                # 7b. Check for successful response
                if response.status_code == 200:
                    print("    ...Success. Parsing HTML...")
                    # 7c. Parse the HTML content
                    scraped_data = parser_func(target_url, response.text)
                    
                    # 7d. If parsing was successful, save to DB
                    if scraped_data:
                        process_and_save(db, scraped_data)
                else:
                    print(f"    ERROR: Failed to fetch page. Status code: {response.status_code}")
            
            except Exception as e:
                print(f"    ERROR: An error occurred for {target_url}: {e}")
                traceback.print_exc()

    finally:
        db.close()
        
    print("\n--- Scraper Finished ---")

# --- 8. Make the file runnable ---
if __name__ == "__main__":
    create_db_and_tables() 
    run_scrapers() # Run the main function