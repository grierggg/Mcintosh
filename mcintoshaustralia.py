import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
import logging
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class McIntoshScraper:
    def __init__(self):
        self.base_url = "https://www.mcintoshlabs.com/ajax/AjaxFunctions.aspx"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        self.dealers = set()  # Using set to avoid duplicates
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('scraper.log'),
                logging.StreamHandler()
            ]
        )

    def search_dealers(self, zip_code: str, lat: float, lon: float, radius: int = 50):
        """Search dealers with exact parameters"""
        try:
            payload = {
                'ReqCase': 'getDealersInRange',
                'radius': str(radius),
                'userLat': str(lat),
                'userLong': str(lon),
                'zipCode': zip_code,
                'country': 'AU'
            }
            
            response = requests.post(
                self.base_url,
                data=payload,
                headers=self.headers,
                verify=False
            )
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            dealer_items = soup.find_all('li', class_='item')
            
            dealers = []
            for item in dealer_items:
                if 'No Dealer Found' not in item.text:
                    dealer = self.parse_dealer(item)
                    if dealer:
                        dealers.append(dealer)
                        logging.info(f"Found dealer: {dealer['name']}")
            
            return dealers
            
        except Exception as e:
            logging.error(f"Error searching dealers for {zip_code}: {e}")
            return []

    def parse_dealer(self, item):
        """Parse only needed dealer information"""
        try:
            dealer = {}
            
            # Get name
            head = item.find('div', class_='dealers-head')
            if head and head.find('h4'):
                dealer['name'] = head.find('h4').get_text(strip=True)
            else:
                return None
            
            # Get address
            details = item.find('div', class_='dealers-details')
            if details:
                address = details.find('p')
                if address:
                    # Clean up address formatting
                    addr_parts = [part.strip() for part in address.stripped_strings 
                                if not part.startswith(('P:', 'F:', 'E:'))]
                    dealer['address'] = ' '.join(addr_parts)
                else:
                    dealer['address'] = ''
                
                # Get website
                website = details.find('a', href=lambda x: x and (x.startswith('http') or x.startswith('www')))
                dealer['website'] = website['href'] if website else ''
                
                # Get email
                email = details.find('a', href=lambda x: x and x.startswith('mailto:'))
                dealer['email'] = email['href'].replace('mailto:', '') if email else ''
            
            return dealer
            
        except Exception as e:
            logging.error(f"Error parsing dealer: {e}")
            return None

    def scrape_dealers(self):
        """Cover major Australian metropolitan areas"""
        # Major Australian metro areas with coordinates
        locations = [
            # New South Wales
            ("2000", -33.8688, 151.2093),  # Sydney
            ("2300", -32.9283, 151.7817),  # Newcastle
            ("2500", -34.4278, 150.8931),  # Wollongong
            
            # Victoria
            ("3000", -37.8136, 144.9631),  # Melbourne
            ("3220", -38.1485, 144.3613),  # Geelong
            
            # Queensland
            ("4000", -27.4705, 153.0260),  # Brisbane
            ("4217", -28.0167, 153.4000),  # Gold Coast
            ("4870", -16.9186, 145.7781),  # Cairns
            
            # Western Australia
            ("6000", -31.9505, 115.8605),  # Perth
            ("6230", -33.3283, 115.6400),  # Bunbury
            
            # South Australia
            ("5000", -34.9285, 138.6007),  # Adelaide
            
            # Tasmania
            ("7000", -42.8821, 147.3272),  # Hobart
            
            # Northern Territory
            ("0800", -12.4634, 130.8456),  # Darwin
            
            # ACT
            ("2600", -35.2809, 149.1300)   # Canberra
        ]

        all_dealers = []
        for zip_code, lat, lon in locations:
            logging.info(f"Searching dealers near {zip_code}")
            dealers = self.search_dealers(zip_code, lat, lon, radius=100)
            all_dealers.extend(dealers)
            time.sleep(2)  # Respectful delay between requests

        # Remove duplicates based on name and address
        seen = set()
        unique_dealers = []
        for dealer in all_dealers:
            key = f"{dealer['name']}-{dealer['address']}"
            if key not in seen:
                seen.add(key)
                unique_dealers.append(dealer)

        # Save all unique dealers
        self.save_dealers(unique_dealers)

    def save_dealers(self, dealers):
        """Save dealers to CSV"""
        if not dealers:
            logging.info("No dealers found to save")
            return
            
        df = pd.DataFrame(dealers)
        csv_filename = 'mcintosh_dealers_australia.csv'
        df.to_csv(csv_filename, index=False)
        logging.info(f"Saved {len(dealers)} unique dealers to {csv_filename}")

if __name__ == "__main__":
    scraper = McIntoshScraper()
    scraper.scrape_dealers()