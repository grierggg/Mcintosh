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
                'country': 'US'
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
        """Cover major US metropolitan areas"""
        # Major US metro areas with coordinates
        locations = [
            # Northeast
            ("10001", 40.7505, -73.9965),  # NYC
            ("02108", 42.3588, -71.0707),  # Boston
            ("19102", 39.9516, -75.1665),  # Philadelphia
            
            # West Coast
            ("90048", 34.0741, -118.3725),  # LA
            ("94102", 37.7793, -122.4192),  # San Francisco
            ("98101", 47.6097, -122.3331),  # Seattle
            
            # Midwest
            ("60601", 41.8843, -87.6253),  # Chicago
            ("48226", 42.3314, -83.0458),  # Detroit
            ("55401", 44.9778, -93.2650),  # Minneapolis
            
            # South
            ("30303", 33.7490, -84.3880),  # Atlanta
            ("75201", 32.7831, -96.7987),  # Dallas
            ("77002", 29.7604, -95.3698),  # Houston
            ("33131", 25.7617, -80.1918),  # Miami
            
            # Mountain
            ("80202", 39.7392, -104.9903),  # Denver
            ("85004", 33.4484, -112.0740)   # Phoenix
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
        csv_filename = 'mcintosh_dealers_current.csv'
        df.to_csv(csv_filename, index=False)
        logging.info(f"Saved {len(dealers)} unique dealers to {csv_filename}")

if __name__ == "__main__":
    scraper = McIntoshScraper()
    scraper.scrape_dealers()