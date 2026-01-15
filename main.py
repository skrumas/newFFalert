import os
import asyncio
import aiohttp
import json
import logging
import gspread
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials
import re

# --- Configuration ---
PRISYNC_SITELIST_URL_TEMPLATE = "https://prisync.me/admin/fetchField/siteList/Site_page/{}/Site_sort/id.desc"
SPREADSHEET_NAME = 'New FF Alert'

# Concurrency settings
CONCURRENT_REQUESTS = 20
START_PAGE = 1
END_PAGE = 92 

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GoogleSheetsManager:
    def __init__(self, spreadsheet_name):
        self.spreadsheet_name = spreadsheet_name
        self.client = None
        self.sheet = None
        self.existing_ids = set()

    def connect(self):
        try:
            scopes = [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive'
            ]
            # GitHub Secret'tan gelen JSON verisini okuyoruz
            json_creds = json.loads(os.environ["GSPREAD_JSON"])
            creds = Credentials.from_service_account_info(json_creds, scopes=scopes)
            self.client = gspread.authorize(creds)
            self.sheet = self.client.open(self.spreadsheet_name).get_worksheet(0)
            logger.info("Connected to Google Sheets successfully.")
            self._ensure_headers()
            self._load_existing_ids()
        except Exception as e:
            logger.critical(f"Failed to connect to Google Sheets: {e}")
            raise

    def _ensure_headers(self):
        try:
            headers = self.sheet.row_values(1)
            if not headers or headers[0] != "site_id":
                logger.info("Headers missing. Inserting...")
                self.sheet.insert_row(["site_id", "URL", "ff_site"], index=1)
        except Exception as e:
            logger.error(f"Error checking headers: {e}")

    def _load_existing_ids(self):
        """
        GÜÇLENDİRİLMİŞ OKUMA:
        Google Sheets'ten gelen veriyi (Sayı, Metin, Noktalı Sayı) ne olursa olsun
        temiz bir String ID'ye çevirir. Duplicate sorununu çözer.
        """
        try:
            # Sadece A sütununu (ID'leri) çekiyoruz
            col_a_values = self.sheet.col_values(1)
            
            for val in col_a_values:
                raw_val = str(val).strip()
                if not raw_val: 
                    continue
                
                clean_id = None
                try:
                    # 1. Adım: Virgülleri temizle (Binlik ayracı varsa)
                    raw_val = raw_val.replace(",", "")
                    
                    # 2. Adım: Önce Float yap (3615.0 gibi gelirse hata vermesin)
                    val_float = float(raw_val)
                    
                    # 3. Adım: Int yap (Küsuratı at: 3615.0 -> 3615)
                    val_int = int(val_float)
                    
                    # 4. Adım: String yap ve kaydet
                    clean_id = str(val_int)
                    
                except ValueError:
                    # Eğer matematiksel sayı değilse (örn: URL veya Metin karışmışsa)
                    # İçindeki ilk sayı grubunu bulmaya çalış
                    match = re.search(r'(\d+)', raw_val)
                    if match:
                        clean_id = match.group(1)
                
                if clean_id:
                    self.existing_ids.add(clean_id)

            logger.info(f"Loaded {len(self.existing_ids)} unique existing IDs (Robust Clean).")
        except Exception as e:
            logger.error(f"Error loading existing IDs: {e}")

class AsyncScraper:
    def __init__(self):
        # Cookie stringini direkt GitHub Secret'tan alıp parse ediyoruz
        raw_cookie = os.environ.get("PRISYNC_COOKIE", "")
        self.cookies = {}
        if raw_cookie:
            for item in raw_cookie.split(";"):
                if "=" in item:
                    k, v = item.strip().split("=", 1)
                    self.cookies[k] = v
        
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        }
        self.results = []
        self.lock = asyncio.Lock()

    async def fetch_page(self, session, page_num):
        url = PRISYNC_SITELIST_URL_TEMPLATE.format(page_num)
        try:
            async with session.get(url, allow_redirects=False) as response:
                if response.status == 200:
                    return await response.text()
                elif response.status == 302:
                    logger.warning(f"Page {page_num} redirected. COOKIE MIGHT BE EXPIRED!")
                else:
                    logger.warning(f"Page {page_num} failed: {response.status}")
        except Exception as e:
            logger.error(f"Error fetching page {page_num}: {e}")
        return None

    def parse_html(self, html):
        data = []
        try:
            soup = BeautifulSoup(html, 'lxml')
            rows = soup.select("#ff-grid table.items tbody tr")
            for row in rows:
                cols = row.find_all("td")
                if len(cols) >= 3:
                    seller_name = cols[2].get_text(strip=True)
                    if not seller_name: # Boş satıcı (Fetch Field)
                        site_id = cols[0].get_text(strip=True)
                        site_url = cols[1].get_text(strip=True)
                        data.append({"ID": site_id, "Site": site_url})
        except Exception:
            pass
        return data

    async def worker(self, queue, session):
        while True:
            page_num = await queue.get()
            html = await self.fetch_page(session, page_num)
            if html:
                page_data = self.parse_html(html)
                if page_data:
                    async with self.lock:
                        self.results.extend(page_data)
            queue.task_done()

    async def run(self):
        queue = asyncio.Queue()
        for i in range(START_PAGE, END_PAGE + 1):
            queue.put_nowait(i)
        
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(cookies=self.cookies, headers=self.headers, connector=connector) as session:
            tasks = []
            for i in range(CONCURRENT_REQUESTS):
                task = asyncio.create_task(self.worker(queue, session))
                tasks.append(task)
            await queue.join()
            for task in tasks: task.cancel()
        
        return self.results

class SlackNotifier:
    def __init__(self):
        self.webhook_url = os.environ.get("SLACK_WEBHOOK")

    async def send_notification(self, new_sites):
        if not self.webhook_url: return
        try:
            message_text = f"🚨 *{len(new_sites)} Yeni Site Bulundu!* 🚨\n\n"
            for site in new_sites:
                message_text += f"• *{site[1]}* (ID: {site[0]})\n  <{site[2]}|Prisync Linki>\n"
            
            connector = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=connector) as session:
                await session.post(self.webhook_url, json={"text": message_text})
                logger.info("Slack notification sent.")
        except Exception as e:
            logger.error(f"Slack error: {e}")

async def main():
    # 1. Google Sheets Başlat
    sheets_manager = GoogleSheetsManager(SPREADSHEET_NAME)
    try:
        sheets_manager.connect()
    except Exception:
        return 

    # 2. Scrape İşlemi
    logger.info("Starting scrape cycle...")
    scraper = AsyncScraper()
    found_sites = await scraper.run()
    logger.info(f"Scrape finished. Found {len(found_sites)} candidates.")

    # 3. Filtreleme ve Kayıt
    new_unique_sites = []
    if found_sites:
        found_sites.sort(key=lambda x: int(x['ID']) if str(x['ID']).isdigit() else 0, reverse=True)
        rows_to_add = []
        for item in found_sites:
            site_id = str(item['ID']).strip()
            
            # --- GÜVENLİ KONTROL ---
            if site_id not in sheets_manager.existing_ids:
                link = f"https://prisync.me/admin/fetchField/site?site_id={site_id}"
                row = [site_id, item['Site'], link]
                rows_to_add.append(row)
                sheets_manager.existing_ids.add(site_id) # Set'e ekle ki aynı turda tekrar eklemesin
        
        if rows_to_add:
            try:
                sheets_manager.sheet.append_rows(rows_to_add)
                new_unique_sites = rows_to_add
                logger.info(f"Added {len(rows_to_add)} new sites.")
            except Exception as e:
                logger.error(f"Sheets error: {e}")

    # 4. Slack Bildirimi
    if new_unique_sites:
        slack = SlackNotifier()
        await slack.send_notification(new_unique_sites)

if __name__ == "__main__":
    asyncio.run(main())
