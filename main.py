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
            json_creds = json.loads(os.environ["GSPREAD_JSON"])
            creds = Credentials.from_service_account_info(json_creds, scopes=scopes)
            self.client = gspread.authorize(creds)
            self.sheet = self.client.open(self.spreadsheet_name).get_worksheet(0)
            logger.info("Connected to Google Sheets successfully.")
            
            # Başlık kontrolü (Sadece yoksa ekler, varsa dokunmaz)
            self._ensure_headers()
            
            # Mevcut ID'leri yükle
            self._load_existing_ids()
        except Exception as e:
            logger.critical(f"Failed to connect to Google Sheets: {e}")
            raise

    def _ensure_headers(self):
        try:
            headers = self.sheet.row_values(1)
            # Eğer sayfa boşsa başlık ekle, değilse dokunma
            if not headers:
                logger.info("Sheet is empty. Adding headers...")
                # Checkbox sütunu için başlık da ekleyelim
                self.sheet.insert_row(["site_id", "URL", "ff_site", "done or not"], index=1)
        except Exception as e:
            logger.error(f"Error checking headers: {e}")

    def _load_existing_ids(self):
        """
        MEVCUT DÜZENİ BOZMADAN OKUMA:
        Sadece A sütunundaki ID'leri okur. Senin Checkboxlarına dokunmaz.
        Regex kullanarak "361564.0", "361564" veya metin formatındaki ID'leri
        tek bir formata (saf sayı stringi) çevirip hafızaya alır.
        """
        try:
            # Sadece 1. sütunu (ID) çekiyoruz. Diğer sütunlar (Checkbox vs) umurumuzda değil.
            col_a_values = self.sheet.col_values(1)
            
            # Başlığı atla
            if col_a_values:
                col_a_values = col_a_values[1:]

            count = 0
            for val in col_a_values:
                raw_val = str(val).strip()
                if not raw_val: continue

                # --- REGEX İLE SAF ID ÇIKARMA ---
                # Ne gelirse gelsin (123.0, 123, site_id=123) içindeki sayıyı alır.
                match = re.search(r'(\d+)', raw_val.replace(",", "").replace(".", ""))
                
                if match:
                    # Bulduğu sayıyı hafızaya atar
                    clean_id = match.group(1)
                    # Çok önemli: Prisync ID'leri genelde 9 hanelidir. 
                    # 123.0 gibi durumlarda sondaki 0'ı ID sanmaması için basit bir kontrol:
                    # Ancak regex .replace(".", "") yaptığımız için 1230 olur.
                    # Daha güvenli yöntem: float -> int -> str
                    try:
                        clean_id = str(int(float(raw_val)))
                    except:
                        pass # Regex sonucu zaten clean_id idi
                    
                    self.existing_ids.add(clean_id)
                    count += 1

            logger.info(f"✅ Loaded {len(self.existing_ids)} unique existing IDs (Scanning existing rows).")
        except Exception as e:
            logger.error(f"Error loading existing IDs: {e}")

    def append_new_sites(self, rows_to_add):
        """
        Sadece yeni verileri EN ALTA ekler.
        Mevcut satırlara, checkboxlara ASLA dokunmaz.
        """
        if rows_to_add:
            try:
                # append_rows fonksiyonu mevcut verinin en altına ekleme yapar.
                self.sheet.append_rows(rows_to_add)
                logger.info(f"✅ Appended {len(rows_to_add)} new rows to the bottom.")
            except Exception as e:
                logger.error(f"Error appending rows: {e}")

class AsyncScraper:
    def __init__(self):
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
        except Exception:
            pass
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
                    if not seller_name: 
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

    async def send_notification(self, new_rows):
        if not self.webhook_url or not new_rows: return
        try:
            # new_rows format: [site_id, url, link]
            count = len(new_rows)
            # İlk 10 taneyi göster
            display_rows = new_rows[:10]
            
            message_text = f"🚨 *{count} Yeni Site Bulundu!* (Sona Eklendi) 🚨\n\n"
            for row in display_rows:
                message_text += f"• *{row[1]}* (ID: {row[0]})\n  <{row[2]}|Prisync Linki>\n"
            
            if count > 10:
                message_text += f"\n... ve {count - 10} site daha."

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

    # 3. Filtreleme (Duplicate Killer)
    rows_to_add = []
    
    # Prisync'ten gelenler ID'ye göre tersten (büyükten küçüğe) gelir genelde.
    # Ama biz Sheet'in sonuna ekleyeceğimiz için sıralama çok dert değil.
    # Yine de düzenli olsun diye ID'ye göre sıralayabiliriz.
    found_sites.sort(key=lambda x: int(x['ID']) if str(x['ID']).isdigit() else 0, reverse=False)

    for item in found_sites:
        site_id = str(item['ID']).strip()
        
        # --- KRİTİK KONTROL ---
        # Bot, sadece sheets_manager.existing_ids içinde OLMAYANLARI alır.
        if site_id not in sheets_manager.existing_ids:
            link = f"https://prisync.me/admin/fetchField/site?site_id={site_id}"
            # [ID, URL, Link] formatında satır hazırla.
            # Checkbox sütunu (D) boş kalacak, sen sonra tik atacaksın.
            row = [site_id, item['Site'], link]
            rows_to_add.append(row)
            
            # Aynı döngüde tekrar eklememek için hafızaya da ekle
            sheets_manager.existing_ids.add(site_id)

    # 4. Kaydetme ve Bildirim
    if rows_to_add:
        # Sadece yeni satırları EN ALTA ekle (Mevcutlara dokunma!)
        sheets_manager.append_new_sites(rows_to_add)
        
        # Slack'e bildir
        slack = SlackNotifier()
