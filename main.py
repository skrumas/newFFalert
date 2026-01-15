import os
import asyncio
import aiohttp
import json
import logging
import gspread
import pandas as pd
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from bs4 import BeautifulSoup
from google.oauth2.service_account import Credentials

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
        except Exception as e:
            logger.critical(f"Failed to connect to Google Sheets: {e}")
            raise

    def get_existing_data(self):
        """Mevcut veriyi Pandas DataFrame olarak çeker ve temizler"""
        try:
            # Sayfadaki her şeyi DataFrame olarak al
            df = get_as_dataframe(self.sheet, evaluate_formulas=True)
            
            # Tamamen boş satırları sil
            df = df.dropna(how='all')
            
            # 'site_id' sütunu yoksa oluştur (boş df ise)
            if 'site_id' not in df.columns:
                df = pd.DataFrame(columns=['site_id', 'URL', 'ff_site'])
            
            # ID'leri stringe çevir ve ".0" gibi küsuratları temizle
            df['site_id'] = df['site_id'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
            
            # "site_id" yazan başlık satırı veriye karıştıysa temizle
            df = df[df['site_id'] != 'site_id']
            
            # Kopya kayıtları sil (Eskiden kalanlar varsa temizlenir)
            df = df.drop_duplicates(subset=['site_id'], keep='first')
            
            logger.info(f"Loaded {len(df)} unique rows from sheet.")
            return df
        except Exception as e:
            logger.error(f"Error reading sheet: {e}")
            return pd.DataFrame(columns=['site_id', 'URL', 'ff_site'])

    def overwrite_sheet(self, df_final):
        """Temizlenmiş ve birleştirilmiş veriyi sayfaya yazar"""
        try:
            # Yazmadan önce tekrar son bir duplicate kontrolü yap
            df_final = df_final.drop_duplicates(subset=['site_id'], keep='first')
            
            # ID'lere göre sırala (En yeni en üstte veya ID'ye göre)
            # İstersen burayı kapatabilirsin, şu an ID'si büyük olanı en üste alır.
            # Sayısal sıralama için geçici çevirim
            df_final['temp_id'] = pd.to_numeric(df_final['site_id'], errors='coerce')
            df_final = df_final.sort_values(by='temp_id', ascending=False).drop(columns=['temp_id'])
            
            # Sayfayı temizle
            self.sheet.clear()
            
            # Yeni veriyi yaz
            set_with_dataframe(self.sheet, df_final)
            logger.info(f"Successfully overwrote sheet with {len(df_final)} unique rows.")
        except Exception as e:
            logger.error(f"Error overwriting sheet: {e}")

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
                    if not seller_name: 
                        site_id = cols[0].get_text(strip=True)
                        site_url = cols[1].get_text(strip=True)
                        data.append({"site_id": site_id, "URL": site_url})
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

    async def send_notification(self, new_sites_df):
        if not self.webhook_url or new_sites_df.empty: return
        try:
            # Sadece ilk 15 siteyi bildir (Spam olmaması için)
            sites_to_show = new_sites_df.head(15)
            count = len(new_sites_df)
            
            message_text = f"🚨 *{count} Yeni Site Bulundu!* 🚨\n\n"
            for _, row in sites_to_show.iterrows():
                message_text += f"• *{row['URL']}* (ID: {row['site_id']})\n  <{row['ff_site']}|Prisync Linki>\n"
            
            if count > 15:
                message_text += f"\n... ve {count - 15} site daha."

            connector = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=connector) as session:
                await session.post(self.webhook_url, json={"text": message_text})
                logger.info("Slack notification sent.")
        except Exception as e:
            logger.error(f"Slack error: {e}")

async def main():
    # 1. Google Sheets Bağlantısı
    sheets_manager = GoogleSheetsManager(SPREADSHEET_NAME)
    try:
        sheets_manager.connect()
    except Exception:
        return 

    # 2. Mevcut Veriyi Çek ve Temizle (Eski kopyaları da temizler)
    df_existing = sheets_manager.get_existing_data()
    existing_ids_set = set(df_existing['site_id'].tolist())

    # 3. Scrape İşlemi
    logger.info("Starting scrape cycle...")
    scraper = AsyncScraper()
    found_sites_list = await scraper.run() # List of dicts
    logger.info(f"Scrape finished. Found {len(found_sites_list)} candidates.")

    # 4. Yeni Veriyi DataFrame'e Çevir
    if not found_sites_list:
        logger.info("No sites found in scrape.")
        return

    df_found = pd.DataFrame(found_sites_list)
    # Link sütununu oluştur
    df_found['ff_site'] = df_found['site_id'].apply(lambda x: f"https://prisync.me/admin/fetchField/site?site_id={x}")
    
    # 5. Sadece GERÇEKTEN yeni olanları ayıkla (Slack için)
    # df_found içindeki ID'lerden, existing_ids_set içinde OLMAYANLARI bul
    df_new_unique = df_found[~df_found['site_id'].isin(existing_ids_set)].copy()
    
    # Kendi içinde duplicate varsa temizle (aynı turda 2 kere çekildiyse)
    df_new_unique = df_new_unique.drop_duplicates(subset=['site_id'])

    if not df_new_unique.empty:
        logger.info(f"Found {len(df_new_unique)} TRULY new sites.")
        
        # 6. Eskilerle Yenileri Birleştir
        df_final = pd.concat([df_existing, df_new_unique], ignore_index=True)
        
        # 7. Sayfayı tamamen sil ve yeniden yaz (Overwrite)
        sheets_manager.overwrite_sheet(df_final)
        
        # 8. Slack Bildirimi
        slack = SlackNotifier()
        await slack.send_notification(df_new_unique)
    else:
        # Eğer yeni site yoksa ama eskilerde duplicate varsa temizlemek için yine de yazabilirsin
        # İsteğe bağlı: sheets_manager.overwrite_sheet(df_existing)
        logger.info("No new unique sites to add.")

if __name__ == "__main__":
    asyncio.run(main())
