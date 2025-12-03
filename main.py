import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager
from io import StringIO
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import json
import os

# --- FIREBASE BAĞLANTISI ---
firebase_creds_str = os.environ.get('FIREBASE_CREDENTIALS')

if firebase_creds_str:
    cred_dict = json.loads(firebase_creds_str)
    cred = credentials.Certificate(cred_dict)
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    db = firestore.client()
else:
    if os.path.exists("serviceAccountKey.json"):
        cred = credentials.Certificate("serviceAccountKey.json")
        if not firebase_admin._apps:
             firebase_admin.initialize_app(cred)
        db = firestore.client()
    else:
        print("KRİTİK HATA: Firebase şifresi bulunamadı!")
        exit(1)

def get_fintables_funds():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        url = "https://fintables.com/fonlar/getiri"
        print("Fintables'a gidiliyor...")
        driver.get(url)
        time.sleep(5)

        # --- 1. ADIM: ÇEREZ UYARISINI KAPAT ---
        try:
            cookie_buttons = driver.find_elements(By.XPATH, "//button[contains(text(), 'Kabul Et') or contains(text(), 'Tamam') or contains(text(), 'Anladım')]")
            if cookie_buttons:
                print("Çerez uyarısı kapatılıyor...")
                driver.execute_script("arguments[0].click();", cookie_buttons[0])
                time.sleep(2)
        except:
            pass

        # --- 2. ADIM: SAYFAYI GENİŞLET (TÜMÜNÜ GÖSTER) ---
        print("Tablo genişletilmeye çalışılıyor...")
        try:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            selects = driver.find_elements(By.TAG_NAME, "select")
            paginator_found = False
            
            for select_element in selects:
                options = select_element.find_elements(By.TAG_NAME, "option")
                if len(options) > 0:
                    option_texts = [opt.text for opt in options]
                    print(f"Dropdown bulundu: {option_texts}")
                    
                    # En son seçeneği (Genelde 'Tümü' veya en büyük sayı) seç
                    driver.execute_script("arguments[0].style.display = 'block';", select_element)
                    select_object = Select(select_element)
                    select_object.select_by_index(len(options) - 1)
                    
                    paginator_found = True
                    print("En geniş görünüm seçildi. Tablo güncelleniyor...")
                    time.sleep(10)
                    break
            
            if not paginator_found:
                print("UYARI: Sayfalama kutusu (Select) bulunamadı.")

        except Exception as e:
            print(f"Genişletme Hatası: {e}")

        # -----------------------------

        html = driver.page_source
        tables = pd.read_html(StringIO(html))
        
        if tables:
            df = tables[0]
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = [str(col[-1]).strip() for col in df.columns]
            else:
                df.columns = [str(col).strip() for col in df.columns]
            
            df = df.astype(str)
            return df
        else:
            return None

    except Exception as e:
        print(f"Scrape Hatası: {e}")
        return None
    finally:
        driver.quit()

def upload_to_firestore(df):
    collection_name = "fonlar"
    
    print("-" * 30)
    print(f"TOPLAM ÇEKİLEN FON SAYISI: {len(df)}")
    print("-" * 30)
    
    target_col = df.columns[0]
    kod_cols = [c for c in df.columns if "Kod" in c or "Code" in c]
    if kod_cols:
        target_col = kod_cols[0]
    elif len(df.columns) > 1:
        target_col = df.columns[1]

    records = df.to_dict(orient='records')
    count = 0
    batch = db.batch()
    
    for item in records:
        raw_code = item.get(target_col)
        
        # HATA ALINAN SATIR BURASIYDI - DÜZELTİLDİ
        if raw_code and str(raw_code).lower() not in ['nan', 'none', '']:
            fon_kodu = str(raw_code).strip().replace('/', '-')
            
            doc_ref = db.collection(collection_name).document(fon_kodu)
            item['guncellenme_tarihi'] = firestore.SERVER_TIMESTAMP
            
            batch.set(doc_ref, item)
            count += 1
            
            if count % 400 == 0:
                batch.commit()
                batch = db.batch()
                print(f"{count} fon işlendi...")

    batch.commit()
    print(f"BAŞARILI: Toplam {count} fon Firebase'e yüklendi!")

if __name__ == "__main__":
    df_funds = get_fintables_funds()
    
    if df_funds is not None:
        upload_to_firestore(df_funds)
    else:
        print("HATA: Veri boş geldi.")
        exit(1)
