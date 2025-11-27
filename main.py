from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import os
import json
import time
import io

def get_data_with_selenium_pandas():
    print("Selenium ile Bigpara'ya gidiliyor...")
    
    # Chrome Ayarları
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    try:
        url = "https://bigpara.hurriyet.com.tr/yatirim-fonlari/tum-fon-verileri/"
        driver.get(url)
        
        print("Sayfa açıldı, verilerin yüklenmesi bekleniyor (10sn)...")
        time.sleep(10) # JavaScript'in tabloyu doldurması için bekleme süresi
        
        # Sayfanın o anki dolu halini al
        page_source = driver.page_source
        
        # Pandas ile HTML içindeki tabloları tara
        print("Tablo okunuyor...")
        # io.StringIO kullanarak bellekten okuyoruz
        dfs = pd.read_html(io.StringIO(page_source), thousands='.', decimal=',')
        
        if not dfs:
            print("HATA: Sayfada hiç tablo bulunamadı.")
            return None, None
            
        # Genelde ilk tablo fon tablosudur
        df = dfs[0]
        
        # Sütunları küçük harfe çevirip kontrol et
        df.columns = [str(c).lower() for c in df.columns]
        
        # 'kod' ve 'fiyat' (veya son) içeren sütunları bul
        code_col = next((c for c in df.columns if 'kod' in c), None)
        # Bigpara'da fiyat sütunu genelde 'son' veya 'fiyat' adındadır
        price_col = next((c for c in df.columns if 'son' in c or 'fiyat' in c), None)
        
        if not code_col or not price_col:
            # Bulamazsak 1. ve 3. sütunu varsayalım (Bigpara standardı)
            code_col = df.columns[0]
            price_col = df.columns[2]
            
        print(f"Sütunlar bulundu -> Kod: {code_col}, Fiyat: {price_col}")
        
        # Temizleme
        df = df[[code_col, price_col]].copy()
        df.columns = ['FONKODU', 'FIYAT']
        
        # Veri tiplerini düzelt
        df['FONKODU'] = df['FONKODU'].astype(str).str.strip()
        df['FIYAT'] = pd.to_numeric(df['FIYAT'], errors='coerce')
        df = df.dropna(subset=['FIYAT'])
        
        fund_dict = dict(zip(df['FONKODU'], df['FIYAT']))
        
        today = datetime.now()
        doc_date_str = today.strftime("%Y-%m-%d")
        
        return fund_dict, doc_date_str

    except Exception as e:
        print(f"Selenium Hatası: {e}")
        return None, None
    finally:
        driver.quit()

def save_history_to_firebase(fund_data, doc_date):
    if not fund_data:
        return

    try:
        cred_json = json.loads(os.environ.get('FIREBASE_CREDENTIALS'))
        cred = credentials.Certificate(cred_json)
        # Initialize kontrolü
        try:
            firebase_admin.get_app()
        except ValueError:
            firebase_admin.initialize_app(cred)
        
        db = firestore.client()
        doc_ref = db.collection('fund_history').document(doc_date)
        
        data_payload = {
            "date": doc_date,
            "createdAt": firestore.SERVER_TIMESTAMP,
            "closing": fund_data
        }
        
        doc_ref.set(data_payload, merge=True)
        print(f"BAŞARILI: '{doc_date}' tarihine {len(fund_data)} adet fon kaydedildi.")
        
    except Exception as e:
        print(f"Firebase Hatası: {e}")

if __name__ == "__main__":
    data, date_id = get_data_with_selenium_pandas()
    if data:
        save_history_to_firebase(data, date_id)
    else:
        print("İşlem başarısız.")
        exit(1)
