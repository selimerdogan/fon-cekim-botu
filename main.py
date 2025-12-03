import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
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
    # Lokal test için
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
        time.sleep(10)

        html = driver.page_source
        tables = pd.read_html(StringIO(html))
        
        if tables:
            df = tables[0]
            # Tüm verileri string (yazı) yapalım ki hata çıkmasın
            df = df.astype(str)
            return df
        else:
            print("HATA: Sayfada tablo bulunamadı!")
            return None

    except Exception as e:
        print(f"Scrape Hatası: {e}")
        return None
    finally:
        driver.quit()

def upload_to_firestore(df):
    collection_name = "fonlar"
    
    # 1. Debug: Tablonun ilk 3 satırını yazdıralım (Sorun varsa görelim)
    print("-" * 30)
    print("ÇEKİLEN VERİ ÖRNEĞİ:")
    print(df.head(3))
    print("-" * 30)

    # İlk sütun her zaman Fon Kodudur, ismine bakmadan direkt onu alıyoruz.
    ilk_sutun_ismi = df.columns[0]
    print(f"Fon Kodları '{ilk_sutun_ismi}' sütunundan okunuyor.")
    
    records = df.to_dict(orient='records')
    count = 0
    
    batch = db.batch() # Hızlı yazma modu
    
    for item in records:
        # Fon kodunu al ve temizle
        raw_code = item.get(ilk_sutun_ismi)
        
        # 'nan', 'None' veya boş değilse işlem yap
        if raw_code and raw_code.lower() not in ['nan', 'none', '']:
            fon_kodu = str(raw_code).strip()
            
            # Belge ID'si olarak kullanacağımız için "/" gibi işaretleri silelim
            doc_id = fon_kodu.replace('/', '-')
            
            doc_ref = db.collection(collection_name).document(doc_id)
            
            # Veriye zaman damgası ekle
            item['guncellenme_tarihi'] = firestore.SERVER_TIMESTAMP
            
            # Batch'e ekle (Toplu gönderme)
            batch.set(doc_ref, item)
            count += 1
            
            # Firebase limiti: Her 400 işlemde bir gönderip batch'i boşaltalım
            if count % 400 == 0:
                batch.commit()
                batch = db.batch()
                print(f"{count} fon yazıldı...")

    # Kalanları gönder
    batch.commit()
    print(f"BAŞARILI: Toplam {count} fon Firebase'e yüklendi!")

if __name__ == "__main__":
    df_funds = get_fintables_funds()
    
    if df_funds is not None:
        upload_to_firestore(df_funds)
    else:
        print("Tablo boş geldiği için işlem iptal edildi.")
        exit(1)
