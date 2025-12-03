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

# --- GÜNCELLENEN KISIM BAŞLANGIÇ ---
# Dosya aramak yerine doğrudan GitHub Secret'ı okuyoruz
firebase_creds_str = os.environ.get('FIREBASE_CREDENTIALS')

if firebase_creds_str:
    # Gelen string'i JSON objesine çevir
    cred_dict = json.loads(firebase_creds_str)
    cred = credentials.Certificate(cred_dict)
    
    # Firebase'i başlat (Zaten başlatılmadıysa)
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    
    db = firestore.client()
else:
    # Eğer lokal bilgisayarınızda test ediyorsanız buraya düşebilir
    # Lokal test için manuel dosya yolu:
    if os.path.exists("serviceAccountKey.json"):
        cred = credentials.Certificate("serviceAccountKey.json")
        if not firebase_admin._apps:
             firebase_admin.initialize_app(cred)
        db = firestore.client()
    else:
        print("KRİTİK HATA: Firebase kimlik bilgileri bulunamadı!")
        print("GitHub'da iseniz 'FIREBASE_CREDENTIALS' secret'ının tanımlı olduğundan emin olun.")
        exit(1)
# --- GÜNCELLENEN KISIM BİTİŞ ---

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
        
        # Pandas read_html uyarısını engellemek için sarmalama
        tables = pd.read_html(StringIO(html))
        
        if tables:
            df = tables[0]
            # Veri temizliği: NaN değerleri None yap (Firestore için)
            df = df.where(pd.notnull(df), None)
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
    print(f"{len(df)} adet fon Firestore'a yükleniyor...")
    
    records = df.to_dict(orient='records')
    
    # İlerlemeyi görmek için basit sayaç
    count = 0
    for item in records:
        fon_kodu = item.get('Kod')
        if fon_kodu:
            doc_ref = db.collection(collection_name).document(fon_kodu)
            item['guncellenme_tarihi'] = firestore.SERVER_TIMESTAMP
            doc_ref.set(item)
            count += 1
            
    print(f"İşlem Tamamlandı: {count} fon güncellendi.")

if __name__ == "__main__":
    df_funds = get_fintables_funds()
    
    if df_funds is not None:
        upload_to_firestore(df_funds)
    else:
        print("Veri çekilemediği için yükleme yapılmadı.")
        exit(1)
