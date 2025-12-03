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

# 1. Firebase Bağlantısını Kur
# GitHub Action ortamında mıyız kontrol edelim
cred_path = "serviceAccountKey.json"

if os.path.exists(cred_path):
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred)
    db = firestore.client()
else:
    print("Hata: Firebase anahtar dosyası bulunamadı!")
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
        time.sleep(10) # Yükleme beklemesi

        html = driver.page_source
        tables = pd.read_html(StringIO(html))
        
        if tables:
            df = tables[0]
            
            # Veri Temizliği: Firestore sütun adlarında nokta (.) sevmez, onları temizleyelim
            # Ayrıca NaN (boş) değerleri None yapalım ki Firestore hata vermesin
            df = df.where(pd.notnull(df), None)
            
            # Sütun isimlerindeki potansiyel sorunlu karakterleri düzeltelim (opsiyonel)
            # df.columns = [c.replace('.', '') for c in df.columns]

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
    
    batch = db.batch()
    counter = 0
    
    # DataFrame'i sözlük listesine çevir
    records = df.to_dict(orient='records')
    
    for item in records:
        # Fintables tablosundaki 'Kod' sütununu Belge ID'si (Document ID) yapıyoruz.
        # Böylece aynı fon tekrar eklendiğinde üzerine yazar (update eder), çift kayıt oluşmaz.
        fon_kodu = item.get('Kod') 
        
        if fon_kodu:
            doc_ref = db.collection(collection_name).document(fon_kodu)
            # Veriye çekilme zamanı ekleyelim
            item['guncellenme_tarihi'] = firestore.SERVER_TIMESTAMP
            doc_ref.set(item) # .set() varsa ezer, yoksa oluşturur
            
            # Firestore batch limiti 500 işlemdir, burada tek tek yazıyoruz ama
            # daha hızlı olsun derseniz batch mantığı kurulabilir. Şimdilik basit set() yeterli.
            
    print("Yükleme tamamlandı.")

if __name__ == "__main__":
    df_funds = get_fintables_funds()
    
    if df_funds is not None:
        upload_to_firestore(df_funds)
    else:
        print("Veri çekilemediği için yükleme yapılmadı.")
        exit(1)
