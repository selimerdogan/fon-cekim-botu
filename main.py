import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
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
        
        # Sayfanın ilk yüklenmesi için bekle
        time.sleep(5)

        # --- TÜMÜNÜ GÖSTERME HAMLESİ ---
        print("Tablo genişletilmeye çalışılıyor...")
        try:
            # Sayfanın altına in ki elemanlar yüklensin
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            # Fintables'da genellikle sayfalama kısmında "Tümü" seçeneği olur.
            # Metin içeriği "Tümü" olan bir tıklanabilir öğe arıyoruz.
            # Bu genelde bir dropdown içindedir veya direkt butondur.
            
            # Yöntem 1: Direkt "Tümü" yazısını içeren bir elemente tıkla
            tumunu_goster = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, "//*[contains(text(), 'Tümü')]"))
            )
            tumunu_goster.click()
            print("'Tümü' butonuna tıklandı, veriler yükleniyor...")
            
            # Tıkladıktan sonra tablonun büyümesi için zaman verelim
            time.sleep(10)
            
        except Exception as e:
            print(f"UYARI: 'Tümü' butonu bulunamadı veya tıklanamadı. Varsayılan tablo çekilecek. Hata: {e}")
            # B planı: Belki dropdown açmak gerekiyordur, ama şimdilik text araması %90 çalışır.

        # -----------------------------

        html = driver.page_source
        tables = pd.read_html(StringIO(html))
        
        if tables:
            df = tables[0]
            
            # MultiIndex düzeltme
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
    print(f"TOPLAM FON SAYISI: {len(df)}") # Burası artık 27 değil 500+ olmalı
    print("-" * 30)

    # Otomatik sütun bulma
    target_col = df.columns[0]
    kod_cols = [c for c in df.columns if "Kod" in c or "Code" in c]
    if kod_cols:
        target_col = kod_cols[0]
    elif len(df.columns) > 1:
        target_col = df.columns[1]

    records = df.to_dict(orient='records')
    count = 0
    batch = db.batch()
    
    # Firestore limiti batch başına 500 işlemdir. 
    # Büyük veri setlerinde bu önemlidir.
    
    for item in records:
        raw_code = item.get(target_col)
        
        if raw_code and raw_code.lower() not in ['nan', 'none', '']:
            fon_kodu = str(raw_code).strip().replace('/', '-')
            
            doc_ref = db.collection(collection_name).document(fon_kodu)
            item['guncellenme_tarihi'] = firestore.SERVER_TIMESTAMP
            
            batch.set(doc_ref, item)
            count += 1
            
            # Her 400 kayıtta bir gönder
            if count % 400 == 0:
                batch.commit()
                batch = db.batch()
                print(f"{count} fon işlendi...")

    # Kalan son grubu gönder
    batch.commit()
    print(f"BAŞARILI: Toplam {count} fon Firebase'e yüklendi!")

if __name__ == "__main__":
    df_funds = get_fintables_funds()
    
    if df_funds is not None:
        upload_to_firestore(df_funds)
    else:
        print("HATA: Veri boş geldi.")
        exit(1)
