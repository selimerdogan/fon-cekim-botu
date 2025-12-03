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
            
            # --- KRİTİK DÜZELTME BAŞLANGIÇ ---
            # 1. Eğer başlıklar MultiIndex (Tuple) ise düzleştir
            if isinstance(df.columns, pd.MultiIndex):
                # Sadece son seviyedeki başlığı al (Örn: ('#', 'Kod') -> 'Kod')
                df.columns = [str(col[-1]).strip() for col in df.columns]
            else:
                df.columns = [str(col).strip() for col in df.columns]
            
            # 2. Tüm verileri string'e çevir (Firestore hatasını önler)
            df = df.astype(str)
            # --- KRİTİK DÜZELTME BİTİŞ ---
            
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
    
    # Tabloyu kontrol edelim
    print("-" * 30)
    print(f"Düzeltilmiş Sütunlar: {df.columns.tolist()}")
    print("İlk satır verisi:", df.iloc[0].tolist())
    print("-" * 30)

    # FON KODU HANGİ SÜTUNDA?
    # Genelde 2. sütun (Index 1) kod olur (AAK, AAL...). 
    # İlk sütun (Index 0) genelde sıra numarasıdır (1, 2, 3...).
    target_col = df.columns[0] # Varsayılan
    
    # Eğer "Kod" isminde bir sütun varsa onu kullan, yoksa mantıksal arama yap
    kod_cols = [c for c in df.columns if "Kod" in c or "Code" in c]
    if kod_cols:
        target_col = kod_cols[0]
        print(f"BULDUM: Fon Kodları '{target_col}' sütunundan alınacak.")
    else:
        # İsimle bulamadıysak 2. sütuna bakalım (Genelde kod oradadır)
        if len(df.columns) > 1:
            target_col = df.columns[1]
            print(f"OTOMATİK: İsim bulunamadı, 2. sütun ('{target_col}') kod olarak seçildi.")

    records = df.to_dict(orient='records')
    count = 0
    batch = db.batch()
    
    for item in records:
        raw_code = item.get(target_col)
        
        # Kod geçerli mi kontrol et (Boş değilse ve 3 harfliyse genelde fondur)
        if raw_code and raw_code.lower() not in ['nan', 'none', '']:
            # "/" işaretini temizle (Firestore ID kuralları)
            fon_kodu = str(raw_code).strip().replace('/', '-')
            
            # Gereksiz sayısal index sütununu veritabanına kaydetmeye gerek yok
            # (Opsiyonel temizlik)
            
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
