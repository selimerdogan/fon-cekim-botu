from tefas import Crawler
import pandas as pd
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import json
import os
from datetime import datetime, timedelta

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
        print("HATA: Firebase anahtarı bulunamadı.")
        exit(1)

def get_tefas_data():
    print("TEFAS veritabanına bağlanılıyor...")
    
    crawler = Crawler()
    
    # Bugünü ve dünü al (Hafta sonu ise son iş gününü bulmak gerekir ama kütüphane genelde son veriyi verir)
    # Garanti olsun diye son 3-4 günlük veri isteyelim, kütüphane en güncelini getirir.
    start_date = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
    
    try:
        # fetch fonksiyonu tüm fonları getirir
        # columns parametresi ile sadece istediklerimizi alabiliriz
        df = crawler.fetch(start=start_date)
        
        # Sadece en son tarihe ait verileri alalım (Bugünün verisi)
        if not df.empty:
            last_date = df['date'].max()
            df = df[df['date'] == last_date]
            print(f"Veri Tarihi: {last_date}")
            print(f"Çekilen Fon Sayısı: {len(df)}")
            return df
        else:
            return None

    except Exception as e:
        print(f"TEFAS Hatası: {e}")
        return None

def upload_to_firestore(df):
    collection_name = "fonlar"
    print("Firebase'e yükleniyor...")
    
    batch = db.batch()
    count = 0
    
    # DataFrame'i sözlük listesine çevir
    records = df.to_dict(orient='records')
    
    for item in records:
        # TEFAS Kütüphanesinden gelen sütun adları şöyledir:
        # code, title, price, code vb.
        fon_kodu = item.get('code')
        
        if fon_kodu:
            # Firestore'a uygun formata getir
            doc_ref = db.collection(collection_name).document(fon_kodu)
            
            # Timestamp ekle
            item['guncellenme_tarihi'] = firestore.SERVER_TIMESTAMP
            
            # Tarih objelerini string'e çevir (Firestore hata vermesin)
            if 'date' in item:
                item['date'] = str(item['date'])
                
            batch.set(doc_ref, item)
            count += 1
            
            if count % 400 == 0:
                batch.commit()
                batch = db.batch()
                print(f"{count} fon yüklendi...")
                
    batch.commit()
    print(f"BAŞARILI: Toplam {count} fon güncellendi.")

if __name__ == "__main__":
    df = get_tefas_data()
    if df is not None:
        upload_to_firestore(df)
    else:
        print("Veri alınamadı.")
