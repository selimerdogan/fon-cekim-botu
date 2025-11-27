import requests
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta
import os
import json

# --- 1. TEFAS'TAN VERİ ÇEKME ---
def get_tefas_data():
    url = "https://www.tefas.gov.tr/api/DB/BindComparisonFundReport"
    
    # Hafta içi akşam çalışacağı için bugünün tarihini alıyoruz
    # Eğer gece yarısından sonra (01:00 gibi) çalıştıracaksan timedelta(days=1) yapmalısın.
    date = datetime.now().strftime("%d.%m.%Y")
    
    headers = {
        "Referer": "https://www.tefas.gov.tr/FonKarsilastirma.aspx",
        "X-Requested-With": "XMLHttpRequest",
    }
    
    payload = {
        "calismatipi": "2",
        "fontip": "YAT",
        "sfontip": "IYF",
        "sonuctip": "MO",
        "bastarih": date,
        "bittarih": date,
        "strperiod": "1,1,1,1,1,1,1"
    }
    
    try:
        print(f"TEFAS'tan veri çekiliyor... ({date})")
        response = requests.post(url, data=payload, headers=headers)
        result = response.json()
        
        if 'data' in result and result['data']:
            df = pd.DataFrame(result['data'])
            
            # Veri Temizliği
            df = df[['FONKODU', 'FONUNVANI', 'FIYAT']]
            df.columns = ['kod', 'ad', 'fiyat']
            
            # Fiyatı sayıya çevir
            df['fiyat'] = df['fiyat'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            df['fiyat'] = pd.to_numeric(df['fiyat'])
            
            # Tarih ekle
            df['tarih'] = date
            
            return df.to_dict(orient='records')
        else:
            print("Veri boş döndü.")
            return []
    except Exception as e:
        print(f"Hata: {e}")
        return []

# --- 2. FIREBASE'E YAZMA ---
def upload_to_firebase(data):
    if not data:
        return

    # Secret'tan credentials okuma
    cred_json = json.loads(os.environ.get('FIREBASE_CREDENTIALS'))
    cred = credentials.Certificate(cred_json)
    firebase_admin.initialize_app(cred)
    
    db = firestore.client()
    collection_name = "fonlar" # Firebase'deki koleksiyon adı
    
    print(f"{len(data)} adet fon Firebase'e yazılıyor...")
    
    batch = db.batch()
    counter = 0
    
    for item in data:
        # Belge ID'si fon kodu olsun (örn: "AFT"). Böylece hep güncellenir, duplicate olmaz.
        doc_ref = db.collection(collection_name).document(item['kod'])
        batch.set(doc_ref, item)
        counter += 1
        
        # Firestore batch limiti 500'dür. 400'de bir commit edelim.
        if counter % 400 == 0:
            batch.commit()
            batch = db.batch()
            print(f"{counter} kayıt işlendi...")
            
    # Kalanları gönder
    batch.commit()
    print("Yazma işlemi tamamlandı.")

if __name__ == "__main__":
    fon_verileri = get_tefas_data()
    if fon_verileri:
        upload_to_firebase(fon_verileri)
    else:
        print("İşlenecek veri bulunamadı.")