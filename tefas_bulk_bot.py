import requests
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

def get_tefas_data_direct():
    print("TEFAS API'sine bağlanılıyor...")
    
    # TEFAS'ın resmi API adresi
    url = "https://www.tefas.gov.tr/api/DB/BindHistoryInfo"
    
    # Tarih Ayarı: Garanti olsun diye son 7 günü tarayalım
    # TEFAS tarih formatı: "dd.mm.yyyy" (Örn: 03.12.2025)
    today = datetime.now()
    start_date = today - timedelta(days=7)
    
    date_fmt = "%d.%m.%Y"
    
    # API'ye gönderilecek "Mektup" (Payload)
    payload = {
        "fontip": "YAT", # Yatırım Fonları
        "sfontip": "",
        "bastarih": start_date.strftime(date_fmt),
        "bittarih": today.strftime(date_fmt),
        "fonkod": "" # Boş bırakırsak hepsini getirir
    }
    
    # Kendimizi tarayıcı gibi tanıtacak başlıklar (Headers)
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Origin": "https://www.tefas.gov.tr",
        "Referer": "https://www.tefas.gov.tr/TarihselVeriler.aspx"
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        
        if response.status_code == 200:
            result = response.json()
            data = result.get('data', [])
            
            if data:
                print(f"API Yanıt Verdi! Toplam {len(data)} satır veri çekildi.")
                
                # JSON verisini DataFrame'e çevir
                df = pd.DataFrame(data)
                
                # Sütun İsimleri TEFAS'tan şöyle gelir: 
                # FONKODU, FONUNADI, FIYAT, TARIH vb.
                
                # En son tarihe ait verileri filtreleyelim
                # Tarih sütunu UNIX timestamp veya string gelebilir, kontrol edelim.
                # Genelde 'TARIH' alanı epoch (sayı) olarak gelir.
                
                if 'TARIH' in df.columns:
                    # En büyük (en yeni) tarihi bul
                    max_date = df['TARIH'].max()
                    df_latest = df[df['TARIH'] == max_date].copy()
                    
                    # Tarihi okunabilir formata çevir (Opsiyonel)
                    # TEFAS epoch formatı genelde milisaniyedir (/1000 gerekebilir)
                    
                    print(f"Filtreleme Sonrası Güncel Fon Sayısı: {len(df_latest)}")
                    return df_latest
                else:
                    print("UYARI: Tarih sütunu bulunamadı, tüm veri dönülüyor.")
                    return df
            else:
                print("API boş veri döndürdü.")
                return None
        else:
            print(f"API Hatası: Kod {response.status_code}")
            return None

    except Exception as e:
        print(f"Bağlantı Hatası: {e}")
        return None

def upload_to_firestore(df):
    collection_name = "fonlar"
    print("Firebase'e yükleme başlıyor...")
    
    batch = db.batch()
    count = 0
    records = df.to_dict(orient='records')
    
    for item in records:
        # TEFAS API'sinden gelen anahtar isimleri BÜYÜK HARFLİDİR (FONKODU, FIYAT vb.)
        fon_kodu = item.get('FONKODU')
        
        if fon_kodu:
            doc_ref = db.collection(collection_name).document(fon_kodu)
            
            # Veri tiplerini düzeltelim (Firestore uyumu için)
            item['guncellenme_tarihi'] = firestore.SERVER_TIMESTAMP
            
            # Tüm sayısal olmayan değerleri string yapalım ki hata çıkmasın
            for key, val in item.items():
                if val is None:
                    item[key] = ""
                # Tarih epoch ise dokunmayalım, okunabilir olsun derseniz çevirebiliriz
            
            batch.set(doc_ref, item)
            count += 1
            
            if count % 400 == 0:
                batch.commit()
                batch = db.batch()
                print(f"{count} fon işlendi...")
                
    batch.commit()
    print(f"BAŞARILI: Toplam {count} fon veritabanına yazıldı! 🚀")

if __name__ == "__main__":
    df = get_tefas_data_direct()
    if df is not None:
        upload_to_firestore(d
