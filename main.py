import requests
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import json
import os
from datetime import datetime, timedelta
import sys

# --- FIREBASE BAĞLANTISI ---
# GitHub Secret'tan gelen anahtarı kullanır
firebase_creds_str = os.environ.get('FIREBASE_CREDENTIALS')

if firebase_creds_str:
    try:
        cred_dict = json.loads(firebase_creds_str)
        cred = credentials.Certificate(cred_dict)
        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred)
        db = firestore.client()
    except Exception as e:
        print(f"Firebase Bağlantı Hatası: {e}")
        sys.exit(1)
else:
    # Lokal test için
    if os.path.exists("serviceAccountKey.json"):
        cred = credentials.Certificate("serviceAccountKey.json")
        if not firebase_admin._apps:
             firebase_admin.initialize_app(cred)
        db = firestore.client()
    else:
        print("HATA: Firebase anahtarı bulunamadı.")
        sys.exit(1)

def get_tefas_data_with_change():
    print("TEFAS API'sine bağlanılıyor (Değişim Analizi Modu)...")
    
    url = "https://www.tefas.gov.tr/api/DB/BindHistoryInfo"
    
    # Son 10 günün verisini alalım (Tatilleri atlamak için geniş tutuyoruz)
    today = datetime.now()
    start_date = today - timedelta(days=10)
    
    payload = {
        "fontip": "YAT",
        "sfontip": "",
        "bastarih": start_date.strftime("%d.%m.%Y"),
        "bittarih": today.strftime("%d.%m.%Y"),
        "fonkod": ""
    }
    
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
                print(f"API'den {len(data)} satır veri çekildi. Analiz başlıyor...")
                df = pd.DataFrame(data)
                
                # Tarih sütununu düzelt (Unix Timestamp -> Datetime)
                # Düzeltme: Önce sayısal değere çevir, sonra tarihe dönüştür (FutureWarning giderildi)
                if 'TARIH' in df.columns:
                    df['tarih_dt'] = pd.to_datetime(pd.to_numeric(df['TARIH']), unit='ms')
                else:
                    print("HATA: Tarih sütunu bulunamadı.")
                    return None

                # Veriyi Fon Kodu ve Tarihe göre sırala
                df = df.sort_values(by=['FONKODU', 'tarih_dt'])
                
                # --- GÜNLÜK DEĞİŞİM HESAPLAMA SİHRİ ---
                # Her fon grubu içinde 'shift' yaparak bir önceki günün fiyatını yanına getiriyoruz
                df['onceki_fiyat'] = df.groupby('FONKODU')['FIYAT'].shift(1)
                
                # Yüzdelik Değişim Formülü: ((Yeni - Eski) / Eski) * 100
                df['gunluk_degisim'] = ((df['FIYAT'] - df['onceki_fiyat']) / df['onceki_fiyat']) * 100
                
                # Değişim verisi olmayanları (ilk gün verisi) 0 yap
                df['gunluk_degisim'] = df['gunluk_degisim'].fillna(0.0)
                
                # Sadece EN GÜNCEL tarihi al (Her fonun son durumu)
                # Her fon grubu için son satırı alıyoruz
                df_latest = df.groupby('FONKODU').tail(1).copy()
                
                print(f"Analiz Tamamlandı. Güncel Fon Sayısı: {len(df_latest)}")
                return df_latest
            else:
                print("API boş veri döndürdü.")
                return None
        else:
            print(f"API Hatası: Kod {response.status_code}")
            return None

    except Exception as e:
        print(f"Hata: {e}")
        return None

def upload_to_firestore(df):
    collection_name = "fonlar"
    print("Firebase'e yükleniyor...")
    
    batch = db.batch()
    count = 0
    records = df.to_dict(orient='records')
    
    for item in records:
        fon_kodu = item.get('FONKODU')
        
        if fon_kodu:
            doc_ref = db.collection(collection_name).document(fon_kodu)
            
            # Kaydedilecek Temiz Veri Paketi
            kayit = {
                'kod': item.get('FONKODU'),
                'ad': item.get('FONUNADI'),
                'fiyat': float(item.get('FIYAT', 0)),
                'tarih': item.get('tarih_dt'),
                # Yüzde değişim verisi (Virgülden sonra 2 basamak yuvarla)
                'degisim': round(float(item.get('gunluk_degisim', 0)), 2),
                'son_guncelleme': firestore.SERVER_TIMESTAMP
            }
            
            batch.set(doc_ref, kayit)
            count += 1
            
            if count % 400 == 0:
                batch.commit()
                batch = db.batch()
                print(f"{count} fon işlendi...")
                
    batch.commit()
    print(f"BAŞARILI: Toplam {count} fon (Değişim oranlarıyla) kaydedildi! 🚀")

if __name__ == "__main__":
    df = get_tefas_data_with_change()
    if df is not None:
        upload_to_firestore(df)
    else:
        sys.exit(1)
