import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import json
import os
from datetime import datetime, timedelta
import sys

# --- YENİ KÜTÜPHANE ---
from tefas import Crawler

# --- FIREBASE BAĞLANTISI ---
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
    # Lokal test
    if os.path.exists("serviceAccountKey.json"):
        cred = credentials.Certificate("serviceAccountKey.json")
        if not firebase_admin._apps:
             firebase_admin.initialize_app(cred)
        db = firestore.client()
    else:
        print("HATA: Firebase anahtarı bulunamadı.")
        sys.exit(1)

def run_crawler_bot():
    print("🚀 TEFAS Crawler (Kütüphane) başlatılıyor...")
    
    # 1. Kütüphaneyi Çağır
    crawler = Crawler()
    
    # 2. Tarihleri Ayarla (Son 3 günü çekelim ki değişim hesaplayabilelim)
    today = datetime.now()
    start_date = today - timedelta(days=5) # Hafta sonu riskine karşı 5 gün
    
    date_fmt = "%Y-%m-%d" # Kütüphane genelde bu formatı sever
    
    try:
        # TEFAS'tan veriyi tek satırda çekiyoruz!
        print("📡 Veriler çekiliyor (Bu işlem çok hızlıdır)...")
        df = crawler.fetch(start=start_date.strftime(date_fmt), 
                           end=today.strftime(date_fmt),
                           columns=["code", "date", "price", "title"])
        
        if df is None or df.empty:
            print("❌ Veri bulunamadı.")
            return

        # Sütun isimlerini düzelt (İngilizce gelebilir, standartlaştıralım)
        # Kütüphane genelde: 'code', 'date', 'price', 'title' döndürür.
        
        # Tarih formatını datetime'a çevir
        df['date'] = pd.to_datetime(df['date'])
        
        # Sıralama
        df = df.sort_values(by=['code', 'date'])
        
        # Günlük Değişim Hesabı
        df['onceki_fiyat'] = df.groupby('code')['price'].shift(1)
        df['degisim'] = ((df['price'] - df['onceki_fiyat']) / df['onceki_fiyat']) * 100
        df['degisim'] = df['degisim'].fillna(0.0)
        
        # Her fonun EN GÜNCEL verisini al
        df_latest = df.groupby('code').tail(1).copy()
        
        print(f"✅ Analiz Tamamlandı. {len(df_latest)} fon işleniyor...")
        
        # 3. Firestore Map Formatına Çevir
        fon_map = {}
        records = df_latest.to_dict(orient='records')
        
        for item in records:
            fon_kodu = item['code']
            
            fon_map[fon_kodu] = {
                'fiyat': float(item['price']),
                'degisim': round(float(item['degisim']), 2),
                'ad': item.get('title', ''),
                # Not: Bu kütüphane varsayılan olarak Kişi Sayısı/Büyüklük getirmeyebilir.
                # Eğer getirmezse 0 basarız, sistem bozulmaz.
                'buyukluk': 0, 
                'kisi_sayisi': 0
            }

        # 4. Veritabanına Yaz
        date_str = today.strftime("%Y-%m-%d")
        time_str = today.strftime("%H:%M")

        print(f"💾 Firebase'e yazılıyor: fonlar/{date_str}/snapshots/{time_str}")
        
        # Tarih Dökümanı
        db.collection('fonlar').document(date_str).set({'created_at': firestore.SERVER_TIMESTAMP}, merge=True)
        
        # Saat Dökümanı (Tek Liste)
        target_ref = db.collection('fonlar').document(date_str).collection('snapshots').document(time_str)
        target_ref.set(fon_map)
        
        print(f"🎉 İŞLEM BAŞARILI! {len(fon_map)} fon kaydedildi.")
        
    except Exception as e:
        print(f"🔥 Hata Oluştu: {e}")

if __name__ == "__main__":
    run_crawler_bot()
