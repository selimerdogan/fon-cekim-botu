import requests
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import json
import os
from datetime import datetime, timedelta
import sys

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
    if os.path.exists("serviceAccountKey.json"):
        cred = credentials.Certificate("serviceAccountKey.json")
        if not firebase_admin._apps:
             firebase_admin.initialize_app(cred)
        db = firestore.client()
    else:
        print("HATA: Firebase anahtarı bulunamadı.")
        sys.exit(1)

def get_all_funds_data():
    """Tüm fonları çeker ve değişim oranlarını hesaplar"""
    print("TEFAS'tan tüm veriler çekiliyor...")
    
    url = "https://www.tefas.gov.tr/api/DB/BindHistoryInfo"
    
    # Değişim hesabı için son 6 günün verisi (Araya hafta sonu girerse diye)
    today = datetime.now()
    start_date = today - timedelta(days=6)
    
    payload = {
        "fontip": "YAT",
        "sfontip": "",
        "bastarih": start_date.strftime("%d.%m.%Y"),
        "bittarih": today.strftime("%d.%m.%Y"),
        "fonkod": "" 
    }
    
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)"
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        data = response.json().get('data', [])
        
        if not data:
            return None
            
        df = pd.DataFrame(data)
        
        # Tarih düzeltme
        if 'TARIH' in df.columns:
            df['tarih_dt'] = pd.to_datetime(pd.to_numeric(df['TARIH']), unit='ms')
        
        # Sıralama ve Değişim Hesabı
        df = df.sort_values(by=['FONKODU', 'tarih_dt'])
        df['onceki_fiyat'] = df.groupby('FONKODU')['FIYAT'].shift(1)
        df['gunluk_degisim'] = ((df['FIYAT'] - df['onceki_fiyat']) / df['onceki_fiyat']) * 100
        df['gunluk_degisim'] = df['gunluk_degisim'].fillna(0.0)
        
        # Her fonun sadece EN SON (Güncel) verisini al
        df_latest = df.groupby('FONKODU').tail(1).copy()
        
        return df_latest
        
    except Exception as e:
        print(f"Veri Çekme Hatası: {e}")
        return None

def save_bulk_snapshot():
    # 1. Veriyi Hazırla
    df = get_all_funds_data()
    if df is None:
        sys.exit(1)
        
    print(f"Toplam {len(df)} adet fon işleniyor...")

    # 2. DataFrame'i Map yapısına çevir (İstediğin yeni alanlarla)
    fon_map = {}
    records = df.to_dict(orient='records')
    
    for item in records:
        fon_kodu = item['FONKODU']
        
        # Güvenli Veri Çekme (None veya boş gelirse 0 yap)
        kisi_sayisi_raw = item.get('KISISAYISI')
        buyukluk_raw = item.get('FONTOPLAMDEGER')

        fon_map[fon_kodu] = {
            'fiyat': float(item.get('FIYAT', 0)),
            'degisim': round(float(item.get('gunluk_degisim', 0)), 2),
            
            # --- YENİ EKLENEN ALANLAR ---
            'ad': item.get('FONUNADI', ''),
            # Kişi sayısı float gelebilir, int'e çeviriyoruz
            'kisi_sayisi': int(float(kisi_sayisi_raw)) if kisi_sayisi_raw else 0,
            # Fon büyüklüğü
            'buyukluk': float(buyukluk_raw) if buyukluk_raw else 0.0
        }

    # 3. Firestore'a Yaz
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M")

    print(f"Yazılıyor: fonlar/{date_str}/snapshots/{time_str}")
    
    try:
        # Tarih dökümanını oluştur
        db.collection('fonlar').document(date_str).set({'created_at': firestore.SERVER_TIMESTAMP}, merge=True)
        
        # Saat dökümanına tüm haritayı bas
        target_ref = db.collection('fonlar').document(date_str).collection('snapshots').document(time_str)
        target_ref.set(fon_map)
        
        print(f"✅ BAŞARILI! {len(fon_map)} fon (Ad, Büyüklük, Kişi Sayısı ile) kaydedildi.")
        
    except Exception as e:
        print(f"🔥 Yazma Hatası: {e}")

if __name__ == "__main__":
    save_bulk_snapshot()
