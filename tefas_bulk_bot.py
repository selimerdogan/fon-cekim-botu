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

# --- 1. FON KÜNYE BİLGİLERİNİ ÇEKEN FONKSİYON (DÜZELTİLDİ) ---
def get_fund_metadata():
    """Fonların Adı, Büyüklüğü ve Kişi Sayısı gibi statik verilerini çeker."""
    print("Fon kimlik bilgileri (GenelVeriler) çekiliyor...")
    url = "https://www.tefas.gov.tr/api/DB/GenelVeriler"
    
    today = datetime.now()
    start_date = today - timedelta(days=5)
    
    payload = {
        "fontip": "YAT",
        "sfontip": "",
        "bastarih": start_date.strftime("%d.%m.%Y"),
        "bittarih": today.strftime("%d.%m.%Y"),
        "fonkod": ""
    }
    
    # DÜZELTME: Bu başlıklar olmadan TEFAS veriyi reddeder
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Origin": "https://www.tefas.gov.tr",
        "Referer": "https://www.tefas.gov.tr/FonKarsilastirma.aspx", # Bu sayfa referans gösterilmeli
        "X-Requested-With": "XMLHttpRequest"
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        
        # Yanıt kontrolü
        if response.status_code != 200:
            print(f"Metadata API Hatası: {response.status_code}")
            return {}

        data = response.json().get('data', [])
        
        metadata_map = {}
        for item in data:
            kod = item.get('FONKODU')
            if kod:
                metadata_map[kod] = {
                    'ad': item.get('FONUNADI', ''),
                    'buyukluk': float(item.get('FONTOPLAMDEGER', 0) or 0),
                    'kisi_sayisi': int(float(item.get('KISISAYISI', 0) or 0))
                }
        print(f"✅ {len(metadata_map)} fon için kimlik bilgisi başarıyla alındı.")
        return metadata_map
        
    except Exception as e:
        print(f"Metadata Çekme Hatası: {e}")
        # Hata olsa bile kodun patlamaması için boş dönüyoruz
        return {}

# --- 2. FİYAT VE DEĞİŞİM VERİLERİNİ ÇEKEN FONKSİYON ---
def get_price_history():
    """Fonların fiyat geçmişini ve günlük değişimini hesaplar."""
    print("Fiyat geçmişi (BindHistoryInfo) çekiliyor...")
    
    url = "https://www.tefas.gov.tr/api/DB/BindHistoryInfo"
    
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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)",
        "Origin": "https://www.tefas.gov.tr",
        "Referer": "https://www.tefas.gov.tr/TarihselVeriler.aspx"
    }
    
    try:
        response = requests.post(url, json=payload, headers=headers)
        data = response.json().get('data', [])
        
        if not data:
            return None
            
        df = pd.DataFrame(data)
        
        if 'TARIH' in df.columns:
            df['tarih_dt'] = pd.to_datetime(pd.to_numeric(df['TARIH']), unit='ms')
        
        df = df.sort_values(by=['FONKODU', 'tarih_dt'])
        df['onceki_fiyat'] = df.groupby('FONKODU')['FIYAT'].shift(1)
        df['gunluk_degisim'] = ((df['FIYAT'] - df['onceki_fiyat']) / df['onceki_fiyat']) * 100
        df['gunluk_degisim'] = df['gunluk_degisim'].fillna(0.0)
        
        df_latest = df.groupby('FONKODU').tail(1).copy()
        return df_latest
        
    except Exception as e:
        print(f"Fiyat Verisi Hatası: {e}")
        return None

def save_bulk_snapshot():
    # 1. Metadata
    metadata = get_fund_metadata()
    
    # 2. Fiyat
    df = get_price_history()
    
    if df is None:
        sys.exit(1)
        
    print(f"Veriler birleştiriliyor... ({len(df)} fon)")

    # 3. Birleştirme
    fon_map = {}
    records = df.to_dict(orient='records')
    
    for item in records:
        fon_kodu = item['FONKODU']
        
        # Metadata eşleştirme
        detay = metadata.get(fon_kodu, {'ad': '', 'buyukluk': 0, 'kisi_sayisi': 0})
        
        fon_map[fon_kodu] = {
            'fiyat': float(item.get('FIYAT', 0)),
            'degisim': round(float(item.get('gunluk_degisim', 0)), 2),
            'ad': detay['ad'],
            'buyukluk': detay['buyukluk'],
            'kisi_sayisi': detay['kisi_sayisi']
        }

    # 4. Kayıt
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    time_str = now.strftime("%H:%M")

    print(f"Yazılıyor: fonlar/{date_str}/snapshots/{time_str}")
    
    try:
        db.collection('fonlar').document(date_str).set({'created_at': firestore.SERVER_TIMESTAMP}, merge=True)
        target_ref = db.collection('fonlar').document(date_str).collection('snapshots').document(time_str)
        target_ref.set(fon_map)
        print(f"✅ BAŞARILI! {len(fon_map)} fon (Tam Detaylı) kaydedildi.")
        
    except Exception as e:
        print(f"🔥 Yazma Hatası: {e}")

if __name__ == "__main__":
    save_bulk_snapshot()
