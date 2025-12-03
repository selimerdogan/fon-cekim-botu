import requests
import firebase_admin
from firebase_admin import credentials, firestore
import json
import os
from datetime import datetime, timedelta
import sys

# --- AYARLAR ---
# Takip edilecek TEK fonun kodu (Burayı değiştirebilirsin)
SECILEN_FON = "TTE"  # Örnek: TTE, MAC, YAS vb.

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

def get_tefas_price(fon_kodu):
    """Seçilen fonun TEFAS'taki son fiyatını çeker"""
    print(f"{fon_kodu} için TEFAS verisi çekiliyor...")
    
    url = "https://www.tefas.gov.tr/api/DB/BindHistoryInfo"
    
    # Son veriyi yakalamak için son 5 günü istiyoruz
    today = datetime.now()
    start_date = today - timedelta(days=5)
    
    payload = {
        "fontip": "YAT",
        "sfontip": "",
        "bastarih": start_date.strftime("%d.%m.%Y"),
        "bittarih": today.strftime("%d.%m.%Y"),
        "fonkod": fon_kodu
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
                # API tarihsel sıralı döner, son eleman en günceldir
                son_veri = data[-1]
                fiyat = float(son_veri.get('FIYAT', 0))
                print(f"Güncel Fiyat Bulundu: {fiyat} TL")
                return fiyat
            else:
                print("Veri bulunamadı.")
                return None
        else:
            print(f"API Hatası: {response.status_code}")
            return None
    except Exception as e:
        print(f"Hata: {e}")
        return None

def save_snapshot():
    # 1. Fiyatı Çek
    fiyat = get_tefas_price(SECILEN_FON)
    
    if fiyat is None:
        print("Fiyat alınamadığı için işlem iptal edildi.")
        sys.exit(1)

    # 2. Tarih ve Saat Bilgisi
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d") # Döküman ID: 2025-12-03
    time_str = now.strftime("%H:%M")    # Döküman ID: 19:42

    # 3. Firestore'a Yazma (Senin İstediğin Yapı)
    # Koleksiyon: fonlar -> Döküman: [Tarih] -> Koleksiyon: snapshots -> Döküman: [Saat]
    
    print(f"Firebase'e yazılıyor... Yol: fonlar/{date_str}/snapshots/{time_str}")
    
    try:
        # Önce Tarih Dökümanını oluştur (Boş kalmaması için created_at ekliyoruz)
        date_ref = db.collection('fonlar').document(date_str)
        date_ref.set({'created_at': firestore.SERVER_TIMESTAMP}, merge=True)
        
        # Sonra Snapshot'ı ekle
        snapshot_ref = date_ref.collection('snapshots').document(time_str)
        
        # Veri Alanı: fon_tl
        snapshot_ref.set({
            'fon_tl': fiyat,
            # İstersen fon kodunu da ekleyebilirsin ama istemediğin için yorum satırı yaptım:
            # 'fon_kodu': SECILEN_FON 
        })
        
        print("✅ İŞLEM BAŞARILI! Tek fon snapshot kaydedildi.")
        
    except Exception as e:
        print(f"🔥 Yazma Hatası: {e}")
        sys.exit(1)

if __name__ == "__main__":
    save_snapshot()
