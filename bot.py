import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore
import os

# 1. Firebase Bağlantısını Kur (Bu kısım eksikti)
# Eğer uygulama daha önce başlatılmadıysa başlat
if not firebase_admin._apps:
    cred = credentials.Certificate("firebase_key.json")
    firebase_admin.initialize_app(cred)

# Veritabanı istemcisini oluştur
db = firestore.client()

def veri_guncelle():
    print("Veri çekiliyor...")
    hisse = yf.Ticker("SASA.IS")
    
    # ---------------------------------------------------------
    # A. GRAFİK İÇİN GEÇMİŞ VERİ (1 Yıllık)
    # ---------------------------------------------------------
    try:
        # Son 1 yılın günlük verisini al
        hist = hisse.history(period="1y", interval="1d")
        
        grafik_verisi = []
        
        # Pandas DataFrame'i Firebase'in seveceği JSON listesine çevir
        for date, row in hist.iterrows():
            grafik_verisi.append({
                "timestamp": int(date.timestamp() * 1000), # JS için milisaniye
                "value": round(row['Close'], 2) # Virgülden sonra 2 hane
            })
            
    except Exception as e:
        print(f"Grafik verisi çekilirken hata: {e}")
        grafik_verisi = []

    # ---------------------------------------------------------
    # B. GÜNCEL BİLGİLER (Fiyat, Yüzde vb.)
    # ---------------------------------------------------------
    try:
        info = hisse.info 
        guncel_fiyat = info.get('currentPrice')
        
        # Bazen 'recommendationMean' boş gelebilir, yerine önceki kapanışı kullanabiliriz
        # Basitçe değişim yüzdesi hesaplayalım:
        onceki_kapanis = info.get('previousClose', guncel_fiyat)
        if onceki_kapanis:
            degisim_yuzde = ((guncel_fiyat - onceki_kapanis) / onceki_kapanis) * 100
        else:
            degisim_yuzde = 0.0

        veri = {
            "sembol": "SASA",
            "fiyat": guncel_fiyat,
            "degisim_yuzde": round(degisim_yuzde, 2),
            "son_guncelleme": firestore.SERVER_TIMESTAMP,
            "chart_data": grafik_verisi  # Grafik çizdirecek liste
        }

        # ---------------------------------------------------------
        # C. FIREBASE'E YAZ
        # ---------------------------------------------------------
        db.collection("hisseler").document("SASA").set(veri)
        print(f"SASA verisi başarıyla güncellendi. Fiyat: {guncel_fiyat}")
        
    except Exception as e:
        print(f"Firebase yazma hatası: {e}")

if __name__ == "__main__":
    veri_guncelle()
