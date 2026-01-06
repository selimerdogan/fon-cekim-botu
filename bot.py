import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore
import os

# 1. Firebase Bağlantısı (GitHub Secrets'tan anahtarı alacak)
# Not: Bu kısım için Firebase'den indirdiğin .json anahtarını kullanacağız
cred = credentials.Certificate("firebase_key.json") 
firebase_admin.initialize_app(cred)
db = firestore.client()

def veri_guncelle():
    # 2. Veriyi Çek (SASA Örneği)
    hisse = yf.Ticker("SASA.IS")
    # Son anlık veriyi al
    info = hisse.info 
    
    veri = {
        "sembol": "SASA",
        "fiyat": info.get('currentPrice'),
        "degisim_yuzde": info.get('recommendationMean'), # Örnek veri
        "son_guncelleme": firestore.SERVER_TIMESTAMP
    }

    # 3. Firebase'e Yaz
    # 'hisseler' koleksiyonunda 'SASA' dökümanını günceller
    db.collection("hisseler").document("SASA").set(veri)
    print("SASA verisi Firebase'e gönderildi.")

if __name__ == "__main__":
    veri_guncelle()
