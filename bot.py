import yfinance as yf
import firebase_admin
from firebase_admin import credentials, firestore
# ... (Firebase bağlantı kodların aynı kalsın) ...

def veri_guncelle():
    hisse = yf.Ticker("SASA.IS")
    
    # 1. GEÇMİŞ VERİYİ ÇEK (Grafik İçin)
    # period="1y" (1 yıl), interval="1d" (Günlük veri)
    hist = hisse.history(period="1y", interval="1d")
    
    grafik_verisi = []
    
    # Veriyi grafik kütüphanesinin anlayacağı formata çeviriyoruz
    for date, row in hist.iterrows():
        grafik_verisi.append({
            "timestamp": int(date.timestamp() * 1000), # JS için milisaniye
            "value": row['Close'] # Kapanış fiyatı
        })

    # 2. GÜNCEL VERİYİ AL (Başlık İçin)
    info = hisse.info 
    
    veri = {
        "sembol": "SASA",
        "fiyat": info.get('currentPrice'),
        "degisim": info.get('recommendationMean'), # Örnek
        "son_guncelleme": firestore.SERVER_TIMESTAMP,
        "chart_data": grafik_verisi  # <--- İşte grafiği çizecek olan liste bu!
    }

    # Firebase'e yaz
    db.collection("hisseler").document("SASA").set(veri)
    print("SASA grafik verisiyle birlikte güncellendi.")

if __name__ == "__main__":
    veri_guncelle()
