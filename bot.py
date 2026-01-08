import requests
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta
import sys
import os
import json
import warnings
from bs4 import BeautifulSoup
import time
import pandas as pd
import yfinance as yf 

# --- KÜTÜPHANELER ---
from tefas import Crawler
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

warnings.simplefilter(action='ignore', category=FutureWarning)

headers_general = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# --- KİMLİK DOĞRULAMA ---
firebase_key_str = os.environ.get('FIREBASE_KEY')
CMC_API_KEY = os.environ.get('CMC_API_KEY')

# Local test veya GitHub Actions ayrımı
if firebase_key_str:
    try:
        cred_dict = json.loads(firebase_key_str)
        cred = credentials.Certificate(cred_dict)
    except:
        # Bazen string escape sorunu olabilir, dosyadan okumayı dene
        if os.path.exists("firebase_key.json"):
            cred = credentials.Certificate("firebase_key.json")
        else:
            print("HATA: Firebase Key JSON formatında değil.")
            sys.exit(1)
elif os.path.exists("firebase_key.json"): 
    cred = credentials.Certificate("firebase_key.json")
else:
    try:
        cred = credentials.Certificate("serviceAccountKey.json")
    except:
        print("HATA: Firebase anahtarı bulunamadı!")
        sys.exit(1)

try:
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    db = firestore.client()
except Exception as e:
    print(f"HATA: Firebase hatası: {e}")
    sys.exit(1)

def metni_sayiya_cevir(metin):
    try:
        temiz = str(metin).replace('TL', '').replace('USD', '').replace('$', '').replace('%', '').strip()
        if "," in temiz:
            temiz = temiz.replace('.', '').replace(',', '.')
        return float(temiz)
    except:
        return 0.0

# ==============================================================================
# BÖLÜM 1: CANLI VERİ ÇEKME FONKSİYONLARI
# ==============================================================================

def get_doviz_foreks():
    print("1. Döviz Kurları çekiliyor...")
    data = {}
    isim_map = {
        "Kanada Doları": "CAD", "Euro": "EUR", "Sterlin": "GBP", 
        "İsviçre Frangı": "CHF", "Japon Yeni": "JPY", "Rus Rublesi": "RUB",
        "Çin Yuanı": "CNY", "BAE Dirhemi": "AED", "Dolar": "USD"
    }
    url = "https://www.foreks.com/doviz/"
    chrome_options = Options()
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    driver = None
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        driver.get(url)
        time.sleep(3)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        for row in soup.find_all("tr"):
            text_row = row.get_text()
            if "Bitcoin" in text_row: continue
            for tr_name, kod in isim_map.items():
                if tr_name in text_row:
                    cols = row.find_all("td")
                    if len(cols) >= 3:
                        try:
                            fiyat = metni_sayiya_cevir(cols[1].get_text(strip=True))
                            degisim = metni_sayiya_cevir(cols[2].get_text(strip=True))
                            if fiyat == 0 and len(cols) > 5:
                                fiyat = metni_sayiya_cevir(cols[5].get_text(strip=True))
                            if fiyat > 0:
                                data[kod] = {"price": fiyat, "change": degisim, "name": tr_name}
                        except: continue
                    break
    except Exception as e:
        print(f"Foreks Hatası: {e}")
    finally:
        if driver: driver.quit()
    return data

def get_altin_site():
    print("2. Altın Fiyatları çekiliyor...")
    data = {}
    try:
        r = requests.get("https://altin.doviz.com/", headers=headers_general, timeout=20)
        soup = BeautifulSoup(r.content, "html.parser")
        table = soup.find("table")
        if table:
            for tr in table.find_all("tr"):
                tds = tr.find_all("td")
                if len(tds) > 3:
                    try:
                        isim = tds[0].get_text(strip=True)
                        if "Ons" not in isim:
                            fiyat = metni_sayiya_cevir(tds[2].get_text(strip=True))
                            degisim = metni_sayiya_cevir(tds[3].get_text(strip=True))
                            data[isim] = {"price": fiyat, "change": degisim, "name": isim}
                    except: continue
    except: pass
    return data

def get_bist_tradingview():
    print("3. Borsa İstanbul taranıyor...")
    url = "https://scanner.tradingview.com/turkey/scan"
    payload = {
        "filter": [{"left": "type", "operation": "in_range", "right": ["stock", "dr"]}],
        "options": {"lang": "tr"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "close", "change", "description"],
        "range": [0, 600]
    }
    data = {}
    try:
        r = requests.post(url, json=payload, headers=headers_general, timeout=20)
        for h in r.json().get('data', []):
            d = h.get('d', [])
            if len(d) > 3:
                data[d[0]] = {"price": float(d[1]), "change": round(float(d[2]), 2), "name": d[3]}
    except: pass
    return data

def get_abd_tradingview():
    print("4. ABD Borsası taranıyor...")
    url = "https://scanner.tradingview.com/america/scan"
    payload = {
        "filter": [{"left": "type", "operation": "in_range", "right": ["stock", "dr"]}],
        "options": {"lang": "en"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "close", "change", "market_cap_basic", "description"],
        "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
        "range": [0, 100]
    }
    data = {}
    try:
        r = requests.post(url, json=payload, headers=headers_general, timeout=20)
        for h in r.json().get('data', []):
            d = h.get('d', [])
            if len(d) > 4:
                data[d[0]] = {"price": float(d[1]), "change": round(float(d[2]), 2), "name": d[4]}
    except: pass
    return data

def get_crypto_cmc(limit=100):
    if not CMC_API_KEY: 
        print("⚠️ CMC API Key bulunamadı, Kripto atlanıyor.")
        return {}
    print(f"5. Kripto Piyasası taranıyor...")
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    params = {'start': '1', 'limit': str(limit), 'convert': 'USD'}
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': CMC_API_KEY}
    data = {}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=20)
        for coin in r.json()['data']:
            quote = coin['quote']['USD']
            symbol = coin['symbol']
            data[f"{symbol}-USD"] = {
                "price": round(float(quote['price']), 4),
                "change": round(float(quote['percent_change_24h']), 2),
                "name": coin['name']
            }
    except Exception as e:
        print(f"CMC Hatası: {e}")
    return data

def get_tefas_lib():
    print("6. TEFAS Fonları çekiliyor...")
    try:
        crawler = Crawler()
        bugun = datetime.now()
        baslangic = bugun - timedelta(days=5) 
        
        # 'code' ve 'title' olarak istiyoruz
        df = crawler.fetch(
            start=baslangic.strftime("%Y-%m-%d"), 
            end=bugun.strftime("%Y-%m-%d"), 
            columns=["code", "date", "price", "title"]
        )
        
        if df is None or df.empty: return {}

        # Sütun isimlerini garantiye alalım (Büyük/Küçük harf duyarlılığı için)
        df.columns = [c.lower() for c in df.columns] 

        df['date'] = pd.to_datetime(df['date'])
        df = df.sort_values(by=['code', 'date'])
        df['onceki_fiyat'] = df.groupby('code')['price'].shift(1)
        df['degisim'] = ((df['price'] - df['onceki_fiyat']) / df['onceki_fiyat']) * 100
        df['degisim'] = df['degisim'].fillna(0.0)
        
        df_latest = df.groupby('code').tail(1)
        data = {}
        for item in df_latest.to_dict('records'):
            # BURADA ARTIK 'code' KULLANIYORUZ
            kod = item.get('code') 
            if kod:
                data[kod] = {
                    "price": float(item['price']), 
                    "change": round(float(item['degisim']), 2), 
                    "name": item.get('title', '')
                }
        return data
    except Exception as e:
        print(f"⚠️ TEFAS Hatası Detay: {e}")
        return {}

# ==============================================================================
# BÖLÜM 2: GRAFİK GÜNCELLEME (DEPO)
# ==============================================================================
def update_charts_bulk():
    """
    Seçili sembollerin 1 yıllık grafiğini çeker ve 'history' alt koleksiyonuna yazar.
    """
    print("\n--- Grafik Verileri Güncelleniyor ---")
    
    # GRAFİĞİ ÇİZİLECEK SEMBOLLER LİSTESİ
    # İleride burayı genişletebilirsin.
    targets = [
        {"id": "BIST_SASA", "y": "SASA.IS"},
        {"id": "BIST_THYAO", "y": "THYAO.IS"},
        {"id": "BIST_EREGL", "y": "EREGL.IS"},
        {"id": "US_AAPL", "y": "AAPL"},
        {"id": "US_TSLA", "y": "TSLA"},
        {"id": "CRYPTO_BTC-USD", "y": "BTC-USD"},
        {"id": "FOREX_USD", "y": "TRY=X"}, # Dolar/TL
        {"id": "GOLD_Gram Altın", "y": "GC=F"} # Altın (Yaklaşık değer)
    ]
    
    batch = db.batch()
    count = 0
    
    for item in targets:
        try:
            ticker = yf.Ticker(item["y"])
            hist = ticker.history(period="1y", interval="1d")
            
            if hist.empty: continue

            chart_data = []
            for date, row in hist.iterrows():
                chart_data.append({
                    "t": int(date.timestamp() * 1000), 
                    "v": round(row['Close'], 2)
                })
            
            # Sub-collection yapısı: live_market -> BIST_SASA -> history -> 1y
            ref = db.collection('live_market').document(item["id"]) \
                    .collection('history').document('1y')
            
            batch.set(ref, {"data": chart_data})
            count += 1

            if count >= 400: # Batch limiti koruması
                batch.commit()
                batch = db.batch()
                count = 0

        except Exception:
            continue

    if count > 0:
        batch.commit()
        print(f"✅ {count} adet grafik verisi güncellendi.")

# ==============================================================================
# BÖLÜM 3: ANA İŞLEM (VİTRİN KAYDI)
# ==============================================================================
def save_to_firestore_bulk(all_data):
    batch = db.batch()
    count = 0
    collection_ref = db.collection('live_market')

    for category, items in all_data.items():
        asset_type = "other"
        if category == "doviz_tl": asset_type = "forex"
        elif category == "altin_tl": asset_type = "gold"
        elif category == "borsa_tr_tl": asset_type = "bist"
        elif category == "borsa_abd_usd": asset_type = "us_stock"
        elif category == "kripto_usd": asset_type = "crypto"
        elif category == "fon_tl": asset_type = "fund"

        for symbol, details in items.items():
            # Temiz ID Oluşturma
            clean_symbol = str(symbol).replace('.', '_').replace('/', '').replace(' ', '')
            doc_id = f"{asset_type.upper()}_{clean_symbol}"
            
            doc_ref = collection_ref.document(doc_id)
            doc_data = {
                "symbol": str(symbol),
                "name": str(details.get('name', symbol)),
                "price": details.get('price', 0.0),
                "change": details.get('change', 0.0),
                "type": asset_type,
                "last_updated": firestore.SERVER_TIMESTAMP
            }
            batch.set(doc_ref, doc_data, merge=True)
            count += 1
            if count >= 400:
                batch.commit()
                print("📦 400 veri yazıldı...")
                batch = db.batch()
                count = 0

    if count > 0: batch.commit()
    print(f"✅ Toplam {count} canlı veri güncellendi.")

# --- ÇALIŞTIRMA ---
if __name__ == "__main__":
    try:
        print("--- BOT BAŞLIYOR ---")
        
        # 1. Canlı Verileri Topla
        raw_data = {
            "doviz_tl": get_doviz_foreks(),
            "altin_tl": get_altin_site(),
            "borsa_tr_tl": get_bist_tradingview(),
            "borsa_abd_usd": get_abd_tradingview(),
            "kripto_usd": get_crypto_cmc(100),
            "fon_tl": get_tefas_lib() # TEFAS Düzeltildi
        }

        # 2. Canlı Verileri Kaydet (Vitrin)
        save_to_firestore_bulk(raw_data)

        # 3. Grafikleri Güncelle (Depo)
        update_charts_bulk()

    except Exception as e:
        print(f"KRİTİK HATA: {e}")
        sys.exit(1)
