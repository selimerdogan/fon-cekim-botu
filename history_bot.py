import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta
import pandas as pd
import yfinance as yf
from tefas import Crawler
import requests
import sys
import os
import json
import time

# --- AYARLAR ---
# GitHub Actions environment variable kontrolü
if os.environ.get('FIREBASE_KEY'):
    cred = credentials.Certificate(json.loads(os.environ.get('FIREBASE_KEY')))
elif os.path.exists("serviceAccountKey.json"):
    cred = credentials.Certificate("serviceAccountKey.json")
else:
    print("HATA: Firebase anahtarı bulunamadı!")
    sys.exit(1)

if not firebase_admin._apps:
    firebase_admin.initialize_app(cred)
db = firestore.client()

def save_to_firebase_batch(data_list, collection_name="market_grafigi"):
    batch = db.batch()
    counter = 0
    total = 0
    
    print(f"💾 {len(data_list)} adet veri Firebase'e yazılıyor...")
    
    for item in data_list:
        # ÖNEMLİ DÜZELTME: Sembol içinde '/' varsa '_' ile değiştir
        # Firebase '/' karakterini alt koleksiyon sanıyor.
        safe_symbol = item['symbol'].replace("/", "_")
        
        doc_id = f"{item['prefix']}_{safe_symbol}"
        doc_ref = db.collection(collection_name).document(doc_id)
        
        payload = {
            "symbol": item['symbol'],
            "type": item['type'],
            "last_updated": firestore.SERVER_TIMESTAMP,
            "period": "1y",
            "history": item['history']
        }
        
        batch.set(doc_ref, payload, merge=True)
        counter += 1
        total += 1
        
        if counter >= 400:
            batch.commit()
            print(f"   -> 📦 {total} veri gönderildi...")
            batch = db.batch()
            counter = 0
            time.sleep(1)

    if counter > 0:
        batch.commit()
        print(f"   -> ✅ Kalan {counter} veri gönderildi. Toplam: {total}")

# ==============================================================================
# 1. TEFAS FONLARI GEÇMİŞİ
# ==============================================================================
def get_tefas_history():
    print("--- 1. TEFAS Fon Geçmişi Çekiliyor ---")
    crawler = Crawler()
    
    # Bugün ve 1 yıl öncesi
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    
    print(f"   📅 Tarih: {start_date} - {end_date}")
    
    try:
        df = crawler.fetch(start=start_date, end=end_date, columns=["code", "date", "price"])
        
        if df is None or df.empty:
            print("   ⚠️ TEFAS verisi boş geldi (API sorunu olabilir).")
            # Fallback: Belki tarih aralığı sorundur, son 30 günü deneyelim en azından grafik boş kalmasın
            # start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            # df = crawler.fetch(start=start_date, end=end_date, columns=["code", "date", "price"])
            return

        df['date'] = pd.to_datetime(df['date'])
        
        grouped = df.groupby('code')
        results = []
        
        for code, group in grouped:
            history_data = []
            for _, row in group.iterrows():
                history_data.append({
                    "d": row['date'].strftime("%Y-%m-%d"),
                    "c": float(row['price'])
                })
            
            results.append({
                "prefix": "FUND",
                "symbol": code,
                "type": "fund",
                "history": history_data
            })
            
        save_to_firebase_batch(results)
        
    except Exception as e:
        print(f"   ❌ TEFAS Hatası: {e}")

# ==============================================================================
# 2. YFINANCE MODÜLÜ (DÜZELTİLMİŞ)
# ==============================================================================
def process_yfinance_tickers(ticker_list, prefix, asset_type, suffix=""):
    if not ticker_list:
        return

    # DÜZELTME 1: Yahoo Finance için Sembol Temizliği
    # TradingView "BRK.B" verir, Yahoo "BRK-B" ister.
    # Ayrıca "/" içeren (JPM/PL gibi) imtiyazlı hisseleri filtreleyelim, genelde sorun çıkarır.
    
    clean_tickers = []
    original_map = {} # Yahoo sembolü -> Orijinal sembol eşleşmesi

    for t in ticker_list:
        if "/" in t: continue # Slash içerenleri (Preferred stocks) atla, Firebase'i bozar.
        
        yahoo_symbol = t.replace(".", "-") # BRK.B -> BRK-B
        full_symbol = f"{yahoo_symbol}{suffix}"
        
        clean_tickers.append(full_symbol)
        original_map[full_symbol] = t # Orijinal ismini sakla

    print(f"--- {prefix} ({len(clean_tickers)} Adet) Geçmiş Veri İndiriliyor ---")
    
    if not clean_tickers:
        print("   ⚠️ İndirilecek geçerli sembol kalmadı.")
        return

    try:
        # thread sayısını düşürdük, Yahoo bazen IP ban atıyor
        data = yf.download(clean_tickers, period="1y", interval="1d", group_by='ticker', progress=False, threads=False) 
        
        results = []
        
        # Tek sembol kontrolü
        if len(clean_tickers) == 1:
            iterator = [(clean_tickers[0], data)]
        else:
            iterator = data.items()

        for symbol_raw, df_symbol in iterator:
            try:
                if df_symbol.empty or 'Close' not in df_symbol.columns:
                    continue
                
                # NaN değerleri temizle
                df_clean = df_symbol.dropna(subset=['Close'])
                
                history_data = []
                for date, row in df_clean.iterrows():
                    val = row['Close']
                    if isinstance(val, pd.Series): val = val.iloc[0]
                    
                    history_data.append({
                        "d": date.strftime("%Y-%m-%d"),
                        "c": round(float(val), 4)
                    })
                
                # Firebase'e kaydederken Orijinal Sembolü kullanalım (BRK.B görünsün)
                real_symbol_name = original_map.get(symbol_raw, symbol_raw.replace(suffix, ""))
                
                results.append({
                    "prefix": prefix,
                    "symbol": real_symbol_name, 
                    "type": asset_type,
                    "history": history_data
                })
                
            except Exception as e:
                continue

        save_to_firebase_batch(results)

    except Exception as e:
        print(f"   ❌ YFinance Hatası ({prefix}): {e}")

# ==============================================================================
# SEMBOL LİSTELERİ
# ==============================================================================
def get_bist_symbols():
    url = "https://scanner.tradingview.com/turkey/scan"
    payload = {"filter": [{"left": "type", "operation": "in_range", "right": ["stock"]}],
               "columns": ["name"], "range": [0, 100]}
    try:
        r = requests.post(url, json=payload).json()
        return [x['d'][0] for x in r['data']]
    except: return ["THYAO", "GARAN"]

def get_us_symbols():
    # ABD için en büyük 50
    url = "https://scanner.tradingview.com/america/scan"
    payload = {"filter": [{"left": "type", "operation": "in_range", "right": ["stock"]}], # Sadece STOCK, DR/Preferred yok
               "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
               "columns": ["name"], "range": [0, 50]} 
    try:
        r = requests.post(url, json=payload).json()
        return [x['d'][0] for x in r['data']]
    except: return ["AAPL", "MSFT"]

def get_crypto_symbols():
    return ["BTC", "ETH", "SOL", "BNB", "XRP", "AVAX"]

# ==============================================================================
# MAIN
# ==============================================================================
if __name__ == "__main__":
    print("🚀 GEÇMİŞ VERİ BOTU BAŞLATILIYOR...\n")
    
    get_tefas_history()
    process_yfinance_tickers(get_crypto_symbols(), prefix="CRYPTO", asset_type="crypto", suffix="-USD")
    process_yfinance_tickers(get_bist_symbols(), prefix="BIST", asset_type="stock", suffix=".IS")
    process_yfinance_tickers(get_us_symbols(), prefix="US", asset_type="stock", suffix="")
    
    print("--- 5. Altın ve Döviz ---")
    # CMD (Gold) hatasını çözmek için sadece FX kullanıyoruz
    process_yfinance_tickers(["TRY=X", "EURTRY=X", "XAUUSD=X"], prefix="FX", asset_type="currency", suffix="")
    
    print("\n✅ TÜM İŞLEMLER TAMAMLANDI.")
