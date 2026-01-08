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
    if not data_list:
        return

    batch = db.batch()
    counter = 0
    total = 0
    
    print(f"💾 {len(data_list)} adet veri Firebase'e yazılıyor...")
    
    for item in data_list:
        # ÖNEMLİ DÜZELTME: Sembol içinde '/' varsa '_' ile değiştir (Örn: JPM/PL -> JPM_PL)
        # Firebase '/' karakterini alt koleksiyon sanıyor ve patlıyor.
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
    
    # SENİN SİSTEM SAATİNİ KULLANIYORUZ (2026)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365) # Son 1 Yıl
    
    s_str = start_date.strftime("%Y-%m-%d")
    e_str = end_date.strftime("%Y-%m-%d")
    
    print(f"   📅 Tarih Aralığı: {s_str} - {e_str}")
    
    try:
        df = crawler.fetch(start=s_str, end=e_str, columns=["code", "date", "price"])
        
        if df is None or df.empty:
            print("   ⚠️ TEFAS verisi boş geldi.")
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
# 2. YFINANCE MODÜLÜ (2026 UYUMLU)
# ==============================================================================
def process_yfinance_tickers(ticker_list, prefix, asset_type, suffix=""):
    if not ticker_list:
        return

    clean_tickers = []
    original_map = {}

    for t in ticker_list:
        # Slash '/' içerenleri (JPM/PL vb.) filtrelemesek bile aşağıda replace ediyoruz ama
        # YFinance genelde bunları bulamaz. Yine de listede kalsın, clean_tickers'a ekleyelim.
        if "/" in t:
             # Yahoo formatı: JPM-PL veya JPM-pL olabilir, denemek lazım ama genelde -p eklenir.
             # Şimdilik risk almamak için replace ediyoruz.
             yahoo_symbol = t.replace("/", "-") 
        else:
             yahoo_symbol = t.replace(".", "-") # BRK.B -> BRK-B

        full_symbol = f"{yahoo_symbol}{suffix}"
        clean_tickers.append(full_symbol)
        original_map[full_symbol] = t # Orijinal ismi sakla

    print(f"--- {prefix} ({len(clean_tickers)} Adet) Geçmiş Veri İndiriliyor ---")
    
    if not clean_tickers:
        return

    try:
        # SENİN SİSTEM SAATİNİ KULLANIYORUZ
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        
        # threads=False: IP ban yememek için güvenli mod
        data = yf.download(clean_tickers, start=start_date, end=end_date, interval="1d", group_by='ticker', progress=False, threads=False) 
        
        results = []
        
        if data.empty:
            print("   ⚠️ YFinance hiç veri döndürmedi (Tarih aralığında veri yok).")
            return

        # Tekil sembol kontrolü
        if len(clean_tickers) == 1:
            iterator = [(clean_tickers[0], data)]
        else:
            iterator = data.items()

        for symbol_raw, df_symbol in iterator:
            try:
                if df_symbol.empty: continue
                # Sütun kontrolü (Bazen sadece Open/High döner, Close olmaz)
                if 'Close' not in df_symbol.columns: continue
                
                # NaN temizliği
                df_clean = df_symbol.dropna(subset=['Close'])
                if df_clean.empty: continue
                
                history_data = []
                for date, row in df_clean.iterrows():
                    val = row['Close']
                    if isinstance(val, pd.Series): val = val.iloc[0]
                    
                    history_data.append({
                        "d": date.strftime("%Y-%m-%d"),
                        "c": round(float(val), 4)
                    })
                
                if not history_data: continue

                # Firebase'e kaydederken Orijinal Sembolü (örn: BRK.B) kullanalım
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
# MAIN
# ==============================================================================
if __name__ == "__main__":
    print(f"🚀 GEÇMİŞ VERİ BOTU BAŞLATILIYOR... (Sistem Tarihi: {datetime.now().strftime('%Y-%m-%d')})\n")
    
    # 1. TEFAS
    get_tefas_history()
    
    # 2. KRİPTO
    process_yfinance_tickers(["BTC", "ETH", "SOL", "BNB", "XRP", "AVAX"], prefix="CRYPTO", asset_type="crypto", suffix="-USD")
    
    # 3. BIST (Manuel Örnek Liste - Hızlı Test İçin)
    bist_ornek = ["THYAO", "GARAN", "AKBNK", "EREGL", "ASELS", "SISE", "KCHOL", "BIMAS"] 
    process_yfinance_tickers(bist_ornek, prefix="BIST", asset_type="stock", suffix=".IS")
    
    # 4. ABD (Manuel Örnek Liste)
    us_ornek = ["AAPL", "MSFT", "TSLA", "NVDA", "GOOGL", "AMZN", "META"]
    process_yfinance_tickers(us_ornek, prefix="US", asset_type="stock", suffix="")
    
    # 5. FX & ALTIN
    print("--- 5. Altın ve Döviz ---")
    # XAUUSD=X yerine GC=F (Gold Futures) daha sağlıklıdır
    process_yfinance_tickers(["TRY=X", "EURTRY=X", "GC=F"], prefix="FX", asset_type="currency", suffix="")
    
    print("\n✅ TÜM İŞLEMLER TAMAMLANDI.")
