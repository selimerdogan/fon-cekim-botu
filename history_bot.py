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
FIREBASE_KEY_PATH = "serviceAccountKey.json" # Kendi dosya yolun
# Firebase Init
if not firebase_admin._apps:
    cred = credentials.Certificate(FIREBASE_KEY_PATH)
    firebase_admin.initialize_app(cred)
db = firestore.client()

def save_to_firebase_batch(data_list, collection_name="market_grafigi"):
    """
    Verileri 400'lü paketler halinde Firebase'e yazar.
    """
    batch = db.batch()
    counter = 0
    total = 0
    
    print(f"💾 {len(data_list)} adet veri Firebase'e yazılıyor...")
    
    for item in data_list:
        # Doküman ID Örn: BIST_THYAO, US_AAPL, FUND_AFT
        doc_id = f"{item['prefix']}_{item['symbol']}"
        doc_ref = db.collection(collection_name).document(doc_id)
        
        # Veriyi hazırla
        payload = {
            "symbol": item['symbol'],
            "type": item['type'],
            "last_updated": firestore.SERVER_TIMESTAMP,
            "period": "1y",
            "history": item['history'] # [{d: '2024..', c: 12.5}, ...]
        }
        
        batch.set(doc_ref, payload, merge=True)
        counter += 1
        total += 1
        
        if counter >= 400:
            batch.commit()
            print(f"   -> 📦 {total} veri gönderildi...")
            batch = db.batch()
            counter = 0
            time.sleep(1) # Firebase'i boğmamak için ufak bekleme

    if counter > 0:
        batch.commit()
        print(f"   -> ✅ Kalan {counter} veri gönderildi. Toplam: {total}")

# ==============================================================================
# 1. TEFAS FONLARI GEÇMİŞİ
# ==============================================================================
def get_tefas_history():
    print("--- 1. TEFAS Fon Geçmişi Çekiliyor ---")
    crawler = Crawler()
    
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d") # 1 Yıl
    
    print(f"   📅 Tarih: {start_date} - {end_date}")
    
    try:
        # Tüm fonları tek seferde çekiyoruz (En hızlı yöntem)
        df = crawler.fetch(start=start_date, end=end_date, columns=["code", "date", "price"])
        
        if df is None or df.empty:
            print("   ⚠️ TEFAS verisi boş.")
            return

        # Tarih formatını düzelt
        df['date'] = pd.to_datetime(df['date'])
        
        # Fon bazında grupla
        grouped = df.groupby('code')
        results = []
        
        for code, group in grouped:
            history_data = []
            # Veriyi sıkıştır (Tarih ve Fiyat)
            # Boyut tasarrufu için: d=date, c=close
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
# 2. YFINANCE MODÜLÜ (BIST, ABD, KRİPTO, ALTIN, DÖVİZ İÇİN ORTAK)
# ==============================================================================
def process_yfinance_tickers(ticker_list, prefix, asset_type, suffix=""):
    """
    Yfinance kullanarak toplu geçmiş veri çeker.
    ticker_list: ['THYAO', 'GARAN'] gibi saf liste
    suffix: BIST için '.IS', Kripto için '-USD' gibi ekler.
    """
    if not ticker_list:
        return

    print(f"--- {prefix} ({len(ticker_list)} Adet) Geçmiş Veri İndiriliyor ---")
    
    # Yfinance'in anlayacağı formata çevir (Örn: THYAO.IS)
    yf_tickers = [f"{t}{suffix}" for t in ticker_list]
    
    # Veriyi toplu indir (Threading ile hızlıdır)
    # group_by='ticker' önemli, veriyi hisse bazında ayırır.
    try:
        data = yf.download(yf_tickers, period="1y", interval="1d", group_by='ticker', progress=False, threads=True)
        
        results = []
        
        # Tek hisse/coin mi çoklu mu kontrolü
        if len(yf_tickers) == 1:
            # Tek veri gelince yapı farklı oluyor, onu listeye çevirelim
            iterator = [(yf_tickers[0], data)]
        else:
            iterator = data.items() # Sütun bazlı döner ama group_by ticker olduğu için ticker bazlı döner

        # DataFrame yapısını çözme (Biraz karmaşıktır multi-index)
        # Yfinance son sürümde yapıyı değiştirdi, en garantisi tek tek işlemektir.
        
        for symbol_raw in yf_tickers:
            try:
                # İlgili hissenin verisini al
                if len(yf_tickers) == 1:
                    df_symbol = data
                else:
                    df_symbol = data[symbol_raw]
                
                # Boş veri kontrolü
                if df_symbol.empty or 'Close' not in df_symbol.columns:
                    continue
                
                # NaN temizliği
                df_symbol = df_symbol.dropna(subset=['Close'])
                
                history_data = []
                for date, row in df_symbol.iterrows():
                    val = row['Close']
                    # Sayı kontrolü (Pandas Series gelebilir)
                    if isinstance(val, pd.Series):
                        val = val.iloc[0]
                        
                    history_data.append({
                        "d": date.strftime("%Y-%m-%d"),
                        "c": round(float(val), 4)
                    })
                
                # Temiz sembol adı (Soneki kaldır)
                clean_symbol = symbol_raw.replace(suffix, "")
                
                results.append({
                    "prefix": prefix,
                    "symbol": clean_symbol,
                    "type": asset_type,
                    "history": history_data
                })
                
            except Exception as e:
                # Bazı hisselerde veri olmayabilir, atla
                continue

        save_to_firebase_batch(results)

    except Exception as e:
        print(f"   ❌ YFinance Hatası ({prefix}): {e}")

# ==============================================================================
# YARDIMCI: SEMBOL LİSTELERİNİ GETİR
# ==============================================================================
def get_bist_symbols():
    # TradingView scanner'dan hisse listesini alıp Yfinance'e vereceğiz
    url = "https://scanner.tradingview.com/turkey/scan"
    payload = {"filter": [{"left": "type", "operation": "in_range", "right": ["stock", "dr"]}],
               "columns": ["name"], "range": [0, 100]} # İLK 100 HİSSE (Hepsini istersen range'i artır)
    try:
        r = requests.post(url, json=payload).json()
        return [x['d'][0] for x in r['data']]
    except: return ["THYAO", "GARAN", "ASELS", "EREGL", "SISE"] # Fallback

def get_us_symbols():
    # ABD için en büyük 50 şirketi çekelim (Hepsini çekmek çok sürer)
    url = "https://scanner.tradingview.com/america/scan"
    payload = {"filter": [{"left": "type", "operation": "in_range", "right": ["stock"]}],
               "sort": {"sortBy": "market_cap_basic", "sortOrder": "desc"},
               "columns": ["name"], "range": [0, 50]} 
    try:
        r = requests.post(url, json=payload).json()
        return [x['d'][0] for x in r['data']]
    except: return ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]

def get_crypto_symbols():
    # En popüler kriptolar
    return ["BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "AVAX", "DOGE"]

# ==============================================================================
# ANA ÇALIŞTIRMA BLOĞU
# ==============================================================================
if __name__ == "__main__":
    print("🚀 GEÇMİŞ VERİ BOTU BAŞLATILIYOR...\n")
    
    # 1. FONLAR
    get_tefas_history()
    
    # 2. KRİPTO
    process_yfinance_tickers(get_crypto_symbols(), prefix="CRYPTO", asset_type="crypto", suffix="-USD")
    
    # 3. BIST (TR)
    bist_list = get_bist_symbols()
    process_yfinance_tickers(bist_list, prefix="BIST", asset_type="stock", suffix=".IS")
    
    # 4. ABD BORSASI
    us_list = get_us_symbols()
    process_yfinance_tickers(us_list, prefix="US", asset_type="stock", suffix="")
    
    # 5. ALTIN & DÖVİZ (Manuel Liste)
    # XAUUSD=X (Ons Altın), TRY=X (Dolar/TL), EURTRY=X (Euro/TL)
    emtia_list = ["XAUUSD=X", "TRY=X", "EURTRY=X"]
    # Bunları özel işleyelim, prefix karmaşası olmasın
    # Manuel olarak Yfinance fonksiyonuna atıyoruz ama suffix yok.
    
    print("--- 5. Altın ve Döviz Geçmişi ---")
    # Özel isim mapping gerekebilir, şimdilik raw indiriyoruz.
    # Frontend'de 'TRY=X' görünce 'USD/TRY' olduğunu anlamalısın.
    process_yfinance_tickers(["TRY=X", "EURTRY=X"], prefix="FX", asset_type="currency", suffix="")
    process_yfinance_tickers(["GC=F"], prefix="CMD", asset_type="gold", suffix="") # GC=F = Gold Futures

    print("\n✅ TÜM İŞLEMLER TAMAMLANDI.")

