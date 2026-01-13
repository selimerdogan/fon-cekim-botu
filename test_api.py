import pandas as pd
import yfinance as yf
from tefas import Crawler
from datetime import datetime, timedelta

print(f"--- SİSTEM ZAMANI: {datetime.now()} ---")

# 1. TEFAS TESTİ
print("\n1. TEFAS TESTİ (Son 1 Ay)")
try:
    crawler = Crawler()
    # Bugünü değil, dünü bitiş tarihi alalım (Garanti olsun)
    end = datetime.now() - timedelta(days=1) 
    start = end - timedelta(days=30)
    
    print(f"   İstek Aralığı: {start.strftime('%Y-%m-%d')} - {end.strftime('%Y-%m-%d')}")
    
    df = crawler.fetch(start=start.strftime('%Y-%m-%d'), end=end.strftime('%Y-%m-%d'), columns=["code", "date", "price"])
    
    if df is None or df.empty:
        print("   ❌ TEFAS Sonuç: BOŞ (Veri gelmedi)")
    else:
        print(f"   ✅ TEFAS Sonuç: {len(df)} satır veri geldi.")
        print(df.head(2))
except Exception as e:
    print(f"   ❌ TEFAS HATA: {e}")

# 2. YFINANCE TESTİ (BIST)
print("\n2. YFINANCE TESTİ (THYAO.IS)")
try:
    # Bitiş tarihini yarına verelim ki bugünü tam kapsasın (Yfinance mantığı)
    end_yf = datetime.now() + timedelta(days=1)
    start_yf = datetime.now() - timedelta(days=30)
    
    print(f"   Sembol: THYAO.IS")
    print(f"   İstek Aralığı: {start_yf.strftime('%Y-%m-%d')} - {end_yf.strftime('%Y-%m-%d')}")
    
    data = yf.download("THYAO.IS", start=start_yf, end=end_yf, progress=False)
    
    if data.empty:
        print("   ❌ YFinance Sonuç: BOŞ")
    else:
        print(f"   ✅ YFinance Sonuç: {len(data)} satır veri geldi.")
        print(data.tail(2)) # Son verileri görelim
except Exception as e:
    print(f"   ❌ YFinance HATA: {e}")

# 3. YFINANCE TESTİ (ABD)
print("\n3. YFINANCE TESTİ (AAPL)")
try:
    # Manuel tarih yerine 'period' parametresiyle deneyelim, bazen daha güvenlidir
    data_us = yf.download("AAPL", period="1mo", progress=False)
    if data_us.empty:
        print("   ❌ Apple Sonuç: BOŞ")
    else:
        print(f"   ✅ Apple Sonuç: {len(data_us)} satır veri geldi.")
        print(data_us.tail(2))
except Exception as e:
    print(f"   ❌ Apple HATA: {e}")
