import requests
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import os
import json

def get_data_from_tradingview():
    # TradingView Türkiye Fon Tarama Endpoint'i
    url = "https://scanner.tradingview.com/turkey/scan"
    
    print("TradingView üzerinden fon verileri çekiliyor...")
    
    # TradingView'den sadece "Yatırım Fonlarını" istiyoruz.
    # Bu sorgu: Exchange = TEFAS olanları getirir.
    payload = {
        "columns": ["name", "close", "description"],
        "filter": [
            {"left": "type", "operation": "equal", "right": "fund"},
            {"left": "exchange", "operation": "equal", "right": "TEFAS"}
        ],
        "range": [0, 2000], # İlk 2000 fonu getir (Tümünü kapsar)
        "sort": {"sortBy": "name", "sortOrder": "asc"}
    }
    
    try:
        response = requests.post(url, json=payload, timeout=20)
        response.raise_for_status()
        
        data = response.json()
        
        if not data or 'data' not in data:
            print("TradingView veri döndürmedi.")
            return None, None
            
        print(f"Toplam {len(data['data'])} adet fon bulundu.")
        
        # Veriyi işle
        fund_dict = {}
        for item in data['data']:
            # item['d'] içinde sırasıyla ["name", "close", "description"] var
            # name genelde "TEFAS:AFT" şeklinde gelir.
            symbol_raw = item['d'][0] # Örn: TEFAS:AFT
            price = item['d'][1]      # Örn: 0.123456
            
            # Fon kodunu ayıkla (TEFAS:AFT -> AFT)
            code = symbol_raw.split(":")[-1]
            
            # Fiyatı sayıya çevir (Zaten sayı geliyor ama garanti olsun)
            if price is not None:
                fund_dict[code] = float(price)
                
        today = datetime.now()
        doc_date_str = today.strftime("%Y-%m-%d")
        
        return fund_dict, doc_date_str

    except Exception as e:
        print(f"Hata oluştu: {e}")
        return None, None

def save_history_to_firebase(fund_data, doc_date):
    if not fund_data:
        return

    try:
        cred_json = json.loads(os.environ.get('FIREBASE_CREDENTIALS'))
        cred = credentials.Certificate(cred_json)
        try:
            firebase_admin.get_app()
        except ValueError:
            firebase_admin.initialize_app(cred)
        
        db = firestore.client()
        doc_ref = db.collection('fund_history').document(doc_date)
        
        data_payload = {
            "date": doc_date,
            "createdAt": firestore.SERVER_TIMESTAMP,
            "closing": fund_data
        }
        
        doc_ref.set(data_payload, merge=True)
        print(f"BAŞARILI: '{doc_date}' tarihine {len(fund_data)} adet fon kaydedildi.")
        
    except Exception as e:
        print(f"Firebase Hatası: {e}")

if __name__ == "__main__":
    data, date_id = get_data_from_tradingview()
    if data:
        save_history_to_firebase(data, date_id)
    else:
        print("İşlem başarısız.")
        exit(1)
