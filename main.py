import requests
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import os
import json

def get_tefas_data():
    url = "https://www.tefas.gov.tr/api/DB/BindComparisonFundReport"
    
    today = datetime.now()
    date_str = today.strftime("%d.%m.%Y")
    doc_date_str = today.strftime("%Y-%m-%d")
    
    # DÜZELTME: User-Agent eklendi. Bu olmadan TEFAS isteği reddeder.
    headers = {
        "Referer": "https://www.tefas.gov.tr/FonKarsilastirma.aspx",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
    }
    
    payload = {
        "calismatipi": "2",
        "fontip": "YAT",
        "sfontip": "IYF",
        "sonuctip": "MO",
        "bastarih": date_str,
        "bittarih": date_str,
        "strperiod": "1,1,1,1,1,1,1"
    }
    
    print(f"TEFAS'tan veri çekiliyor... ({date_str})")
    
    try:
        response = requests.post(url, data=payload, headers=headers)
        
        # Eğer sunucu hata kodu (403, 500 vs) dönerse işlemi durdur
        response.raise_for_status()
        
        try:
            result = response.json()
        except json.JSONDecodeError:
            print("HATA: TEFAS JSON döndürmedi. Muhtemelen engellendi veya bakımda.")
            print(f"Dönen yanıtın başı: {response.text[:200]}")
            return None, None
        
        if 'data' in result and result['data']:
            df = pd.DataFrame(result['data'])
            df = df[['FONKODU', 'FIYAT']]
            
            # Fiyat düzeltme
            df['FIYAT'] = df['FIYAT'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            df['FIYAT'] = pd.to_numeric(df['FIYAT'])
            
            fund_dict = dict(zip(df['FONKODU'], df['FIYAT']))
            return fund_dict, doc_date_str
        else:
            print("Veri boş döndü. (Resmi tatil veya veri henüz oluşmamış olabilir)")
            return None, None
            
    except Exception as e:
        print(f"Bağlantı Hatası: {e}")
        return None, None

def save_history_to_firebase(fund_data, doc_date):
    if not fund_data:
        return

    try:
        cred_json = json.loads(os.environ.get('FIREBASE_CREDENTIALS'))
        cred = credentials.Certificate(cred_json)
        firebase_admin.initialize_app(cred)
        
        db = firestore.client()
        doc_ref = db.collection('fund_history').document(doc_date)
        
        data_payload = {
            "date": doc_date,
            "createdAt": firestore.SERVER_TIMESTAMP,
            "closing": fund_data
        }
        
        doc_ref.set(data_payload, merge=True)
        print(f"'{doc_date}' belgesine {len(fund_data)} adet fon başarıyla kaydedildi.")
        
    except Exception as e:
        print(f"Firebase Hatası: {e}")

if __name__ == "__main__":
    data, date_id = get_tefas_data()
    if data:
        save_history_to_firebase(data, date_id)
    else:
        # GitHub Actions hatayı görsün diye exit code 1 veriyoruz (opsiyonel)
        pass
