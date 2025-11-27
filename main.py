import requests
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timedelta
import os
import json

# --- 1. TEFAS'TAN VERİ ÇEKME ---
def get_tefas_data():
    url = "https://www.tefas.gov.tr/api/DB/BindComparisonFundReport"
    
    # Hafta içi akşam çalıştırılacağı için bugünün tarihini alıyoruz.
    # Eğer bu kodu gece 00:00'dan sonra çalıştıracaksan timedelta(days=1) yapmalısın.
    today = datetime.now()
    date_str = today.strftime("%d.%m.%Y")       # TEFAS formatı (27.11.2025)
    doc_date_str = today.strftime("%Y-%m-%d")   # Firebase Belge ID formatı (2025-11-27)
    
    headers = {
        "Referer": "https://www.tefas.gov.tr/FonKarsilastirma.aspx",
        "X-Requested-With": "XMLHttpRequest",
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
        result = response.json()
        
        if 'data' in result and result['data']:
            df = pd.DataFrame(result['data'])
            
            # Sadece Kod ve Fiyat alalım
            df = df[['FONKODU', 'FIYAT']]
            
            # Fiyatları sayıya çevirelim
            df['FIYAT'] = df['FIYAT'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            df['FIYAT'] = pd.to_numeric(df['FIYAT'])
            
            # Listeyi { "AFT": 12.34, "YAS": 56.78 } formatına (Map/Dictionary) çevirelim
            # Bu yapı senin ekran görüntüsündeki 'closing' map yapısına uyar.
            fund_dict = dict(zip(df['FONKODU'], df['FIYAT']))
            
            return fund_dict, doc_date_str
        else:
            print("Veri boş döndü.")
            return None, None
            
    except Exception as e:
        print(f"Hata: {e}")
        return None, None

# --- 2. FIREBASE'E YAZMA (TARİHÇE YAPISI) ---
def save_history_to_firebase(fund_data, doc_date):
    if not fund_data:
        return

    # Secret'tan credentials okuma
    cred_json = json.loads(os.environ.get('FIREBASE_CREDENTIALS'))
    cred = credentials.Certificate(cred_json)
    firebase_admin.initialize_app(cred)
    
    db = firestore.client()
    
    # Koleksiyon adı: fund_history (market_history ile karışmasın diye)
    # Belge ID'si: 2025-11-27
    doc_ref = db.collection('fund_history').document(doc_date)
    
    # Ekran görüntüsündeki yapıya uygun veri paketi
    data_payload = {
        "date": doc_date,
        "createdAt": firestore.SERVER_TIMESTAMP,
        "closing": fund_data  # Tüm fonlar burada key-value olarak duracak
    }
    
    # merge=True kullanıyoruz ki varsa üzerine yazsın, yoksa oluştursun
    doc_ref.set(data_payload, merge=True)
    
    print(f"'{doc_date}' belgesine {len(fund_data)} adet fon başarıyla kaydedildi.")

if __name__ == "__main__":
    data, date_id = get_tefas_data()
    
    if data:
        save_history_to_firebase(data, date_id)
    else:
        print("Kaydedilecek veri bulunamadı.")
