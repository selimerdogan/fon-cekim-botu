import requests
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import os
import json

def get_fund_data_from_isyatirim():
    # İş Yatırım'ın verileri çektiği "Arka Kapı" (JSON Endpoint)
    url = "https://www.isyatirim.com.tr/_Layouts/15/IsYatirim.Website/Common/Data.aspx/GetFundReturnList"
    
    today = datetime.now()
    doc_date_str = today.strftime("%Y-%m-%d")
    display_date = today.strftime("%d.%m.%Y")
    
    params = {
        "period": "1",  # Günlük Getiri tablosu (Güncel fiyatları içerir)
        "endOfMonth": "false"
    }
    
    print(f"İş Yatırım üzerinden veriler çekiliyor... ({display_date})")
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        
        # İş Yatırım verisi direkt JSON listesi olarak döner
        data = response.json()
        
        if not data:
            print("Veri boş döndü.")
            return None, None

        # Veriyi DataFrame'e al
        df = pd.DataFrame(data)
        
        # Sütun isimleri İş Yatırım'da şöyledir: 'Code', 'Price' (veya benzeri)
        # Gelen veriyi kontrol edip doğru sütunları alalım.
        # Genelde: 'Code' -> Fon Kodu, 'Price' -> Fiyat
        
        if 'Code' in df.columns and 'Price' in df.columns:
            df = df[['Code', 'Price']]
            df.columns = ['FONKODU', 'FIYAT']
        else:
            print("Beklenen sütunlar (Code, Price) bulunamadı.")
            print(f"Mevcut sütunlar: {df.columns}")
            return None, None
            
        # Fiyat sayısal mı kontrol et, değilse düzelt
        # İş Yatırım genelde sayıyı 'double' gönderir, string gelirse düzeltiriz.
        if df['FIYAT'].dtype == 'object':
             df['FIYAT'] = df['FIYAT'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
             df['FIYAT'] = pd.to_numeric(df['FIYAT'])
             
        # Map formatına çevir { "AFT": 12.34, ... }
        fund_dict = dict(zip(df['FONKODU'], df['FIYAT']))
        
        return fund_dict, doc_date_str

    except Exception as e:
        print(f"İş Yatırım Bağlantı Hatası: {e}")
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
        print(f"BAŞARILI: '{doc_date}' tarihine {len(fund_data)} adet fon kaydedildi.")
        
    except Exception as e:
        print(f"Firebase Hatası: {e}")

if __name__ == "__main__":
    data, date_id = get_fund_data_from_isyatirim()
    if data:
        save_history_to_firebase(data, date_id)
    else:
        print("İşlem başarısız.")
        exit(1)
