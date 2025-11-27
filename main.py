from curl_cffi import requests
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import os
import json

def get_data_from_isyatirim_secure():
    # İş Yatırım JSON Endpoint
    url = "https://www.isyatirim.com.tr/_Layouts/15/IsYatirim.Website/Common/Data.aspx/GetFundReturnList"
    
    today = datetime.now()
    doc_date_str = today.strftime("%Y-%m-%d")
    
    # Parametreler (Günlük Getiri Listesi)
    params = {
        "period": "1",
        "endOfMonth": "false"
    }
    
    # Bu Headerlar sunucuyu tarayıcı olduğumuza inandırır
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://www.isyatirim.com.tr/tr-tr/analiz/fon/Sayfalar/Fon-Arama.aspx",
        "Origin": "https://www.isyatirim.com.tr",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest"
    }
    
    print(f"İş Yatırım (JSON) üzerinden veri çekiliyor... ({doc_date_str})")
    
    try:
        # requests yerine curl_cffi kullanıyoruz -> impersonate="chrome120"
        response = requests.get(
            url, 
            params=params, 
            headers=headers, 
            impersonate="chrome120",
            timeout=30
        )
        
        # 404 veya 403 gelirse hata fırlat
        if response.status_code != 200:
            print(f"Sunucu Hatası Kodu: {response.status_code}")
            return None, None
            
        # JSON verisini al
        try:
            data = response.json()
        except Exception:
            # Bazen sunucu JSON yerine HTML hata dönerse
            print("HATA: JSON formatı bozuk veya HTML döndü.")
            return None, None
            
        if not data:
            print("HATA: Veri boş geldi.")
            return None, None
            
        # DataFrame oluştur
        df = pd.DataFrame(data)
        
        # İş Yatırım'da sütun adları: 'Code', 'Price' (İsimler değişebilir, kontrol ediyoruz)
        # Genelde Code = Fon Kodu, Price = Son Fiyat
        if 'Code' in df.columns and 'Price' in df.columns:
            df = df[['Code', 'Price']]
            df.columns = ['FONKODU', 'FIYAT']
        else:
            print(f"Beklenen sütunlar bulunamadı. Mevcut sütunlar: {df.columns}")
            return None, None
            
        # Temizlik
        df['FONKODU'] = df['FONKODU'].astype(str).str.strip()
        df['FIYAT'] = pd.to_numeric(df['FIYAT'], errors='coerce')
        df = df.dropna(subset=['FIYAT'])
        
        # Sözlüğe çevir
        fund_dict = dict(zip(df['FONKODU'], df['FIYAT']))
        
        return fund_dict, doc_date_str

    except Exception as e:
        print(f"Bağlantı Hatası: {e}")
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
    data, date_id = get_data_from_isyatirim_secure()
    if data:
        save_history_to_firebase(data, date_id)
    else:
        print("İşlem başarısız.")
        exit(1)
