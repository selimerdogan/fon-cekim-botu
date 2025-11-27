import requests
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import os
import json
import time

def get_tefas_data():
    # URL'ler
    main_url = "https://www.tefas.gov.tr/FonKarsilastirma.aspx"
    api_url = "https://www.tefas.gov.tr/api/DB/BindComparisonFundReport"
    
    today = datetime.now()
    date_str = today.strftime("%d.%m.%Y")
    doc_date_str = today.strftime("%Y-%m-%d")
    
    # Tarayıcı gibi görünmek için detaylı header
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "tr-TR,tr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://www.tefas.gov.tr/FonKarsilastirma.aspx",
        "X-Requested-With": "XMLHttpRequest",
        "Origin": "https://www.tefas.gov.tr"
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
    
    print(f"Oturum başlatılıyor ve çerez alınıyor... ({date_str})")
    
    try:
        # SESSION OLUŞTURMA (ÖNEMLİ KISIM)
        session = requests.Session()
        
        # 1. Adım: Ana sayfaya gidip çerezleri (cookies) kapıyoruz
        # Bu işlem TEFAS'ın bizi "gerçek ziyaretçi" sanmasını sağlar.
        session.get(main_url, headers=headers)
        
        # Biraz insani bekleme (opsiyonel ama güvenli)
        time.sleep(2)
        
        print("API isteği atılıyor...")
        
        # 2. Adım: API'ye elimizdeki çerezlerle POST atıyoruz
        response = session.post(api_url, data=payload, headers=headers)
        
        # Hata kontrolü
        response.raise_for_status()
        
        try:
            result = response.json()
        except json.JSONDecodeError:
            print("HATA: JSON dönmedi. İçerik şifrelenmiş veya engellenmiş olabilir.")
            # Hata ayıklama için içeriğin bir kısmını yazdıralım
            print(f"İçerik özeti: {response.text[:200]}")
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
            print("Veri boş döndü.")
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
        print(f"SUCCESS: '{doc_date}' belgesine {len(fund_data)} adet fon başarıyla kaydedildi.")
        
    except Exception as e:
        print(f"Firebase Hatası: {e}")

if __name__ == "__main__":
    data, date_id = get_tefas_data()
    if data:
        save_history_to_firebase(data, date_id)
    else:
        # Veri çekilemezse workflow hata versin ki görelim
        print("İşlem başarısız oldu.")
        exit(1)
