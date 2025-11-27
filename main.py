from curl_cffi import requests
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
    
    # Header bilgileri
    headers = {
        "Referer": "https://www.tefas.gov.tr/FonKarsilastirma.aspx",
        "Origin": "https://www.tefas.gov.tr",
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
    
    print(f"Chrome taklidi yapılarak veri çekiliyor... ({date_str})")
    
    try:
        # ÖNEMLİ: impersonate="chrome120" ile gerçek tarayıcı taklidi yapıyoruz
        response = requests.post(
            url, 
            data=payload, 
            headers=headers, 
            impersonate="chrome120"
        )
        
        # Eğer yine engel sayfası gelirse
        if "<title>Erişim Engellendi</title>" in response.text:
            print("HATA: TEFAS güvenlik duvarı (WAF) yine engelledi.")
            return None, None

        try:
            result = response.json()
        except Exception:
            print("HATA: Gelen veri JSON formatında değil.")
            # Gelen hatanın ne olduğunu görmek için başını yazdıralım
            print(f"Sunucu Yanıtı: {response.text[:200]}")
            return None, None
        
        if 'data' in result and result['data']:
            df = pd.DataFrame(result['data'])
            # Sadece Kod ve Fiyat alalım
            df = df[['FONKODU', 'FIYAT']]
            
            # Fiyatları temizle: "1.234,56" -> 1234.56 formatına
            df['FIYAT'] = df['FIYAT'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
            df['FIYAT'] = pd.to_numeric(df['FIYAT'])
            
            # Map formatına çevir
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
        # Koleksiyon: fund_history
        doc_ref = db.collection('fund_history').document(doc_date)
        
        data_payload = {
            "date": doc_date,
            "createdAt": firestore.SERVER_TIMESTAMP,
            "closing": fund_data
        }
        
        doc_ref.set(data_payload, merge=True)
        print(f"BAŞARILI: '{doc_date}' belgesine {len(fund_data)} adet fon yazıldı.")
        
    except Exception as e:
        print(f"Firebase Hatası: {e}")

if __name__ == "__main__":
    data, date_id = get_tefas_data()
    if data:
        save_history_to_firebase(data, date_id)
    else:
        print("İşlem başarısız.")
        exit(1)
