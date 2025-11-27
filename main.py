from curl_cffi import requests
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import os
import json
import random
import time

def get_free_proxies():
    """İnternetten güncel ücretsiz proxy listesi çeker"""
    print("Proxy listesi aranıyor...")
    proxies = []
    try:
        # Hızlı ve güncel proxy listesi sunan kaynak
        resp = requests.get("https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt", timeout=10)
        if resp.status_code == 200:
            for line in resp.text.splitlines():
                if line.strip():
                    proxies.append(f"http://{line.strip()}")
        print(f"{len(proxies)} adet proxy bulundu.")
        return proxies
    except Exception as e:
        print(f"Proxy listesi alınamadı: {e}")
        return []

def get_tefas_data_with_proxy():
    url = "https://www.tefas.gov.tr/api/DB/BindComparisonFundReport"
    
    today = datetime.now()
    date_str = today.strftime("%d.%m.%Y")
    doc_date_str = today.strftime("%Y-%m-%d")
    
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

    # Proxy listesini al
    proxy_list = get_free_proxies()
    # Listeyi karıştır ki her seferinde farklı denesin
    random.shuffle(proxy_list)
    
    # Maksimum 20 proxy denesin, olmazsa pes etsin
    max_attempts = 20
    
    print(f"TEFAS verisi için Proxy ile bağlanılıyor... ({date_str})")
    
    for i, proxy_url in enumerate(proxy_list[:max_attempts]):
        try:
            print(f"Deneme {i+1}/{max_attempts} -> Proxy: {proxy_url}")
            
            # Proxy ayarı
            proxies = {"http": proxy_url, "https": proxy_url}
            
            # İsteği at (impersonate="chrome120" ile tarayıcı taklidi + Proxy)
            response = requests.post(
                url, 
                data=payload, 
                headers=headers, 
                impersonate="chrome120",
                proxies=proxies,
                timeout=10 # 10 saniye yanıt vermezse diğerine geç
            )
            
            if response.status_code == 200:
                # Veri geldi mi kontrol et
                try:
                    result = response.json()
                    if 'data' in result and result['data']:
                        print("BAŞARILI! Veri çekildi.")
                        
                        df = pd.DataFrame(result['data'])
                        df = df[['FONKODU', 'FIYAT']]
                        
                        # Fiyat düzeltme
                        df['FIYAT'] = df['FIYAT'].astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False)
                        df['FIYAT'] = pd.to_numeric(df['FIYAT'])
                        
                        fund_dict = dict(zip(df['FONKODU'], df['FIYAT']))
                        return fund_dict, doc_date_str
                    else:
                        print("Sunucu yanıt verdi ama veri boş.")
                except json.JSONDecodeError:
                    print("JSON hatası, proxy bloklanmış olabilir.")
            else:
                print(f"Başarısız kod: {response.status_code}")
                
        except Exception as e:
            # Proxy hatası (timeout vs) normaldir, pas geç
            pass
            
    print("Tüm proxy denemeleri başarısız oldu.")
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
        print(f"FIREBASE KAYIT BAŞARILI: '{doc_date}' tarihine {len(fund_data)} adet fon yazıldı.")
        
    except Exception as e:
        print(f"Firebase Hatası: {e}")

if __name__ == "__main__":
    data, date_id = get_tefas_data_with_proxy()
    if data:
        save_history_to_firebase(data, date_id)
    else:
        print("İşlem başarısız.")
        exit(1)
