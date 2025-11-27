from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import requests
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import os
import json
import time

def get_tefas_data_smart():
    print("1. Adım: Chrome ile siteye girip 'Giriş İzni' (Cookie) alınıyor...")
    
    # Chrome Ayarları
    chrome_options = Options()
    chrome_options.add_argument("--headless=new") 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    session = requests.Session()
    
    try:
        # Ana sayfaya git ve yüklenmesini bekle
        driver.get("https://www.tefas.gov.tr/FonKarsilastirma.aspx")
        time.sleep(8) # İyice yüklensin
        
        # Tarayıcının aldığı Cookie'leri ve User-Agent'ı kopyala
        selenium_cookies = driver.get_cookies()
        user_agent = driver.execute_script("return navigator.userAgent;")
        
        # Cookie'leri requests oturumuna aktar
        for cookie in selenium_cookies:
            session.cookies.set(cookie['name'], cookie['value'])
            
        print("Çerezler kopyalandı. Selenium kapatılıyor.")
        driver.quit() # Artık tarayıcıya gerek yok
        
        # 2. Adım: Kopyalanan izinle veriyi çek
        print("2. Adım: Veri çekiliyor...")
        
        url = "https://www.tefas.gov.tr/api/DB/BindComparisonFundReport"
        today = datetime.now()
        date_str = today.strftime("%d.%m.%Y")
        doc_date_str = today.strftime("%Y-%m-%d")
        
        headers = {
            "User-Agent": user_agent,
            "Referer": "https://www.tefas.gov.tr/FonKarsilastirma.aspx",
            "X-Requested-With": "XMLHttpRequest",
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
        
        response = session.post(url, data=payload, headers=headers)
        
        # Hata kontrolü
        if "<title>Erişim Engellendi</title>" in response.text:
            print("HATA: WAF engeli devam ediyor.")
            return None, None

        try:
            result = response.json()
        except Exception:
            print(f"HATA: JSON dönmedi. Gelen: {response.text[:100]}")
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
            print("Veri boş.")
            return None, None

    except Exception as e:
        print(f"Hata: {e}")
        try:
            driver.quit()
        except:
            pass
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
        print(f"BAŞARILI: {doc_date} tarihine {len(fund_data)} fon kaydedildi.")
        
    except Exception as e:
        print(f"Firebase Hatası: {e}")

if __name__ == "__main__":
    data, date_id = get_tefas_data_smart()
    if data:
        save_history_to_firebase(data, date_id)
    else:
        print("İşlem başarısız.")
        exit(1)
