from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import os
import json
import time

def get_tefas_data_with_selenium():
    print("Selenium ile gerçek Chrome tarayıcısı başlatılıyor...")
    
    # Chrome Ayarları (Headless - Ekran olmadan çalışma modu)
    chrome_options = Options()
    chrome_options.add_argument("--headless=new") # Yeni nesil headless mod
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    # Bot olduğumuzu gizleyen sihirli komutlar:
    chrome_options.add_argument("--disable-blink-features=AutomationControlled") 
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    
    try:
        # 1. Ana sayfaya git ve yüklenmesini bekle (Çerezlerin oluşması için)
        print("TEFAS ana sayfasına gidiliyor...")
        driver.get("https://www.tefas.gov.tr/FonKarsilastirma.aspx")
        time.sleep(5) # Sayfanın tam oturması için bekle
        
        # Tarih ayarları
        today = datetime.now()
        date_str = today.strftime("%d.%m.%Y")
        doc_date_str = today.strftime("%Y-%m-%d")

        print(f"Tarayıcı içinden veri talep ediliyor... ({date_str})")

        # 2. Veriyi çekmek için tarayıcının CONSOLE kısmında JavaScript çalıştırıyoruz.
        # Bu yöntem harikadır çünkü istek tarayıcının kendi güvenli oturumundan gider.
        js_script = f"""
        var callback = arguments[0];
        var params = 'calismatipi=2&fontip=YAT&sfontip=IYF&sonuctip=MO&bastarih={date_str}&bittarih={date_str}&strperiod=1,1,1,1,1,1,1';
        
        var xhr = new XMLHttpRequest();
        xhr.open('POST', '/api/DB/BindComparisonFundReport', true);
        xhr.setRequestHeader('Content-type', 'application/x-www-form-urlencoded; charset=UTF-8');
        
        xhr.onreadystatechange = function() {{
            if (xhr.readyState === 4) {{
                if (xhr.status === 200) {{
                    callback(xhr.responseText);
                }} else {{
                    callback('ERROR: ' + xhr.status);
                }}
            }}
        }};
        xhr.send(params);
        """
        
        # JavaScript'i çalıştır ve sonucu Python'a al
        response_text = driver.execute_async_script(js_script)
        
        if response_text and response_text.startswith("ERROR"):
            print(f"JavaScript Hatası: {response_text}")
            return None, None

        # Gelen JSON verisini işle
        result = json.loads(response_text)
        
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
        print(f"Selenium Hatası: {e}")
        return None, None
    finally:
        driver.quit()

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
        print(f"BAŞARILI: '{doc_date}' belgesine {len(fund_data)} adet fon yazıldı.")
        
    except Exception as e:
        print(f"Firebase Hatası: {e}")

if __name__ == "__main__":
    data, date_id = get_tefas_data_with_selenium()
    if data:
        save_history_to_firebase(data, date_id)
    else:
        print("İşlem başarısız.")
        exit(1)
