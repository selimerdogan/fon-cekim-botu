import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from io import StringIO
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import json
import os

# --- FIREBASE BAĞLANTISI ---
firebase_creds_str = os.environ.get('FIREBASE_CREDENTIALS')

if firebase_creds_str:
    cred_dict = json.loads(firebase_creds_str)
    cred = credentials.Certificate(cred_dict)
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    db = firestore.client()
else:
    if os.path.exists("serviceAccountKey.json"):
        cred = credentials.Certificate("serviceAccountKey.json")
        if not firebase_admin._apps:
             firebase_admin.initialize_app(cred)
        db = firestore.client()
    else:
        print("KRİTİK HATA: Firebase şifresi bulunamadı!")
        exit(1)

def get_fintables_funds():
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    
    all_dataframes = []

    try:
        url = "https://fintables.com/fonlar/getiri"
        print("Fintables'a gidiliyor...")
        driver.get(url)
        time.sleep(5)

        # Çerez uyarısını kapat (Varsa)
        try:
            cookie_buttons = driver.find_elements(By.XPATH, "//button[contains(text(), 'Kabul Et') or contains(text(), 'Tamam') or contains(text(), 'Anladım')]")
            if cookie_buttons:
                driver.execute_script("arguments[0].click();", cookie_buttons[0])
                time.sleep(1)
        except:
            pass

        current_page_num = 1
        
        while True:
            print(f"--- Sayfa {current_page_num} Taranıyor ---")
            
            # 1. Mevcut Sayfayı Oku
            html = driver.page_source
            tables = pd.read_html(StringIO(html))
            
            if tables:
                df = tables[0]
                # Başlık Temizliği
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = [str(col[-1]).strip() for col in df.columns]
                else:
                    df.columns = [str(col).strip() for col in df.columns]
                
                df = df.astype(str)
                all_dataframes.append(df)
                print(f"Sayfa {current_page_num}: {len(df)} veri alındı.")
            
            # 2. Bir Sonraki Sayfa Numarasına Tıkla
            target_page_num = current_page_num + 1
            
            try:
                # Sayfanın en altına in
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                
                # Hedef numaranın olduğu butonu bul (Örn: "2" yazan buton)
                # Hem <a> hem <button> hem de <li> içinde arar.
                xpath_query = f"//a[text()='{target_page_num}'] | //button[text()='{target_page_num}'] | //li[text()='{target_page_num}']"
                
                next_page_elements = driver.find_elements(By.XPATH, xpath_query)
                
                found_next = False
                for elem in next_page_elements:
                    if elem.is_displayed():
                        # JavaScript ile tıkla (Böylece üzerine bir şey binse bile tıklar)
                        driver.execute_script("arguments[0].click();", elem)
                        time.sleep(3) # Yüklenmesi için bekle
                        found_next = True
                        current_page_num += 1
                        break
                
                if not found_next:
                    # Belki numara buton olarak değil, "İleri" oku olarak vardır
                    # Eğer numara bulamazsak son çare ok işaretini deneriz
                    print(f"Sayfa {target_page_num} numarası bulunamadı, 'Sonraki' butonu deneniyor...")
                    next_arrows = driver.find_elements(By.XPATH, "//button[contains(@class, 'next')] | //li[contains(@class, 'next')]")
                    
                    if next_arrows:
                         driver.execute_script("arguments[0].click();", next_arrows[0])
                         time.sleep(3)
                         current_page_num += 1
                    else:
                        print("Gidilecek başka sayfa kalmadı.")
                        break

            except Exception as e:
                print(f"Sayfa geçiş hatası: {e}")
                break
                
            # Güvenlik Limiti (Sonsuz döngüye girmesin)
            if current_page_num > 50:
                print("Sayfa limiti (50) aşıldı.")
                break

        # Tüm sayfaları birleştir
        if all_dataframes:
            final_df = pd.concat(all_dataframes, ignore_index=True)
            final_df = final_df.drop_duplicates()
            return final_df
        else:
            return None

    except Exception as e:
        print(f"Genel Hata: {e}")
        return None
    finally:
        driver.quit()

def upload_to_firestore(df):
    collection_name = "fonlar"
    
    print("-" * 30)
    print(f"TOPLAM İNDİRİLEN FON: {len(df)}")
    print("-" * 30)
    
    target_col = df.columns[0]
    kod_cols = [c for c in df.columns if "Kod" in c or "Code" in c]
    if kod_cols:
        target_col = kod_cols[0]
    elif len(df.columns) > 1:
        target_col = df.columns[1]

    records = df.to_dict(orient='records')
    count = 0
    batch = db.batch()
    
    for item in records:
        raw_code = item.get(target_col)
        
        if raw_code and str(raw_code).lower() not in ['nan', 'none', '']:
            fon_kodu = str(raw_code).strip().replace('/', '-')
            
            doc_ref = db.collection(collection_name).document(fon_kodu)
            item['guncellenme_tarihi'] = firestore.SERVER_TIMESTAMP
            
            batch.set(doc_ref, item)
            count += 1
            
            if count % 400 == 0:
                batch.commit()
                batch = db.batch()
                print(f"{count} fon işlendi...")

    batch.commit()
    print(f"BAŞARILI: Toplam {count} fon Firebase'e yüklendi!")

if __name__ == "__main__":
    df_funds = get_fintables_funds()
    
    if df_funds is not None:
        upload_to_firestore(df_funds)
    else:
        print("HATA: Veri çekilemedi.")
        exit(1)
