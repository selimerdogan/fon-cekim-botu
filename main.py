import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
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
        
        # Çerezleri Temizle
        try:
            cookie_btns = driver.find_elements(By.XPATH, "//button[contains(text(), 'Kabul') or contains(text(), 'Tamam')]")
            if cookie_btns:
                driver.execute_script("arguments[0].click();", cookie_btns[0])
                time.sleep(1)
        except:
            pass

        # İLK VERİYİ AL
        html = driver.page_source
        tables = pd.read_html(StringIO(html))
        current_df = tables[0]
        
        # Temizlik
        if isinstance(current_df.columns, pd.MultiIndex):
            current_df.columns = [str(col[-1]).strip() for col in current_df.columns]
        else:
            current_df.columns = [str(col).strip() for col in current_df.columns]
        current_df = current_df.astype(str)
        all_dataframes.append(current_df)
        print(f"Sayfa 1: {len(current_df)} satır alındı.")

        page_num = 1
        max_pages = 60 
        
        while page_num < max_pages:
            try:
                # Sayfanın en altına in
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)

                # --- NAVİGASYON AVCISI ---
                # Sayfadaki tüm listeleri (ul) ve nav'ları tara.
                # İçinde en çok "li" (liste elemanı) olan yapı, sayfalama barıdır.
                
                potential_navs = driver.find_elements(By.CSS_SELECTOR, "ul, nav, div[class*='pagination']")
                target_button = None
                
                best_nav = None
                max_items = 0
                
                # En kalabalık listeyi bul (Pagination genelde en çok elemana sahip listedir: 1,2,3,4,5...)
                for nav in potential_navs:
                    items = nav.find_elements(By.CSS_SELECTOR, "li, button, a")
                    # En az 3 elemanı olsun (Geri, 1, İleri gibi)
                    if len(items) > 3:
                        if len(items) > max_items:
                            max_items = len(items)
                            best_nav = items

                if best_nav:
                    # Listenin SON elemanı genelde "İleri" butonudur.
                    last_item = best_nav[-1]
                    
                    # Eğer son eleman disabled ise (son sayfadaysak) çık
                    if "disabled" in last_item.get_attribute("class") or last_item.get_attribute("disabled"):
                        print("Son sayfaya gelindi (Buton pasif).")
                        break
                        
                    target_button = last_item
                
                # --- ALTERNATİF: SVG İKON ARA ---
                if not target_button:
                     # Sayfanın en altındaki sağa bakan ok ikonunu bul
                     # Bu çok güçlü bir yöntemdir.
                     svgs = driver.find_elements(By.CSS_SELECTOR, "button svg")
                     if svgs:
                         target_button = svgs[-1].find_element(By.XPATH, "./..") # SVG'nin sahibi olan butonu al

                # --- TIKLAMA VE KONTROL ---
                if target_button:
                    # Eski veriyi sakla (Değişim kontrolü için)
                    old_first_val = current_df.iloc[0, 0] if not current_df.empty else "YOK"
                    
                    # Tıkla
                    driver.execute_script("arguments[0].click();", target_button)
                    
                    # Verinin değişmesini bekle (Maksimum 10 saniye)
                    data_changed = False
                    for i in range(10):
                        time.sleep(1)
                        new_html = driver.page_source
                        new_tables = pd.read_html(StringIO(new_html))
                        if new_tables:
                            check_df = new_tables[0]
                            new_first_val = str(check_df.iloc[0, 0])
                            
                            if new_first_val != str(old_first_val):
                                # Veri değişti! Yeni sayfadayız.
                                if isinstance(check_df.columns, pd.MultiIndex):
                                    check_df.columns = [str(col[-1]).strip() for col in check_df.columns]
                                else:
                                    check_df.columns = [str(col).strip() for col in check_df.columns]
                                check_df = check_df.astype(str)
                                
                                current_df = check_df
                                all_dataframes.append(current_df)
                                page_num += 1
                                print(f"Sayfa {page_num}: {len(current_df)} satır alındı. (Toplam: {sum(len(d) for d in all_dataframes)})")
                                data_changed = True
                                break
                    
                    if not data_changed:
                        print("Butona tıklandı ama veri değişmedi. Döngü bitiyor.")
                        break
                else:
                    print("Sonraki sayfa butonu bulunamadı.")
                    break

            except Exception as e:
                print(f"Hata oluştu: {e}")
                break

        # BİRLEŞTİRME
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
    print(f"FİNAL TOPLAM FON SAYISI: {len(df)}")
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
