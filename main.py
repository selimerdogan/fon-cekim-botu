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
        time.sleep(8)
        
        # Sayfa sonuna in (Footer çakışmasını önle)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)

        # Çerez kapat
        try:
            cookie_btns = driver.find_elements(By.XPATH, "//button[contains(text(), 'Kabul') or contains(text(), 'Tamam')]")
            if cookie_btns:
                driver.execute_script("arguments[0].click();", cookie_btns[0])
                time.sleep(1)
        except:
            pass

        # İLK SAYFAYI AL
        html = driver.page_source
        tables = pd.read_html(StringIO(html))
        current_df = tables[0]
        
        # Veri temizliği
        if isinstance(current_df.columns, pd.MultiIndex):
            current_df.columns = [str(col[-1]).strip() for col in current_df.columns]
        else:
            current_df.columns = [str(col).strip() for col in current_df.columns]
        current_df = current_df.astype(str)
        
        all_dataframes.append(current_df)
        print(f"Sayfa 1 Alındı. ({len(current_df)} satır)")

        # DÖNGÜ BAŞLIYOR
        page_num = 1
        max_pages = 70 # Güvenlik limiti
        
        while page_num < max_pages:
            # Kontrol Noktası: Şu anki tablonun ilk satırındaki ilk veriyi al (Örn: "AAK")
            # Bir sonraki sayfaya geçtiğimizde bu verinin değişmesi lazım.
            last_first_val = current_df.iloc[0, 0] if not current_df.empty else "YOK"
            last_second_val = current_df.iloc[0, 1] if not current_df.empty and len(current_df.columns) > 1 else "YOK"
            
            # --- TIKLAMA MANTIĞI ---
            clicked = False
            try:
                # Sayfanın en altına in
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)

                # 1. Yöntem: Pagination içindeki "Sonraki" oku (> veya ikon)
                # Genelde pagination listesinin son elemanı veya sondan bir önceki elemandır.
                next_buttons = driver.find_elements(By.CSS_SELECTOR, "ul.pagination li:last-child a, ul.pagination li:last-child button")
                
                # Eğer spesifik yapı bulunamazsa genel arama:
                if not next_buttons:
                    next_buttons = driver.find_elements(By.XPATH, "//button[contains(@class, 'next')] | //li[contains(@class, 'next')]//a")

                if next_buttons:
                    btn = next_buttons[0]
                    # Görünürlük kontrolü yapmadan JS ile zorla tıkla
                    driver.execute_script("arguments[0].click();", btn)
                    clicked = True
                
            except Exception as e:
                print(f"Tıklama hatası: {e}")
            
            if not clicked:
                print("Sonraki buton bulunamadı, işlem bitiriliyor.")
                break

            # --- VERİ DEĞİŞİM KONTROLÜ (Wait Loop) ---
            print(f"Sayfa {page_num+1} için tıklandı, veri değişimi bekleniyor...")
            
            data_changed = False
            for attempt in range(10): # 10 saniye boyunca dene
                time.sleep(1)
                
                try:
                    new_html = driver.page_source
                    new_tables = pd.read_html(StringIO(new_html))
                    if new_tables:
                        check_df = new_tables[0]
                        
                        # Yeni tablonun ilk verisi eskisiyle farklı mı?
                        # Bazen sadece 1. sütun sıra nosudur (1), 2. sütun kod (AAK) olur. İkisini de kontrol et.
                        new_first_val = str(check_df.iloc[0, 0])
                        new_second_val = str(check_df.iloc[0, 1]) if len(check_df.columns) > 1 else "YOK"
                        
                        # Eğer veri değiştiyse (Eşit değilse)
                        if str(last_first_val) != new_first_val or str(last_second_val) != new_second_val:
                            # Harika! Yeni sayfa yüklenmiş.
                            
                            # Temizliği yap ve kaydet
                            if isinstance(check_df.columns, pd.MultiIndex):
                                check_df.columns = [str(col[-1]).strip() for col in check_df.columns]
                            else:
                                check_df.columns = [str(col).strip() for col in check_df.columns]
                            check_df = check_df.astype(str)
                            
                            current_df = check_df # Referansı güncelle
                            all_dataframes.append(current_df)
                            data_changed = True
                            page_num += 1
                            print(f"BAŞARILI: Sayfa {page_num} verisi alındı. (İlk veri: {new_second_val})")
                            break
                except:
                    continue
            
            if not data_changed:
                print("DİKKAT: Butona tıklandı ama veri değişmedi. Sayfa sonuna gelinmiş olabilir.")
                break

        # BİRLEŞTİRME
        if all_dataframes:
            print("Tablolar birleştiriliyor...")
            final_df = pd.concat(all_dataframes, ignore_index=True)
            
            print(f"Duplicate öncesi satır sayısı: {len(final_df)}")
            final_df = final_df.drop_duplicates()
            print(f"Duplicate sonrası satır sayısı: {len(final_df)}")
            
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
