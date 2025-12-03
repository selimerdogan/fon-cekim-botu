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

        # Klavye ile sayfa sonuna git (Yüklenmeyen elemanları tetikler)
        ActionChains(driver).send_keys(Keys.END).perform()
        time.sleep(2)

        # 1. Çerezleri Kapat
        try:
            cookie_btns = driver.find_elements(By.XPATH, "//button[contains(text(), 'Kabul') or contains(text(), 'Tamam')]")
            if cookie_btns:
                driver.execute_script("arguments[0].click();", cookie_btns[0])
                time.sleep(1)
        except:
            pass

        # 2. STRATEJİ: MODERN DROPDOWN AVCI (Select etiketi olmayanlar için)
        print("Gelişmiş Dropdown Araması yapılıyor...")
        try:
            # İçinde '20', '50' veya '100' yazan küçük tıklanabilir alanları bul
            # Bu genellikle "Sayfada 20 kayıt göster" kutusudur.
            candidates = driver.find_elements(By.XPATH, "//*[text()='20' or text()='50' or text()='100']")
            
            dropdown_success = False
            for cand in candidates:
                # Ebeveyn elementine bak (Genelde sayı bir div içindedir)
                parent = cand.find_element(By.XPATH, "./..")
                if parent.is_displayed():
                    driver.execute_script("arguments[0].click();", parent)
                    time.sleep(1)
                    
                    # Tıkladıktan sonra 'Tümü' çıktı mı?
                    all_opts = driver.find_elements(By.XPATH, "//*[text()='Tümü' or text()='Hepsi' or text()='All']")
                    for opt in all_opts:
                        if opt.is_displayed():
                            driver.execute_script("arguments[0].click();", opt)
                            print("MÜKEMMEL: Dropdown 'Tümü' seçeneği bulundu!")
                            dropdown_success = True
                            time.sleep(10) # Veri yüklenmesi için bekle
                            break
                if dropdown_success: break
            
            if not dropdown_success:
                print("Dropdown bulunamadı, sayfa gezme moduna geçiliyor.")
        except Exception as e:
            print(f"Dropdown hatası: {e}")

        # 3. STRATEJİ: SAYFA SAYFA GEZME (İkon Tıklama Modu)
        
        # İlk sayfayı al
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
            print(f"Başlangıç: {len(df)} veri alındı.")

        page_count = 1
        while page_count < 60: # Max 60 sayfa (Güvenlik)
            try:
                # Sayfanın en altına in
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)

                # "SONRAKİ" BUTONUNU BULMA SANATI
                # Metin araması yerine, Pagination barın SON butonunu hedefliyoruz.
                # Genelde yapı şöyledir: [1] [2] [3] [...] [>]
                
                # Tüm butonları bul
                # class'ında 'pagination', 'page', 'next' geçen containerları bul
                next_btn = None
                
                # Yöntem A: Pagination içindeki son butona tıkla
                paginations = driver.find_elements(By.CSS_SELECTOR, "ul[class*='pagination'], div[class*='pagination'], nav[class*='pagination']")
                
                clicked = False
                if paginations:
                    # Pagination içindeki tüm tıklanabilirleri al (li, button, a)
                    items = paginations[0].find_elements(By.CSS_SELECTOR, "li, button, a")
                    if items:
                        # En sondaki eleman genelde "İleri" butonudur
                        last_item = items[-1]
                        # Eğer bu buton 'disabled' değilse tıkla
                        if "disabled" not in last_item.get_attribute("class"):
                             driver.execute_script("arguments[0].click();", last_item)
                             clicked = True
                
                # Yöntem B: Eğer Pagination container bulunamadıysa, SVG ikonlu butonları dene
                if not clicked:
                    # İçinde SVG (ikon) olan tüm butonları bul (Genelde ok işaretidir)
                    svg_buttons = driver.find_elements(By.CSS_SELECTOR, "button svg, a svg")
                    if svg_buttons:
                        # Sayfanın en altındaki svg butonu muhtemelen "ileri"dir
                        parent_btn = svg_buttons[-1].find_element(By.XPATH, "./..")
                        driver.execute_script("arguments[0].click();", parent_btn)
                        clicked = True

                if clicked:
                    time.sleep(3) # Yeni sayfanın yüklenmesini bekle
                    
                    # Yeni veriyi oku
                    html = driver.page_source
                    new_tables = pd.read_html(StringIO(html))
                    if new_tables:
                        new_df = new_tables[0]
                        # Temizlik
                        if isinstance(new_df.columns, pd.MultiIndex):
                            new_df.columns = [str(col[-1]).strip() for col in new_df.columns]
                        else:
                            new_df.columns = [str(col).strip() for col in new_df.columns]
                        new_df = new_df.astype(str)
                        
                        all_dataframes.append(new_df)
                        print(f"Sayfa {page_count + 1} eklendi. ({len(new_df)} satır)")
                        page_count += 1
                    else:
                        print("Tablo okunamadı, durduruluyor.")
                        break
                else:
                    print("Tıklanacak 'Sonraki' butonu bulunamadı. Son sayfa olabilir.")
                    break

            except Exception as e:
                print(f"Döngü hatası: {e}")
                break

        # BİRLEŞTİRME VE TEMİZLİK
        if all_dataframes:
            final_df = pd.concat(all_dataframes, ignore_index=True)
            
            # Tekrar eden satırları temizle (Sayfa geçişlerinde bazen overlap olur)
            # Tüm sütunlara göre duplicate kontrolü
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
    
    if len(df) < 50:
         print("UYARI: Hala az fon var. Site yapısı çok inatçı çıktı.")

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
