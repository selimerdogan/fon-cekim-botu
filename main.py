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
        time.sleep(8) # İyice yüklensin

        # 1. Çerezleri Kapat
        try:
            cookie_btn = driver.find_element(By.XPATH, "//button[contains(text(), 'Kabul') or contains(text(), 'Tamam')]")
            driver.execute_script("arguments[0].click();", cookie_btn)
            time.sleep(1)
        except:
            pass

        # 2. STRATEJİ: 'TÜMÜ' SEÇENEĞİNİ BUL VE TIKLA (En Temiz Yöntem)
        print("Sayfalama Menüsü Aranıyor...")
        try:
            # Sayfanın en altına in
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1)
            
            # Sayfada '10', '20', '50' gibi sayıların yazdığı açılır kutuları bulmaya çalış
            # Bu genellikle tablonun altında "Gösterilecek Kayıt" kutusudur.
            potential_dropdowns = driver.find_elements(By.XPATH, "//*[contains(text(), '10') or contains(text(), '20') or contains(text(), '50')]")
            
            dropdown_clicked = False
            for elem in potential_dropdowns:
                # Sadece tıklanabilir küçük elemanları dene (Tüm sayfa gövdesini değil)
                if elem.is_displayed() and elem.size['width'] < 200 and elem.size['height'] < 100:
                    try:
                        # Bu bir dropdown olabilir, tıkla!
                        driver.execute_script("arguments[0].click();", elem)
                        time.sleep(1)
                        
                        # Tıkladıktan sonra 'Tümü' veya 'All' seçeneği çıktı mı?
                        all_option = driver.find_elements(By.XPATH, "//*[contains(text(), 'Tümü') or contains(text(), 'Hepsi') or contains(text(), 'All')]")
                        if all_option:
                            for opt in all_option:
                                if opt.is_displayed():
                                    driver.execute_script("arguments[0].click();", opt)
                                    print("MÜKEMMEL: 'Tümü' seçeneği bulundu ve tıklandı!")
                                    dropdown_clicked = True
                                    time.sleep(10) # Verilerin yüklenmesini bekle
                                    break
                        if dropdown_clicked: break
                    except:
                        continue
            
            if not dropdown_clicked:
                print("Dropdown stratejisi işe yaramadı, sayfa sayfa gezme moduna geçiliyor.")
        
        except Exception as e:
            print(f"Dropdown hatası: {e}")

        # 3. STRATEJİ: SAYFA NUMARALARINI GEZ (Yedek Plan)
        # Eğer yukarıdaki çalışmadıysa burası devreye girer
        
        page_num = 1
        while True:
            # Mevcut sayfayı kaydet
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
                print(f"Sayfa {page_num} tarandı. (Satır sayısı: {len(df)})")
            
            # Sonraki sayfaya geç
            try:
                # Sayfanın altına in
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)

                # Sıradaki numarayı bul (Örn: şu an 1 ise, "2" yazan butonu bul)
                next_page = page_num + 1
                
                # Çok geniş bir XPath kullanıyoruz: Text içeriği TAM OLARAK sayıya eşit olan herhangi bir element
                # Hem '2' hem ' 2 ' boşluklu olabilir.
                xpath = f"//li[contains(., '{next_page}')] | //button[contains(., '{next_page}')] | //a[contains(text(), '{next_page}')]"
                
                buttons = driver.find_elements(By.XPATH, xpath)
                clicked = False
                
                # Bulunan butonlardan tıklanabilir olanı seç
                for btn in buttons:
                    # Sayı metni içeriyor mu kontrol et (Gereksiz textleri elemek için)
                    if str(next_page) in btn.text and len(btn.text) < 5: 
                         # Görünür olmasa bile JS ile tıkla
                         driver.execute_script("arguments[0].click();", btn)
                         time.sleep(3)
                         clicked = True
                         page_num += 1
                         break
                
                if not clicked:
                    # Numarayı bulamadıysak "İleri" okunu deneyelim (SVG ikonlu butonlar)
                    # Genelde "last-child" olan buton "İleri" butonudur.
                    # Pagination div'ini bulup son butonuna tıklayalım
                    try:
                        pagination_divs = driver.find_elements(By.CSS_SELECTOR, "[class*='pagination']")
                        if pagination_divs:
                            buttons_in_pag = pagination_divs[0].find_elements(By.TAG_NAME, "button")
                            if buttons_in_pag:
                                last_btn = buttons_in_pag[-1]
                                driver.execute_script("arguments[0].click();", last_btn)
                                time.sleep(3)
                                page_num += 1
                                clicked = True
                    except:
                        pass
                
                if not clicked:
                    print(f"Sayfa {next_page} için buton bulunamadı. Tarama bitiyor.")
                    break
                    
                if page_num > 50: # Güvenlik
                    break

            except Exception as e:
                print(f"Döngü hatası: {e}")
                break

        # Birleştirme
        if all_dataframes:
            final_df = pd.concat(all_dataframes, ignore_index=True)
            final_df = final_df.drop_duplicates() # Mükerrerleri sil
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
         print("UYARI: Hala az fon var. Site yapısı dinamik yükleme yapıyor olabilir.")

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
