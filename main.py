import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from io import StringIO
import os

def get_fintables_funds():
    chrome_options = Options()
    chrome_options.add_argument("--headless") 
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    # GitHub sunucuları Linux olduğu için pencere boyutunu sabitlemek iyidir
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    try:
        url = "https://fintables.com/fonlar/getiri"
        print("Siteye gidiliyor...")
        driver.get(url)
        
        # Verilerin yüklenmesi için bekleme
        time.sleep(10) 

        html = driver.page_source
        tables = pd.read_html(StringIO(html))
        
        if tables:
            df = tables[0]
            # Tarih ekleyelim ki verinin ne zaman çekildiği belli olsun
            df['Cekilme_Tarihi'] = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M')
            return df
        else:
            return None

    except Exception as e:
        print(f"Hata: {e}")
        return None
    finally:
        driver.quit()

if __name__ == "__main__":
    df = get_fintables_funds()
    if df is not None:
        # Dosyayı kaydet
        df.to_excel("guncel_fonlar.xlsx", index=False)
        print("Excel dosyası oluşturuldu.")
    else:
        print("Veri çekilemedi.")
        exit(1) # Hata koduyla çık ki GitHub hatayı görsün
