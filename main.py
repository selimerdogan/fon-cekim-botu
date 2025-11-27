from curl_cffi import requests
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime
import os
import json
import io

def get_data_from_bigpara():
    # Hürriyet Bigpara - Tüm Fonlar Sayfası
    # Bu sayfa HTML tablosu olarak tüm fonları listeler ve bot koruması düşüktür.
    url = "https://bigpara.hurriyet.com.tr/yatirim-fonlari/tum-fon-verileri/"
    
    today = datetime.now()
    doc_date_str = today.strftime("%Y-%m-%d")
    
    print(f"Bigpara üzerinden veriler çekiliyor... ({doc_date_str})")
    
    try:
        # Gerçek bir tarayıcı gibi istek atıyoruz
        response = requests.get(
            url, 
            impersonate="chrome120",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            }
        )
        
        # Sayfa içeriğini Pandas ile okuyoruz (HTML Table Parsing)
        # Bigpara'da tablo genellikle ilk bulunan tablodur.
        # thousands='.' ve decimal=',' parametreleri Türk Lirası formatını (1.234,56) otomatik çözer.
        dfs = pd.read_html(io.StringIO(response.text), thousands='.', decimal=',')
        
        if not dfs:
            print("HATA: Sayfada tablo bulunamadı.")
            return None, None
            
        df = dfs[0] # İlk tabloyu al
        
        # Sütun isimlerini kontrol edelim ve temizleyelim
        # Bigpara sütunları genelde: [Fon Kodu, Fon Adı, Fiyat, ...] şeklindedir.
        # Ancak bazen sütun isimleri değişebilir, biz pozisyona göre de alabiliriz.
        
        # Sütun adlarını standartlaştırma
        df.columns = [c.lower() for c in df.columns]
        
        # Fon Kodu ve Fiyat sütunlarını bulalım
        # Genelde 'fon kodu' veya benzeri bir isimle gelir.
        code_col = next((c for c in df.columns if 'kod' in c), None)
        price_col = next((c for c in df.columns if 'fiyat' in c or 'son' in c), None)
        
        if not code_col or not price_col:
            # İsimden bulamazsak, 1. sütun Kod, 3. sütun Fiyat varsayalım (Bigpara standardı)
            print("Sütun isimleri tanınmadı, varsayılan indeksler kullanılıyor.")
            code_col = df.columns[0]
            price_col = df.columns[2] 
            
        print(f"Kullanılan Sütunlar -> Kod: {code_col}, Fiyat: {price_col}")
        
        # Veriyi temizle
        df = df[[code_col, price_col]].copy()
        df.columns = ['FONKODU', 'FIYAT']
        
        # Kod sütununu temizle (Bazen link falan olabilir)
        df['FONKODU'] = df['FONKODU'].astype(str).str.strip()
        
        # Fiyat sayısal mı emin ol
        df['FIYAT'] = pd.to_numeric(df['FIYAT'], errors='coerce')
        
        # Boş (NaN) fiyatları at
        df = df.dropna(subset=['FIYAT'])
        
        # Map formatına çevir
        fund_dict = dict(zip(df['FONKODU'], df['FIYAT']))
        
        return fund_dict, doc_date_str

    except Exception as e:
        print(f"Veri Çekme Hatası: {e}")
        return None, None

def save_history_to_firebase(fund_data, doc_date):
    if not fund_data:
        return

    try:
        cred_json = json.loads(os.environ.get('FIREBASE_CREDENTIALS'))
        cred = credentials.Certificate(cred_json)
        # Firebase zaten initialize edilmişse hata vermesin
        try:
            firebase_admin.initialize_app(cred)
        except ValueError:
            pass
        
        db = firestore.client()
        doc_ref = db.collection('fund_history').document(doc_date)
        
        data_payload = {
            "date": doc_date,
            "createdAt": firestore.SERVER_TIMESTAMP,
            "closing": fund_data
        }
        
        doc_ref.set(data_payload, merge=True)
        print(f"BAŞARILI: '{doc_date}' tarihine {len(fund_data)} adet fon kaydedildi.")
        
    except Exception as e:
        print(f"Firebase Hatası: {e}")

if __name__ == "__main__":
    data, date_id = get_data_from_bigpara()
    if data:
        save_history_to_firebase(data, date_id)
    else:
        print("İşlem başarısız.")
        exit(1)
