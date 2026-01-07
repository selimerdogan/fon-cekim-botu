import requests
import firebase_admin
from firebase_admin import credentials, firestore, storage
import os
import sys
import json
import io
from PIL import Image  # Resim işleme için gerekli (pip install Pillow)
from datetime import datetime

# --- GENEL AYARLAR ---
headers_general = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# --- KİMLİK KONTROLLERİ VE BAŞLATMA ---
firebase_key_str = os.environ.get('FIREBASE_KEY')
CMC_API_KEY = os.environ.get('CMC_API_KEY')

# Firebase Storage Bucket Adı
BUCKET_NAME = "vario-264d9.firebasestorage.app"

if not firebase_key_str:
    if os.path.exists("serviceAccountKey.json"):
        cred = credentials.Certificate("serviceAccountKey.json")
    else:
        print("HATA: Anahtar (FIREBASE_KEY) bulunamadı!")
        sys.exit(1)
else:
    cred_dict = json.loads(firebase_key_str)
    cred = credentials.Certificate(cred_dict)

try:
    if not firebase_admin._apps:
        # Storage Bucket ayarını buraya ekledik
        firebase_admin.initialize_app(cred, {
            'storageBucket': BUCKET_NAME
        })
    db = firestore.client()
    bucket = storage.bucket() # Storage erişimi
except Exception as e:
    print(f"HATA: Firebase hatası: {e}")
    sys.exit(1)

# ==============================================================================
# YARDIMCI FONKSİYON: RESMİ İNDİR, KÜÇÜLT, YÜKLE (SVG DESTEKLİ)
# ==============================================================================
def upload_logo(original_url, file_name, folder_name):
    """
    Verilen URL'deki resmi indirir.
    - Eğer SVG ise: Direkt yükler (Pillow SVG açamaz, hata vermesin diye).
    - Eğer PNG/JPG ise: 128x128 yapar, PNG olarak yükler.
    Geriye Firebase'deki kalıcı public linki döner.
    """
    # 1. Eğer link zaten bizim Firebase'e veya FlagCDN'e aitse elleme
    if "firebasestorage.googleapis.com" in original_url or "flagcdn.com" in original_url:
        return original_url

    # 2. Avatar servisi ise atla (Tasarruf)
    if "ui-avatars.com" in original_url or not original_url:
        return original_url

    try:
        # 3. Resmi İndir
        resp = requests.get(original_url, headers=headers_general, timeout=15)
        if resp.status_code != 200:
            return original_url 

        content_type = resp.headers.get('Content-Type', '')
        file_data = resp.content

        # --- SENARYO A: DOSYA SVG İSE (TradingView Hatasını Çözen Kısım) ---
        if "svg" in content_type or original_url.endswith(".svg") or b"<svg" in file_data[:100]:
            blob_path = f"logos/{folder_name}/{file_name}.svg"
            blob = bucket.blob(blob_path)
            blob.upload_from_string(file_data, content_type="image/svg+xml")
            blob.make_public()
            return blob.public_url

        # --- SENARYO B: DOSYA RESİM İSE (PNG, JPG) ---
        img_bytes = io.BytesIO(file_data)
        img = Image.open(img_bytes)
        
        if img.mode != 'RGBA':
            img = img.convert('RGBA')
            
        img = img.resize((128, 128), Image.Resampling.LANCZOS)

        output_io = io.BytesIO()
        img.save(output_io, format='PNG', optimize=True)
        image_data = output_io.getvalue()

        blob_path = f"logos/{folder_name}/{file_name}.png"
        blob = bucket.blob(blob_path)
        
        blob.upload_from_string(image_data, content_type="image/png")
        blob.make_public()

        return blob.public_url

    except Exception as e:
        # Hata olsa bile sistemi durdurma, orijinal linki kullan
        # print(f"   ⚠️ Hata ({file_name}): {e}") 
        return original_url 

# ==============================================================================
# 1. BIST & ABD (GÜNCELLENMİŞ - GÜÇLENDİRİLMİŞ HEADERS)
# ==============================================================================
def get_tradingview_metadata(market):
    print(f"   -> {market.upper()} Logoları aranıyor ve yükleniyor...")
    url = f"https://scanner.tradingview.com/{market}/scan"
    
    # TradingView Bot Korumasını Aşmak İçin Gerekli Başlıklar
    headers_tv = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Origin": "https://www.tradingview.com",
        "Referer": "https://www.tradingview.com/",
        "Content-Type": "application/json"
    }
    
    payload = {
        "filter": [{"left": "type", "operation": "in_range", "right": ["stock", "dr", "fund"]}],
        "options": {"lang": "tr"},
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "description", "logoid"],
        "range": [0, 6000] 
    }
    
    data = {}
    base_logo_url = "https://s3-symbol-logo.tradingview.com/"
    bg_color = "b30000" if market == "turkey" else "0D8ABC"
    
    try:
        r = requests.post(url, json=payload, headers=headers_tv, timeout=60)
        
        if r.status_code != 200:
            print(f"      ⛔ HATA: TradingView yanıt vermedi! Kod: {r.status_code}")
            return {}

        items = r.json().get('data', [])
        print(f"      ℹ️  TradingView'dan {len(items)} adet veri çekildi.")

        count = 0
        print(f"      🚀 İşlem başlıyor... Toplam {len(items)} hisse.")

        for h in items:
            d = h.get('d', [])
            if len(d) > 2:
                sembol = d[0] 
                isim = d[1]   
                logo_id = d[2]
                
                if logo_id:
                    raw_url = f"{base_logo_url}{logo_id}.svg"
                    folder_name = f"stocks_{market}" 
                    # Burada SVG destekli upload fonksiyonu çalışacak
                    final_logo = upload_logo(raw_url, sembol, folder_name)
                else:
                    final_logo = f"https://ui-avatars.com/api/?name={sembol}&background={bg_color}&color=fff&size=128&bold=true"
                
                if "," in isim: isim = isim.split(",")[0]
                
                data[sembol] = {"name": isim, "logo": final_logo}
                
                count += 1
                if count % 50 == 0:
                    print(f"      Processing... {count}/{len(items)}")

        print(f"      ✅ {market.upper()}: {len(data)} adet logo başarıyla işlendi.")
    
    except Exception as e:
        print(f"      ⛔ KRİTİK HATA (TradingView): {e}")
        
    return data

# ==============================================================================
# 2. KRİPTO
# ==============================================================================
def get_crypto_metadata():
    print("2. Kripto Logoları (CMC) çekiliyor ve yükleniyor...")
    
    if not CMC_API_KEY:
        print("   -> ⚠️ CMC Key Yok! Manuel liste.")
        btc_url = upload_logo("https://s2.coinmarketcap.com/static/img/coins/64x64/1.png", "BTC-USD", "crypto")
        eth_url = upload_logo("https://s2.coinmarketcap.com/static/img/coins/64x64/1027.png", "ETH-USD", "crypto")
        return {
            "BTC-USD": {"name": "Bitcoin", "logo": btc_url},
            "ETH-USD": {"name": "Ethereum", "logo": eth_url}
        }

    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    params = {'start': '1', 'limit': '300', 'convert': 'USD'}
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': CMC_API_KEY}
    data = {}
    
    try:
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code == 200:
            coins = r.json()['data']
            print(f"      {len(coins)} Kripto para işleniyor...")
            
            for coin in coins:
                sym = coin['symbol']
                name = coin['name']
                coin_id = coin['id']
                raw_logo = f"https://s2.coinmarketcap.com/static/img/coins/64x64/{coin_id}.png"
                
                key = f"{sym}-USD"
                final_logo = upload_logo(raw_logo, key, "crypto")
                
                data[key] = {"name": name, "logo": final_logo}
                
            print(f"   -> ✅ CMC: {len(data)} adet kripto yüklendi.")
    except Exception as e:
        print(f"   -> ⚠️ CMC Hatası: {e}")
        
    return data

# ==============================================================================
# 3. FONLAR (TEFAS - YENİ MAVİ İKON & HATA DÜZELTMESİ)
# ==============================================================================
def get_fon_metadata():
    print("3. Fon İsimleri (TEFAS) taranıyor...")
    data = {}
    
    # SENİN VERDİĞİN YENİ İKON (Varlık Logo)
    ICON_FUND = "https://firebasestorage.googleapis.com/v0/b/vario-264d9.firebasestorage.app/o/varl%C4%B1k_Logo%2Ffon.png?alt=media&token=00855c67-cda8-4dd6-a4e8-f8c3fb93ebae"
    
    url = "https://www.tefas.gov.tr/api/DB/BindComparisonFundReturns"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
        "Referer": "https://www.tefas.gov.tr",
        "Origin": "https://www.tefas.gov.tr",
        "Content-Type": "application/json"
    }
    
    try:
        simdi = datetime.now()
        tarih_str = simdi.strftime("%d.%m.%Y")
        payload = {"calismatipi": "2", "fontip": "YAT", "bastarih": tarih_str, "bittarih": tarih_str}
        
        r = requests.post(url, json=payload, headers=headers, timeout=30)
        
        try:
            l = r.json().get('data', [])
        except json.JSONDecodeError:
            print("   ⚠️ TEFAS sunucusu yanıt vermedi, liste boş geçiliyor.")
            l = []

        if len(l) > 0:
            for f in l:
                kod = f['FONKODU']
                isim = f['FONADI']
                # Tüm fonlara sabit mavi logoyu atıyoruz
                data[kod] = {"name": isim, "logo": ICON_FUND}
            print(f"   -> ✅ TEFAS: {len(data)} adet fon işlendi.")
            
    except Exception as e: 
        print(f"Hata (TEFAS): {e}")
    
    return data

# ==============================================================================
# 4. DÖVİZ & ALTIN
# ==============================================================================
def get_doviz_altin_metadata(): 
    print("--- LOGO/METADATA HAZIRLANIYOR (Döviz & Altın) ---")
    
    # Senin verdiğin Firebase Linkleri
    ICON_GOLD = "https://firebasestorage.googleapis.com/v0/b/vario-264d9.firebasestorage.app/o/varl%C4%B1k_Logo%2Faltin.png?alt=media&token=59ceaffd-adca-48ba-9251-176f88e4b115"
    ICON_METAL = "https://firebasestorage.googleapis.com/v0/b/vario-264d9.firebasestorage.app/o/varl%C4%B1k_Logo%2Fgumus.png?alt=media&token=56f3452f-acca-4a92-8afb-870f361893cb"

    # 1. DÖVİZ
    doviz_config = {
        "USD": {"n": "ABD Doları", "c": "us"},
        "EUR": {"n": "Euro", "c": "eu"},
        "GBP": {"n": "İngiliz Sterlini", "c": "gb"},
        "CHF": {"n": "İsviçre Frangı", "c": "ch"},
        "JPY": {"n": "Japon Yeni", "c": "jp"},
        "RUB": {"n": "Rus Rublesi", "c": "ru"},
        "CNY": {"n": "Çin Yuanı", "c": "cn"},
        "BAE": {"n": "BAE Dirhemi", "c": "ae"},
        "CAD": {"n": "Kanada Doları", "c": "ca"}
    }

    data_doviz = {}
    for kod, info in doviz_config.items():
        data_doviz[kod] = {
            "name": info["n"], 
            "logo": f"https://flagcdn.com/w320/{info['c']}.png"
        }
            
    # 2. ALTIN
    altin_listesi = [
        "14 Ayar Bilezik", "18 Ayar Bilezik", "22 Ayar Bilezik", "Ata Altın",
        "Beşli Altın", "Cumhuriyet Altını", "Gram Altın", "Gram Gümüş",
        "Gram Has Altın", "Gram Paladyum", "Gram Platin", "Gremse Altın",
        "Hamit Altın", "Reşat Altın", "Tam Altın", "Yarım Altın",
        "Çeyrek Altın", "İkibuçuk Altın"
    ]
    
    data_altin = {}
    for isim in altin_listesi:
        if any(x in isim for x in ["Gümüş", "Platin", "Paladyum"]):
            ikon = ICON_METAL
        else:
            ikon = ICON_GOLD
        data_altin[isim] = {"name": isim, "logo": ikon}
    
    return data_doviz, data_altin

# ==============================================================================
# ANA ÇALIŞTIRMA BLOĞU
# ==============================================================================
if __name__ == "__main__":
    print("--- LOGO/METADATA MİGRASYON BAŞLIYOR (FIREBASE STORAGE) ---")
    print("NOT: Bu işlem ilk seferde biraz uzun sürebilir (Resimler indiriliyor...)")

    # 1. Verileri Çek ve Yükle
    meta_kripto = get_crypto_metadata()
    meta_bist = get_tradingview_metadata("turkey")
    
    # --- ABD HİSSELERİNİ AKTİF ETTİK ---
    meta_abd = get_tradingview_metadata("america") 
    # -----------------------------------
    
    meta_fon = get_fon_metadata()
    meta_doviz, meta_altin = get_doviz_altin_metadata()

    # 2. Veritabanına Kaydet
    coll_ref = db.collection(u'system_data')

    if meta_bist: 
        coll_ref.document(u'bist').set({u'data': meta_bist})
        print("✅ BIST veritabanı güncellendi.")
        
    # --- ABD HİSSELERİNİ KAYDETMEYİ AKTİF ETTİK ---
    if meta_abd: 
        coll_ref.document(u'abd').set({u'data': meta_abd})
        print("✅ ABD Borsası veritabanı güncellendi.")
    # ----------------------------------------------
    
    if meta_kripto: 
        coll_ref.document(u'kripto').set({u'data': meta_kripto})
        print("✅ Kripto veritabanı güncellendi.")

    if meta_fon: 
        coll_ref.document(u'fon').set({u'data': meta_fon})
        print("✅ Fon veritabanı güncellendi.")
        
    if meta_doviz:
        coll_ref.document(u'doviz').set({u'data': meta_doviz})
        print("✅ Döviz veritabanı güncellendi.")
        
    if meta_altin:
        coll_ref.document(u'altin').set({u'data': meta_altin})
        print("✅ Altın veritabanı güncellendi.")

    print("\n🎉 TÜM İŞLEMLER BAŞARIYLA TAMAMLANDI!")
