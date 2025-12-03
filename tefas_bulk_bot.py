import requests
import pandas as pd
import firebase_admin
from firebase_admin import credentials, firestore
import json
import os
from datetime import datetime, timedelta
import sys

# --- FIREBASE BAĞLANTISI ---
firebase_creds_str = os.environ.get('FIREBASE_CREDENTIALS')

if firebase_creds_str:
    try:
        cred_dict = json.loads(firebase_creds_str)
        cred = credentials.Certificate(cred_dict)
        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred)
        db = firestore.client()
    except Exception as e:
        print(f"Firebase Bağlantı Hatası: {e}")
        sys.exit(1)
else:
    if os.path.exists("serviceAccountKey.json"):
        cred = credentials.Certificate("serviceAccountKey.json")
        if not firebase_admin._apps:
             firebase_admin.initialize_app(cred)
        db = firestore.client()
    else:
        print("HATA: Firebase anahtarı bulunamadı.")
        sys.exit(1)

# --- SENİN VERDİĞİN HEADERLAR (KAPIYI AÇAN ANAHTAR) ---
SHARED_HEADERS = {
    "Content-Type": "application/json;charset=UTF-8",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Origin": "https://www.tefas.gov.tr",
    "Referer": "https://www.tefas.gov.tr/TarihselVeriler.aspx",
    "X-Requested-With": "XMLHttpRequest"
}

def get_fund_metadata():
    """Fonların ADI, BÜYÜKLÜĞÜ ve KİŞİ SAYISI verilerini çeker (GenelVeriler)."""
    print("Fon kimlik bilgileri çekiliyor...")
    
    url = "https://www.tefas.gov.tr/api/DB/GenelVeriler"
    
    today = datetime.now()
    start_date = today - timedelta(days=7) # Garanti olsun diye 1 hafta
    
    payload = {
        "fontip": "YAT",
        "sfontip": "",
        "bastarih": start_date.strftime("%d.%m.%Y"),
        "bittarih": today.strftime("%d.%m.%Y"),
        "fonkod": ""
    }
    
    try:
        # Direkt istek (Senin yöntemin)
        response = requests.post(url, json=payload, headers=SHARED_HEADERS)
        data = response.json().get('data', [])
        
        metadata_map = {}
        for item in data:
            kod = item.get('FONKODU')
            if kod:
                metadata_map[kod] = {
                    'ad': item.get('FONUNADI', ''),
                    'buyukluk': float(item.get('FONTOPLAMDEGER', 0) or 0),
                    'kisi_sayisi': int(float(item.get('KISISAYISI', 0) or 0))
                }
        print(f"✅ Kimlik Bilgileri Alındı: {len(
